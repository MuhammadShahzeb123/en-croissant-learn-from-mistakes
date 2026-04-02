use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
};

use log::info;
use serde::{Deserialize, Serialize};
use shakmaty::{
    fen::Fen, uci::UciMove, CastlingMode, Chess, Color, EnPassantMode, FromSetup, Position,
    PositionError,
};
use specta::Type;
use tokio::sync::Mutex as TokioMutex;
use vampirc_uci::{parse_one, UciMessage};

use crate::{
    chess::{parse_uci_attrs, BestMoves},
    db::encoding::{decode_move, iter_mainline_move_bytes},
    engine::{BaseEngine, EngineOption, EngineReader, GoMode},
    error::Error,
    progress::update_progress,
    AppState,
};

// ── Lichess Cloud Eval Types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CloudEvalPv {
    #[serde(default)]
    cp: Option<i32>,
    #[serde(default)]
    mate: Option<i32>,
    moves: String,
}

#[derive(Debug, Deserialize)]
struct CloudEvalResponse {
    fen: String,
    #[serde(default)]
    knodes: u64,
    depth: u32,
    pvs: Vec<CloudEvalPv>,
}

/// Fetch a cloud evaluation from Lichess for a given FEN.
/// Returns None if the position is not in the cloud database (404).
async fn fetch_cloud_eval(
    client: &reqwest::Client,
    fen: &str,
    multipv: u16,
) -> Result<Option<CloudEvalResponse>, Error> {
    let url = format!(
        "https://lichess.org/api/cloud-eval?fen={}&multiPv={}",
        fen.replace(' ', "%20"),
        multipv
    );
    let resp = client
        .get(&url)
        .header(
            "User-Agent",
            "EnCroissant/0.15.0 (https://github.com/franciscoBSalgueiro/en-croissant)",
        )
        .send()
        .await
        .map_err(|e| Error::HttpError(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None); // Position not in cloud DB
    }
    if !resp.status().is_success() {
        // Rate limited or server error — return None to skip
        info!(
            "Cloud eval returned status {} for FEN: {}",
            resp.status(),
            fen
        );
        return Ok(None);
    }

    let data: CloudEvalResponse = resp
        .json()
        .await
        .map_err(|e| Error::HttpError(e.to_string()))?;
    Ok(Some(data))
}

/// Convert a CloudEvalPv into a vampirc_uci Score (for reuse with score_from_player_perspective)
fn cloud_pv_to_score(pv: &CloudEvalPv) -> vampirc_uci::uci::Score {
    use vampirc_uci::uci::{Score, ScoreValue};
    if let Some(mate) = pv.mate {
        Score {
            value: ScoreValue::Mate(mate),
            lower_bound: None,
            upper_bound: None,
            wdl: None,
        }
    } else {
        Score {
            value: ScoreValue::Cp(pv.cp.unwrap_or(0)),
            lower_bound: None,
            upper_bound: None,
            wdl: None,
        }
    }
}

// ── FEN cache: avoids re-analyzing identical positions across games ──────────

/// Cached evaluation for a FEN position.
type FenCache = Arc<TokioMutex<HashMap<String, CachedEval>>>;

#[derive(Clone, Debug)]
struct CachedEval {
    best_uci: String,
    best_line: String,
    score: vampirc_uci::uci::Score,
    depth: u32,
}

/// Rate limiter for Lichess cloud API — enforces ≥1s between requests globally.
struct CloudRateLimiter {
    last_request: TokioMutex<tokio::time::Instant>,
}

impl CloudRateLimiter {
    fn new() -> Self {
        Self {
            last_request: TokioMutex::new(
                tokio::time::Instant::now() - std::time::Duration::from_secs(2),
            ),
        }
    }

    async fn wait(&self) {
        let mut last = self.last_request.lock().await;
        let elapsed = last.elapsed();
        let min_interval = std::time::Duration::from_millis(1000);
        if elapsed < min_interval {
            tokio::time::sleep(min_interval - elapsed).await;
        }
        *last = tokio::time::Instant::now();
    }
}

/// Shared counters for hybrid/parallel analysis progress
struct HybridCounters {
    games_done: AtomicU32,
    positions_analyzed: AtomicU32,
    cloud_hits: AtomicU32,
    engine_analyzed: AtomicU32,
    cache_hits: AtomicU32,
}

impl HybridCounters {
    fn new() -> Self {
        Self {
            games_done: AtomicU32::new(0),
            positions_analyzed: AtomicU32::new(0),
            cloud_hits: AtomicU32::new(0),
            engine_analyzed: AtomicU32::new(0),
            cache_hits: AtomicU32::new(0),
        }
    }
}

/// Fetch cloud eval for hybrid mode with:
/// - 3s timeout
/// - Depth >= min_depth check
/// - HTTP 429 retry (wait 60s, retry once)
/// - Rate limiting via shared CloudRateLimiter
async fn fetch_cloud_eval_hybrid(
    client: &reqwest::Client,
    fen: &str,
    multipv: u16,
    min_depth: u32,
    rate_limiter: &CloudRateLimiter,
) -> Result<Option<CloudEvalResponse>, Error> {
    rate_limiter.wait().await;

    let url = format!(
        "https://lichess.org/api/cloud-eval?fen={}&multiPv={}",
        fen.replace(' ', "%20"),
        multipv
    );

    let result = client
        .get(&url)
        .header(
            "User-Agent",
            "EnCroissant/0.15.0 (https://github.com/franciscoBSalgueiro/en-croissant)",
        )
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await;

    let resp = match result {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            info!("Cloud eval timeout for FEN: {}", fen);
            return Ok(None);
        }
        Err(e) => {
            info!("Cloud eval network error: {}", e);
            return Ok(None);
        }
    };

    let status = resp.status();

    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        info!("Cloud eval rate limited (429), waiting 60s and retrying...");
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        rate_limiter.wait().await;
        let result2 = client
            .get(&url)
            .header(
                "User-Agent",
                "EnCroissant/0.15.0 (https://github.com/franciscoBSalgueiro/en-croissant)",
            )
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await;
        let resp2 = match result2 {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        if !resp2.status().is_success() {
            return Ok(None);
        }
        let data: CloudEvalResponse = match resp2.json().await {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        if data.depth < min_depth {
            return Ok(None);
        }
        return Ok(Some(data));
    }

    if status == reqwest::StatusCode::NOT_FOUND || !status.is_success() {
        return Ok(None);
    }

    let data: CloudEvalResponse = resp
        .json()
        .await
        .map_err(|e| Error::HttpError(e.to_string()))?;
    if data.depth < min_depth {
        return Ok(None);
    }
    Ok(Some(data))
}

/// Minimum player move index to start analyzing. move_number <= 4 means skip
/// the first 4 player moves (~8 half-moves). Catches opening theory.
const MIN_PLAYER_MOVE_NUMBER: i32 = 5;

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MistakePuzzle {
    pub id: i64,
    pub source: String,
    pub username: String,
    pub game_id: String,
    pub fen: String,
    pub player_color: String,
    pub played_move: String,
    pub best_move: String,
    pub best_line: String,
    pub opponent_punishment: String,
    pub opponent_line: String,
    pub annotation: String,
    pub cp_loss: i32,
    pub win_chance_drop: f64,
    pub eval_before: String,
    pub eval_after: String,
    pub move_number: i32,
    pub engine_depth: i32,
    pub date_analyzed: String,
    pub completed: i32, // 0=unsolved, 1=correct, 2=wrong
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MistakeStats {
    pub total: i64,
    pub solved_correct: i64,
    pub solved_wrong: i64,
    pub unsolved: i64,
    pub blunders: i64,
    pub mistakes: i64,
    pub inaccuracies: i64,
    pub accuracy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MistakeAnalysisProgress {
    pub games_done: u32,
    pub games_total: u32,
    pub positions_analyzed: u32,
    pub cloud_hits: u32,
    pub engine_analyzed: u32,
    pub cache_hits: u32,
    pub estimated_seconds_left: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MistakePuzzleFilter {
    pub username: String,
    pub source: Option<String>,
    pub annotation: Option<String>,
    pub completed: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

// ── SQLite table management ─────────────────────────────────────────────────

const CREATE_MISTAKE_PUZZLES_TABLE: &str = "
CREATE TABLE IF NOT EXISTS mistake_puzzles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    username TEXT NOT NULL,
    game_id TEXT NOT NULL,
    fen TEXT NOT NULL,
    player_color TEXT NOT NULL,
    played_move TEXT NOT NULL,
    best_move TEXT NOT NULL,
    best_line TEXT NOT NULL DEFAULT '',
    opponent_punishment TEXT NOT NULL DEFAULT '',
    opponent_line TEXT NOT NULL DEFAULT '',
    annotation TEXT NOT NULL,
    cp_loss INTEGER NOT NULL,
    win_chance_drop REAL NOT NULL,
    eval_before TEXT NOT NULL,
    eval_after TEXT NOT NULL,
    move_number INTEGER NOT NULL,
    engine_depth INTEGER NOT NULL,
    date_analyzed TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    predecessor_fen TEXT NOT NULL DEFAULT '',
    predecessor_move TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_mp_username ON mistake_puzzles(username);
CREATE INDEX IF NOT EXISTS idx_mp_source ON mistake_puzzles(source);
CREATE INDEX IF NOT EXISTS idx_mp_annotation ON mistake_puzzles(annotation);
CREATE INDEX IF NOT EXISTS idx_mp_completed ON mistake_puzzles(completed);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mp_dedup ON mistake_puzzles(username, source, game_id, fen, played_move);
";

fn ensure_table(conn: &rusqlite::Connection) -> Result<(), Error> {
    conn.execute_batch(CREATE_MISTAKE_PUZZLES_TABLE)?;
    // Migration: add predecessor columns if table existed before this update
    let _ = conn.execute_batch(
        "ALTER TABLE mistake_puzzles ADD COLUMN predecessor_fen TEXT NOT NULL DEFAULT '';
         ALTER TABLE mistake_puzzles ADD COLUMN predecessor_move TEXT NOT NULL DEFAULT '';",
    );
    Ok(())
}

fn open_db(path: &str) -> Result<rusqlite::Connection, Error> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    ensure_table(&conn)?;
    Ok(conn)
}

// ── Score helpers (mirror the TS logic) ─────────────────────────────────────

fn get_win_chance(cp: f64) -> f64 {
    50.0 + 50.0 * (2.0 / (1.0 + (-0.00368208 * cp).exp()) - 1.0)
}

/// Convert an engine score to centipawns from the PLAYER's perspective.
/// `side_to_move` is whose turn it is in the position the engine evaluated.
/// `player_color` is the color the human player was playing.
///
/// The engine always reports from side-to-move's perspective:
///   positive = good for side-to-move, negative = bad for side-to-move.
///
/// If side_to_move == player_color, score is already from player's view.
/// If side_to_move != player_color (opponent's turn), negate to get player's view.
fn score_from_player_perspective(
    score: &vampirc_uci::uci::Score,
    side_to_move: Color,
    player_color: Color,
) -> f64 {
    use vampirc_uci::uci::ScoreValue;
    let raw = match score.value {
        ScoreValue::Cp(cp) => cp as f64,
        ScoreValue::Mate(m) => {
            if m > 0 {
                100_000.0
            } else {
                -100_000.0
            }
        }
    };
    let cp = if side_to_move == player_color {
        raw
    } else {
        -raw
    };
    cp.clamp(-10000.0, 10000.0)
}

fn format_eval(score: &vampirc_uci::uci::Score) -> String {
    use vampirc_uci::uci::ScoreValue;
    match score.value {
        ScoreValue::Cp(cp) => format!("{:.2}", cp as f64 / 100.0),
        ScoreValue::Mate(m) => format!("M{}", m),
    }
}

fn classify_annotation(win_chance_drop: f64) -> &'static str {
    if win_chance_drop > 20.0 {
        "??"
    } else if win_chance_drop > 10.0 {
        "?"
    } else if win_chance_drop > 5.0 {
        "?!"
    } else {
        ""
    }
}

// ── Batch analysis command ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzeGamesRequest {
    pub id: String,
    pub engine: String,
    pub engine_type: String, // "local", "lichess", or "hybrid"
    pub go_mode: GoMode,
    pub uci_options: Vec<EngineOption>,
    pub db_path: String,
    pub mistake_db_path: String,
    pub username: String,
    pub source: String,
    pub min_win_chance_drop: f64,
}

#[tauri::command]
#[specta::specta]
pub async fn analyze_games_for_mistakes(
    req: AnalyzeGamesRequest,
    state: tauri::State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<MistakeStats, Error> {
    let id = req.id;
    let engine = req.engine;
    let engine_type = req.engine_type;
    let go_mode = req.go_mode;
    let uci_options = req.uci_options;
    let db_path = req.db_path;
    let mistake_db_path = req.mistake_db_path;
    let username = req.username;
    let source = req.source;
    let min_win_chance_drop = req.min_win_chance_drop;
    let cancel_flag = Arc::new(AtomicBool::new(false));
    state
        .analysis_cancel_flags
        .insert(id.clone(), cancel_flag.clone());

    // Open the games database (diesel/sqlite)
    let mut games_conn =
        diesel::SqliteConnection::establish(&db_path).map_err(|_| Error::NoPuzzles)?;

    // Query all games for this player
    let games = find_player_games(&mut games_conn, &username)?;
    let total_games = games.len() as i32;

    info!(
        "Starting mistake analysis ({}): {} games for {} from {}",
        engine_type, total_games, username, source
    );

    let mut pending_mistakes: Vec<PendingMistakePuzzle> = Vec::new();

    if engine_type == "lichess" {
        // ── Cloud-only analysis via Lichess API ─────────────────────────
        let client = reqwest::Client::new();

        for (game_idx, game) in games.iter().enumerate() {
            if cancel_flag.load(Ordering::SeqCst) {
                info!("Analysis cancelled at game {}/{}", game_idx, total_games);
                break;
            }

            update_progress(
                &state.progress_state,
                &app,
                id.clone(),
                (game_idx as f32 / total_games as f32) * 100.0,
                false,
            )?;

            let result = analyze_single_game_cloud(
                &client,
                game,
                &username,
                &source,
                min_win_chance_drop,
                |pos_fraction: f32| {
                    let overall = ((game_idx as f32 + pos_fraction) / total_games as f32) * 100.0;
                    let _ =
                        update_progress(&state.progress_state, &app, id.clone(), overall, false);
                },
            )
            .await;

            match result {
                Ok(game_mistakes) => {
                    info!(
                        "Game {}: found {} mistakes (cloud)",
                        game.id,
                        game_mistakes.len()
                    );
                    pending_mistakes.extend(game_mistakes);
                }
                Err(e) => {
                    info!("Skipping game {}: {}", game.id, e);
                    continue;
                }
            }
        }

        state.analysis_cancel_flags.remove(&id);
        update_progress(&state.progress_state, &app, id, 100.0, true)?;
    } else if engine_type == "hybrid" {
        // ── Hybrid analysis: cloud → local engine fallback, parallel ────
        let client = Arc::new(reqwest::Client::new());
        let rate_limiter = Arc::new(CloudRateLimiter::new());
        let fen_cache: FenCache = Arc::new(TokioMutex::new(HashMap::new()));
        let counters = Arc::new(HybridCounters::new());
        let cancel_flag_clone = cancel_flag.clone();

        // Wrap games in Arc for sharing across tasks
        let games: Arc<Vec<GameRecord>> = Arc::new(games);
        let total_games_u32 = total_games as u32;

        // Concurrency: up to 4 games at a time
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        let all_mistakes: Arc<TokioMutex<Vec<PendingMistakePuzzle>>> =
            Arc::new(TokioMutex::new(Vec::new()));

        let start_time = tokio::time::Instant::now();

        let mut handles = Vec::new();

        for (game_idx, _) in games.iter().enumerate() {
            let sem = semaphore.clone();
            let client = client.clone();
            let rate_limiter = rate_limiter.clone();
            let fen_cache = fen_cache.clone();
            let counters = counters.clone();
            let cancel = cancel_flag_clone.clone();
            let all_mistakes = all_mistakes.clone();
            let games = games.clone();
            let username = username.clone();
            let source = source.clone();
            let engine_path = engine.clone();
            let go_mode = go_mode.clone();
            let uci_options = uci_options.clone();
            let progress_state = state.progress_state.clone();
            let app_handle = app.clone();
            let progress_id = id.clone();

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                if cancel.load(Ordering::SeqCst) {
                    return;
                }

                let game = &games[game_idx];

                let result = analyze_single_game_hybrid(
                    &client,
                    &rate_limiter,
                    &fen_cache,
                    &counters,
                    game,
                    &username,
                    &source,
                    &engine_path,
                    &go_mode,
                    &uci_options,
                    min_win_chance_drop,
                    &cancel,
                )
                .await;

                match result {
                    Ok(game_mistakes) => {
                        info!(
                            "Game {} (hybrid): found {} mistakes",
                            game.id,
                            game_mistakes.len()
                        );
                        all_mistakes.lock().await.extend(game_mistakes);
                    }
                    Err(e) => {
                        info!("Skipping game {} (hybrid): {}", game.id, e);
                    }
                }

                let done = counters.games_done.fetch_add(1, Ordering::SeqCst) + 1;
                let overall = (done as f32 / total_games_u32 as f32) * 100.0;

                let elapsed_secs = start_time.elapsed().as_secs() as u32;
                let est_left = if done > 0 {
                    let per_game = elapsed_secs as f64 / done as f64;
                    let remaining = total_games_u32.saturating_sub(done);
                    (per_game * remaining as f64) as u32
                } else {
                    0
                };

                // Emit progress
                let _ = update_progress(
                    &progress_state,
                    &app_handle,
                    progress_id.clone(),
                    overall,
                    false,
                );
            });

            handles.push(handle);
        }

        // Wait for all game tasks to complete
        for handle in handles {
            let _ = handle.await;
        }

        pending_mistakes = all_mistakes.lock().await.clone();

        info!(
            "Hybrid analysis complete: {} games, {} cloud hits, {} engine analyzed, {} cache hits, {} mistakes",
            counters.games_done.load(Ordering::SeqCst),
            counters.cloud_hits.load(Ordering::SeqCst),
            counters.engine_analyzed.load(Ordering::SeqCst),
            counters.cache_hits.load(Ordering::SeqCst),
            pending_mistakes.len(),
        );

        state.analysis_cancel_flags.remove(&id);
        update_progress(&state.progress_state, &app, id, 100.0, true)?;
    } else {
        // ── Local engine analysis (sequential) ──────────────────────────
        let engine_path = PathBuf::from(&engine);
        let mut proc = BaseEngine::spawn(engine_path).await?;
        proc.init_uci().await?;
        let mut reader = proc.take_reader().ok_or(Error::EngineDisconnected)?;

        // Set UCI options
        for opt in &uci_options {
            if opt.name != "MultiPV" && opt.name != "UCI_Chess960" {
                proc.set_option(&opt.name, &opt.value).await?;
            }
        }
        // Force MultiPV=2
        proc.set_option("MultiPV", "2").await?;

        for (game_idx, game) in games.iter().enumerate() {
            if cancel_flag.load(Ordering::SeqCst) {
                info!("Analysis cancelled at game {}/{}", game_idx, total_games);
                break;
            }

            update_progress(
                &state.progress_state,
                &app,
                id.clone(),
                (game_idx as f32 / total_games as f32) * 100.0,
                false,
            )?;

            let result = analyze_single_game(
                &mut proc,
                &mut reader,
                game,
                &username,
                &source,
                &go_mode,
                min_win_chance_drop,
                |pos_fraction: f32| {
                    let overall = ((game_idx as f32 + pos_fraction) / total_games as f32) * 100.0;
                    let _ =
                        update_progress(&state.progress_state, &app, id.clone(), overall, false);
                },
            )
            .await;

            match result {
                Ok(game_mistakes) => {
                    info!("Game {}: found {} mistakes", game.id, game_mistakes.len());
                    pending_mistakes.extend(game_mistakes);
                }
                Err(e) => {
                    info!("Skipping game {}: {}", game.id, e);
                    continue;
                }
            }
        }

        // Cleanup
        proc.quit().await.ok();
        state.analysis_cancel_flags.remove(&id);
        update_progress(&state.progress_state, &app, id, 100.0, true)?;
    }

    let mistake_conn = open_db(&mistake_db_path)?;

    // Clear old puzzles for this user+source before inserting fresh results
    mistake_conn.execute(
        "DELETE FROM mistake_puzzles WHERE username = ?1 AND source = ?2",
        rusqlite::params![&username, &source],
    )?;
    info!(
        "Cleared old mistake puzzles for {} / {}. Inserting {} new mistakes.",
        username,
        source,
        pending_mistakes.len()
    );

    insert_pending_mistakes(&mistake_conn, &pending_mistakes)?;

    // Return stats
    get_stats_from_db(&mistake_conn, &username, &source)
}

// ── Game data from the en-croissant DB ──────────────────────────────────────

use diesel::prelude::*;

struct GameRecord {
    id: i32,
    fen: Option<String>,
    moves: Vec<u8>,
    white: String,
    black: String,
    white_elo: Option<i32>,
    black_elo: Option<i32>,
    date: Option<String>,
    site: Option<String>,
}

#[derive(Clone)]
struct PendingMistakePuzzle {
    source: String,
    username: String,
    game_id: String,
    fen: String,
    player_color: String,
    played_move: String,
    best_move: String,
    best_line: String,
    opponent_punishment: String,
    opponent_line: String,
    annotation: String,
    cp_loss: i32,
    win_chance_drop: f64,
    eval_before: String,
    eval_after: String,
    move_number: i32,
    engine_depth: i32,
    date_analyzed: String,
    predecessor_fen: String,
    predecessor_move: String,
}

fn insert_pending_mistakes(
    conn: &rusqlite::Connection,
    items: &[PendingMistakePuzzle],
) -> Result<(), Error> {
    for item in items {
        conn.execute(
            "INSERT OR IGNORE INTO mistake_puzzles (
                source, username, game_id, fen, player_color, played_move,
                best_move, best_line, opponent_punishment, opponent_line,
                annotation, cp_loss, win_chance_drop, eval_before, eval_after,
                move_number, engine_depth, date_analyzed, completed,
                predecessor_fen, predecessor_move
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 0, ?19, ?20)",
            rusqlite::params![
                &item.source,
                &item.username,
                &item.game_id,
                &item.fen,
                &item.player_color,
                &item.played_move,
                &item.best_move,
                &item.best_line,
                &item.opponent_punishment,
                &item.opponent_line,
                &item.annotation,
                item.cp_loss,
                item.win_chance_drop,
                &item.eval_before,
                &item.eval_after,
                item.move_number,
                item.engine_depth,
                &item.date_analyzed,
                &item.predecessor_fen,
                &item.predecessor_move,
            ],
        )?;
    }

    Ok(())
}

fn find_player_games(
    conn: &mut diesel::SqliteConnection,
    username: &str,
) -> Result<Vec<GameRecord>, Error> {
    use crate::db::schema::{games, players, sites};

    // Find player IDs matching the username (case-insensitive)
    let lower_username = username.to_lowercase();
    let player_ids: Vec<i32> = players::table
        .filter(
            diesel::dsl::sql::<diesel::sql_types::Bool>("LOWER(\"Name\") = ")
                .bind::<diesel::sql_types::Text, _>(&lower_username),
        )
        .select(players::id)
        .load(conn)?;

    if player_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Get all games where this player was white or black
    let raw_games: Vec<(
        i32,
        Option<String>,
        Vec<u8>,
        i32,
        i32,
        Option<i32>,
        Option<i32>,
        Option<String>,
        i32,
    )> = games::table
        .filter(
            games::white_id
                .eq_any(&player_ids)
                .or(games::black_id.eq_any(&player_ids)),
        )
        .select((
            games::id,
            games::fen,
            games::moves,
            games::white_id,
            games::black_id,
            games::white_elo,
            games::black_elo,
            games::date,
            games::site_id,
        ))
        .load(conn)?;

    let mut result = Vec::new();
    // Cache player and site names to avoid N+1 queries
    let mut player_cache: std::collections::HashMap<i32, String> = std::collections::HashMap::new();
    let mut site_cache: std::collections::HashMap<i32, String> = std::collections::HashMap::new();

    for (gid, fen, moves, white_id, black_id, white_elo, black_elo, date, site_id) in raw_games {
        let white_name = if let Some(name) = player_cache.get(&white_id) {
            name.clone()
        } else {
            let name = players::table
                .find(white_id)
                .select(players::name)
                .first::<Option<String>>(conn)
                .ok()
                .flatten()
                .unwrap_or_default();
            player_cache.insert(white_id, name.clone());
            name
        };
        let black_name = if let Some(name) = player_cache.get(&black_id) {
            name.clone()
        } else {
            let name = players::table
                .find(black_id)
                .select(players::name)
                .first::<Option<String>>(conn)
                .ok()
                .flatten()
                .unwrap_or_default();
            player_cache.insert(black_id, name.clone());
            name
        };
        let site_name = if let Some(name) = site_cache.get(&site_id) {
            name.clone()
        } else {
            let name = sites::table
                .find(site_id)
                .select(sites::name)
                .first::<Option<String>>(conn)
                .ok()
                .flatten()
                .unwrap_or_default();
            site_cache.insert(site_id, name.clone());
            name
        };

        result.push(GameRecord {
            id: gid,
            fen,
            moves,
            white: white_name,
            black: black_name,
            white_elo,
            black_elo,
            date,
            site: Some(site_name),
        });
    }

    Ok(result)
}

// ── Single game analysis ────────────────────────────────────────────────────

async fn analyze_single_game(
    proc: &mut BaseEngine,
    reader: &mut EngineReader,
    game: &GameRecord,
    username: &str,
    source: &str,
    go_mode: &GoMode,
    min_win_chance_drop: f64,
    on_position_progress: impl Fn(f32),
) -> Result<Vec<PendingMistakePuzzle>, Error> {
    let initial_fen = game
        .fen
        .as_deref()
        .unwrap_or("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");

    let fen: Fen = initial_fen.parse()?;
    let setup = fen.as_setup().clone();
    let castling_mode = CastlingMode::detect(&setup);
    let mut chess: Chess = Chess::from_setup(setup, castling_mode)
        .or_else(PositionError::ignore_too_much_material)
        .map_err(|e| Error::ChessPosition(Box::new(e)))?;

    // Determine player color
    let player_is_white = username.eq_ignore_ascii_case(&game.white);
    let player_color = if player_is_white {
        Color::White
    } else {
        Color::Black
    };
    let color_str = if player_is_white { "white" } else { "black" };

    // Decode all mainline moves to UCI strings, tracking predecessor info
    // Each entry: (fen_before, moves_before, played_uci, predecessor_fen, predecessor_move)
    let mut positions: Vec<(Fen, Vec<String>, String, Option<String>, Option<String>)> = Vec::new();
    let mut uci_moves_so_far: Vec<String> = Vec::new();
    let mut last_opponent_fen: Option<String> = None;
    let mut last_opponent_move: Option<String> = None;

    let mut move_count = 0u32;
    for move_byte in iter_mainline_move_bytes(&game.moves) {
        let fen_before = Fen::from_position(chess.clone(), EnPassantMode::Legal);
        let turn_before = chess.turn();
        move_count += 1;

        if let Some(m) = decode_move(move_byte, &chess) {
            let uci = UciMove::from_move(&m, castling_mode).to_string();

            if turn_before == player_color {
                // Player's turn — record position with predecessor info
                positions.push((
                    fen_before,
                    uci_moves_so_far.clone(),
                    uci.clone(),
                    last_opponent_fen.take(),
                    last_opponent_move.take(),
                ));
            } else {
                // Opponent's turn — save as predecessor for the next player position
                last_opponent_fen = Some(fen_before.to_string());
                last_opponent_move = Some(uci.clone());
            }

            chess.play_unchecked(&m);
            uci_moves_so_far.push(uci);
        } else {
            info!(
                "Game {}: move decode failed at ply {} (half-move {}), skipping rest of game ({} positions collected so far)",
                game.id, move_count, move_count, positions.len()
            );
            break;
        }
    }

    let game_id = game
        .site
        .as_deref()
        .unwrap_or(&game.id.to_string())
        .to_string();

    info!(
        "Game {} ({}): decoded {} plies, {} player positions to analyze",
        game.id,
        game_id,
        move_count,
        positions.len()
    );

    let mut mistakes_found: Vec<PendingMistakePuzzle> = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();

    let total_positions = positions.len();

    for (pos_idx, (fen_before, moves_before, played_uci, pred_fen, pred_move)) in
        positions.iter().enumerate()
    {
        let move_number = (pos_idx as i32) + 1;

        // Emit position-level progress within this game
        if total_positions > 0 {
            on_position_progress(pos_idx as f32 / total_positions as f32);
        }

        // Set position and run engine
        proc.set_position(&initial_fen, moves_before).await?;
        proc.go(go_mode).await?;

        let mut best_lines: Vec<BestMoves> = Vec::new();
        let mut current_batch: Vec<BestMoves> = Vec::new();
        let mut last_depth = 0u32;

        while let Ok(Some(line)) = reader.next_line().await {
            match parse_one(&line) {
                UciMessage::Info(attrs) => {
                    if let Ok(bm) =
                        parse_uci_attrs(attrs, &fen_before.to_string().parse()?, moves_before)
                    {
                        if bm.score.lower_bound == Some(true) || bm.score.upper_bound == Some(true)
                        {
                            continue;
                        }
                        let multipv = bm.multipv;
                        let cur_depth = bm.depth;
                        if multipv as usize == current_batch.len() + 1 {
                            current_batch.push(bm);
                            let expected = 2u16.min(
                                Fen::from_ascii(fen_before.to_string().as_bytes())
                                    .ok()
                                    .and_then(|f| {
                                        let s = f.into_setup();
                                        let cm = CastlingMode::detect(&s);
                                        Chess::from_setup(s, cm)
                                            .or_else(PositionError::ignore_too_much_material)
                                            .ok()
                                    })
                                    .map(|p| p.legal_moves().len() as u16)
                                    .unwrap_or(2),
                            );
                            if multipv >= expected {
                                if current_batch.iter().all(|x| x.depth == cur_depth)
                                    && cur_depth >= last_depth
                                {
                                    best_lines = current_batch.clone();
                                    last_depth = cur_depth;
                                }
                                current_batch.clear();
                            }
                        }
                    }
                }
                UciMessage::BestMove { .. } => break,
                _ => {}
            }
        }

        if best_lines.is_empty() {
            continue;
        }

        // The engine's best move
        let engine_best_uci = best_lines
            .first()
            .and_then(|b| b.uci_moves.first())
            .cloned()
            .unwrap_or_default();
        let engine_best_line = best_lines
            .first()
            .map(|b| b.uci_moves.join(" "))
            .unwrap_or_default();

        // Score of the position when playing the engine's best move
        // Before move: side-to-move = player_color
        let eval_before_score = &best_lines[0].score;
        let eval_before_cp =
            score_from_player_perspective(eval_before_score, player_color, player_color);

        // Did the player play the engine's best move?
        if played_uci == &engine_best_uci {
            continue; // Good move, skip
        }

        // We need to find the eval of the position AFTER the player's actual move.
        // Run engine on the position after the played move.
        let mut moves_after_played = moves_before.clone();
        moves_after_played.push(played_uci.clone());

        proc.set_position(initial_fen, &moves_after_played).await?;
        proc.go(go_mode).await?;

        // Pre-compute the FEN after the played move (for parsing engine output)
        let fen_after_str = {
            let f: Fen = initial_fen.parse()?;
            let s = f.into_setup();
            let cm = CastlingMode::detect(&s);
            let mut pos = Chess::from_setup(s, cm)
                .or_else(PositionError::ignore_too_much_material)
                .map_err(|e| Error::ChessPosition(Box::new(e)))?;
            for m_str in &moves_after_played {
                let uci: UciMove = m_str.parse()?;
                let m = uci.to_move(&pos)?;
                pos.play_unchecked(&m);
            }
            Fen::from_position(pos, EnPassantMode::Legal).to_string()
        };

        let mut after_lines: Vec<BestMoves> = Vec::new();
        let mut current_batch2: Vec<BestMoves> = Vec::new();
        let mut last_depth2 = 0u32;

        while let Ok(Some(line)) = reader.next_line().await {
            match parse_one(&line) {
                UciMessage::Info(attrs) => {
                    if let Ok(bm) = parse_uci_attrs(attrs, &fen_after_str.parse()?, &[]) {
                        if bm.score.lower_bound == Some(true) || bm.score.upper_bound == Some(true)
                        {
                            continue;
                        }
                        let multipv = bm.multipv;
                        let cur_depth = bm.depth;
                        if multipv as usize == current_batch2.len() + 1 {
                            current_batch2.push(bm);
                            if multipv >= 1 {
                                if current_batch2.iter().all(|x| x.depth == cur_depth)
                                    && cur_depth >= last_depth2
                                {
                                    after_lines = current_batch2.clone();
                                    last_depth2 = cur_depth;
                                }
                                current_batch2.clear();
                            }
                        }
                    }
                }
                UciMessage::BestMove { .. } => break,
                _ => {}
            }
        }

        if after_lines.is_empty() {
            continue;
        }

        // Eval after the player's move: side-to-move = opponent
        let eval_after_score = &after_lines[0].score;
        let opponent_color = if player_color == Color::White {
            Color::Black
        } else {
            Color::White
        };
        let eval_after_cp =
            score_from_player_perspective(eval_after_score, opponent_color, player_color);

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;

        if win_chance_drop < min_win_chance_drop {
            continue;
        }

        let annotation = classify_annotation(win_chance_drop);
        if annotation.is_empty() {
            continue;
        }

        let cp_loss = (eval_before_cp - eval_after_cp).max(0.0) as i32;
        let engine_depth = last_depth as i32;

        // Opponent punishment: the best response after the bad move
        let opponent_punishment = after_lines
            .first()
            .and_then(|b| b.uci_moves.first())
            .cloned()
            .unwrap_or_default();
        let opponent_line = after_lines
            .first()
            .map(|b| b.uci_moves.join(" "))
            .unwrap_or_default();

        // FEN at the position where the mistake was made
        let fen_str = fen_before.to_string();

        mistakes_found.push(PendingMistakePuzzle {
            source: source.to_string(),
            username: username.to_string(),
            game_id: game_id.clone(),
            fen: fen_str,
            player_color: color_str.to_string(),
            played_move: played_uci.clone(),
            best_move: engine_best_uci,
            best_line: engine_best_line,
            opponent_punishment,
            opponent_line,
            annotation: annotation.to_string(),
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(eval_before_score),
            eval_after: format_eval(eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
        });
    }

    Ok(mistakes_found)
}

// ── Cloud-based single game analysis (Lichess API) ──────────────────────────

async fn analyze_single_game_cloud(
    client: &reqwest::Client,
    game: &GameRecord,
    username: &str,
    source: &str,
    min_win_chance_drop: f64,
    on_position_progress: impl Fn(f32),
) -> Result<Vec<PendingMistakePuzzle>, Error> {
    let initial_fen = game
        .fen
        .as_deref()
        .unwrap_or("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");

    let fen: Fen = initial_fen.parse()?;
    let setup = fen.as_setup().clone();
    let castling_mode = CastlingMode::detect(&setup);
    let mut chess: Chess = Chess::from_setup(setup, castling_mode)
        .or_else(PositionError::ignore_too_much_material)
        .map_err(|e| Error::ChessPosition(Box::new(e)))?;

    // Determine player color
    let player_is_white = username.eq_ignore_ascii_case(&game.white);
    let player_color = if player_is_white {
        Color::White
    } else {
        Color::Black
    };
    let opponent_color = if player_is_white {
        Color::Black
    } else {
        Color::White
    };
    let color_str = if player_is_white { "white" } else { "black" };

    // Decode all mainline moves, tracking positions and predecessor info
    // (fen_before_str, played_uci, predecessor_fen, predecessor_move)
    let mut positions: Vec<(String, String, Option<String>, Option<String>)> = Vec::new();
    let mut last_opponent_fen: Option<String> = None;
    let mut last_opponent_move: Option<String> = None;

    let mut move_count = 0u32;
    for move_byte in iter_mainline_move_bytes(&game.moves) {
        let fen_before = Fen::from_position(chess.clone(), EnPassantMode::Legal);
        let turn_before = chess.turn();
        move_count += 1;

        if let Some(m) = decode_move(move_byte, &chess) {
            let uci = UciMove::from_move(&m, castling_mode).to_string();

            if turn_before == player_color {
                positions.push((
                    fen_before.to_string(),
                    uci.clone(),
                    last_opponent_fen.take(),
                    last_opponent_move.take(),
                ));
            } else {
                last_opponent_fen = Some(fen_before.to_string());
                last_opponent_move = Some(uci.clone());
            }

            chess.play_unchecked(&m);
        } else {
            info!(
                "Game {} (cloud): move decode failed at ply {}, skipping rest ({} positions so far)",
                game.id, move_count, positions.len()
            );
            break;
        }
    }

    let game_id = game
        .site
        .as_deref()
        .unwrap_or(&game.id.to_string())
        .to_string();

    info!(
        "Game {} (cloud): decoded {} plies, {} player positions to analyze",
        game.id,
        move_count,
        positions.len()
    );

    let mut mistakes_found: Vec<PendingMistakePuzzle> = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();
    let total_positions = positions.len();

    for (pos_idx, (fen_str, played_uci, pred_fen, pred_move)) in positions.iter().enumerate() {
        let move_number = (pos_idx as i32) + 1;

        if total_positions > 0 {
            on_position_progress(pos_idx as f32 / total_positions as f32);
        }

        // Fetch cloud eval for the position BEFORE the player's move
        let before_eval = match fetch_cloud_eval(client, fen_str, 2).await? {
            Some(eval) => eval,
            None => continue, // Position not in cloud DB, skip
        };

        if before_eval.pvs.is_empty() {
            continue;
        }

        // Engine's best move from cloud
        let cloud_best_moves_str = &before_eval.pvs[0].moves;
        let cloud_best_uci = cloud_best_moves_str
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        let cloud_best_line = cloud_best_moves_str.to_string();

        // Eval before: cloud reports from side-to-move's perspective
        let eval_before_score = cloud_pv_to_score(&before_eval.pvs[0]);
        let eval_before_cp =
            score_from_player_perspective(&eval_before_score, player_color, player_color);

        // Did the player play the cloud's best move?
        if played_uci == &cloud_best_uci {
            continue; // Good move
        }

        // Compute FEN after the player's actual move
        let fen_after_str = {
            let f: Fen = fen_str.parse()?;
            let s = f.into_setup();
            let cm = CastlingMode::detect(&s);
            let mut pos = Chess::from_setup(s, cm)
                .or_else(PositionError::ignore_too_much_material)
                .map_err(|e| Error::ChessPosition(Box::new(e)))?;
            let uci: UciMove = played_uci.parse()?;
            let m = uci.to_move(&pos)?;
            pos.play_unchecked(&m);
            Fen::from_position(pos, EnPassantMode::Legal).to_string()
        };

        // Fetch cloud eval for position AFTER the player's move
        let after_eval = match fetch_cloud_eval(client, &fen_after_str, 1).await? {
            Some(eval) => eval,
            None => continue, // Position after move not in cloud DB
        };

        if after_eval.pvs.is_empty() {
            continue;
        }

        // Eval after: side-to-move = opponent
        let eval_after_score = cloud_pv_to_score(&after_eval.pvs[0]);
        let eval_after_cp =
            score_from_player_perspective(&eval_after_score, opponent_color, player_color);

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;

        if win_chance_drop < min_win_chance_drop {
            continue;
        }

        let annotation = classify_annotation(win_chance_drop);
        if annotation.is_empty() {
            continue;
        }

        let cp_loss = (eval_before_cp - eval_after_cp).max(0.0) as i32;
        let engine_depth = before_eval.depth as i32;

        // Opponent's best response after the bad move
        let opponent_punishment = after_eval.pvs[0]
            .moves
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        let opponent_line = after_eval.pvs[0].moves.clone();

        mistakes_found.push(PendingMistakePuzzle {
            source: source.to_string(),
            username: username.to_string(),
            game_id: game_id.clone(),
            fen: fen_str.clone(),
            player_color: color_str.to_string(),
            played_move: played_uci.clone(),
            best_move: cloud_best_uci,
            best_line: cloud_best_line,
            opponent_punishment,
            opponent_line,
            annotation: annotation.to_string(),
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(&eval_before_score),
            eval_after: format_eval(&eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
        });
    }

    Ok(mistakes_found)
}

// ── Hybrid single-game analysis (cloud → local engine fallback) ─────────────

async fn analyze_single_game_hybrid(
    client: &reqwest::Client,
    rate_limiter: &CloudRateLimiter,
    fen_cache: &FenCache,
    counters: &HybridCounters,
    game: &GameRecord,
    username: &str,
    source: &str,
    engine_path: &str,
    go_mode: &GoMode,
    uci_options: &[EngineOption],
    min_win_chance_drop: f64,
    cancel_flag: &AtomicBool,
) -> Result<Vec<PendingMistakePuzzle>, Error> {
    let initial_fen = game
        .fen
        .as_deref()
        .unwrap_or("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");

    let fen: Fen = initial_fen.parse()?;
    let setup = fen.as_setup().clone();
    let castling_mode = CastlingMode::detect(&setup);
    let mut chess: Chess = Chess::from_setup(setup, castling_mode)
        .or_else(PositionError::ignore_too_much_material)
        .map_err(|e| Error::ChessPosition(Box::new(e)))?;

    let player_is_white = username.eq_ignore_ascii_case(&game.white);
    let player_color = if player_is_white {
        Color::White
    } else {
        Color::Black
    };
    let opponent_color = if player_is_white {
        Color::Black
    } else {
        Color::White
    };
    let color_str = if player_is_white { "white" } else { "black" };

    // Decode all mainline moves, collecting player positions
    // (fen_str, played_uci, moves_before, predecessor_fen, predecessor_move)
    let mut positions: Vec<(String, String, Vec<String>, Option<String>, Option<String>)> =
        Vec::new();
    let mut uci_moves_so_far: Vec<String> = Vec::new();
    let mut last_opponent_fen: Option<String> = None;
    let mut last_opponent_move: Option<String> = None;

    let mut move_count = 0u32;
    for move_byte in iter_mainline_move_bytes(&game.moves) {
        let fen_before = Fen::from_position(chess.clone(), EnPassantMode::Legal);
        let turn_before = chess.turn();
        move_count += 1;

        if let Some(m) = decode_move(move_byte, &chess) {
            let uci = UciMove::from_move(&m, castling_mode).to_string();

            if turn_before == player_color {
                positions.push((
                    fen_before.to_string(),
                    uci.clone(),
                    uci_moves_so_far.clone(),
                    last_opponent_fen.take(),
                    last_opponent_move.take(),
                ));
            } else {
                last_opponent_fen = Some(fen_before.to_string());
                last_opponent_move = Some(uci.clone());
            }

            chess.play_unchecked(&m);
            uci_moves_so_far.push(uci);
        } else {
            info!(
                "Game {} (hybrid): decode failed at ply {}, {} positions so far",
                game.id,
                move_count,
                positions.len()
            );
            break;
        }
    }

    let game_id = game
        .site
        .as_deref()
        .unwrap_or(&game.id.to_string())
        .to_string();

    let mut mistakes_found: Vec<PendingMistakePuzzle> = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();

    // Lazily spawn local engine only if needed
    let mut local_engine: Option<(BaseEngine, EngineReader)> = None;

    for (pos_idx, (fen_str, played_uci, moves_before, pred_fen, pred_move)) in
        positions.iter().enumerate()
    {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }

        let move_number = (pos_idx as i32) + 1;

        // Skip opening moves
        if move_number < MIN_PLAYER_MOVE_NUMBER {
            continue;
        }

        counters.positions_analyzed.fetch_add(1, Ordering::SeqCst);

        // Step 0: Check FEN cache
        {
            let cache = fen_cache.lock().await;
            if let Some(cached_before) = cache.get(fen_str) {
                // We have a cache hit for the "before" position
                let eval_before_cp =
                    score_from_player_perspective(&cached_before.score, player_color, player_color);

                if played_uci == &cached_before.best_uci {
                    counters.cache_hits.fetch_add(1, Ordering::SeqCst);
                    continue; // Good move
                }

                // Check if we have the "after" position cached too
                let fen_after_str = compute_fen_after(fen_str, played_uci)?;
                if let Some(cached_after) = cache.get(&fen_after_str) {
                    let eval_after_cp = score_from_player_perspective(
                        &cached_after.score,
                        opponent_color,
                        player_color,
                    );
                    let win_before = get_win_chance(eval_before_cp);
                    let win_after = get_win_chance(eval_after_cp);
                    let win_chance_drop = win_before - win_after;

                    counters.cache_hits.fetch_add(1, Ordering::SeqCst);

                    if win_chance_drop >= min_win_chance_drop {
                        let annotation = classify_annotation(win_chance_drop);
                        if !annotation.is_empty() {
                            let cp_loss = (eval_before_cp - eval_after_cp).max(0.0) as i32;
                            mistakes_found.push(PendingMistakePuzzle {
                                source: source.to_string(),
                                username: username.to_string(),
                                game_id: game_id.clone(),
                                fen: fen_str.clone(),
                                player_color: color_str.to_string(),
                                played_move: played_uci.clone(),
                                best_move: cached_before.best_uci.clone(),
                                best_line: cached_before.best_line.clone(),
                                opponent_punishment: cached_after.best_uci.clone(),
                                opponent_line: cached_after.best_line.clone(),
                                annotation: annotation.to_string(),
                                cp_loss,
                                win_chance_drop,
                                eval_before: format_eval(&cached_before.score),
                                eval_after: format_eval(&cached_after.score),
                                move_number,
                                engine_depth: cached_before.depth as i32,
                                date_analyzed: now.clone(),
                                predecessor_fen: pred_fen.clone().unwrap_or_default(),
                                predecessor_move: pred_move.clone().unwrap_or_default(),
                            });
                        }
                    }
                    continue;
                }
                // Only "before" was cached, fall through to analyze "after"
            }
        }

        // Step 1: Try Lichess Cloud Eval (min depth 20 for hybrid)
        let cloud_result = fetch_cloud_eval_hybrid(client, fen_str, 2, 20, rate_limiter).await?;

        if let Some(ref cloud_data) = cloud_result {
            if !cloud_data.pvs.is_empty() {
                counters.cloud_hits.fetch_add(1, Ordering::SeqCst);

                let eval_before_score = cloud_pv_to_score(&cloud_data.pvs[0]);
                let eval_before_cp =
                    score_from_player_perspective(&eval_before_score, player_color, player_color);

                let cloud_best_uci = cloud_data.pvs[0]
                    .moves
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_string();
                let cloud_best_line = cloud_data.pvs[0].moves.clone();

                // Cache the before eval
                {
                    let mut cache = fen_cache.lock().await;
                    cache.insert(
                        fen_str.clone(),
                        CachedEval {
                            best_uci: cloud_best_uci.clone(),
                            best_line: cloud_best_line.clone(),
                            score: eval_before_score.clone(),
                            depth: cloud_data.depth,
                        },
                    );
                }

                if played_uci == &cloud_best_uci {
                    continue; // Good move
                }

                // Get eval for position after the played move (also try cloud)
                let fen_after_str = compute_fen_after(fen_str, played_uci)?;
                let after_cloud =
                    fetch_cloud_eval_hybrid(client, &fen_after_str, 1, 20, rate_limiter).await?;

                if let Some(ref after_data) = after_cloud {
                    if !after_data.pvs.is_empty() {
                        let eval_after_score = cloud_pv_to_score(&after_data.pvs[0]);
                        let eval_after_cp = score_from_player_perspective(
                            &eval_after_score,
                            opponent_color,
                            player_color,
                        );

                        // Cache after eval
                        {
                            let mut cache = fen_cache.lock().await;
                            cache.insert(
                                fen_after_str,
                                CachedEval {
                                    best_uci: after_data.pvs[0]
                                        .moves
                                        .split_whitespace()
                                        .next()
                                        .unwrap_or("")
                                        .to_string(),
                                    best_line: after_data.pvs[0].moves.clone(),
                                    score: eval_after_score.clone(),
                                    depth: after_data.depth,
                                },
                            );
                        }

                        let win_before = get_win_chance(eval_before_cp);
                        let win_after = get_win_chance(eval_after_cp);
                        let win_chance_drop = win_before - win_after;

                        if win_chance_drop >= min_win_chance_drop {
                            let annotation = classify_annotation(win_chance_drop);
                            if !annotation.is_empty() {
                                let cp_loss = (eval_before_cp - eval_after_cp).max(0.0) as i32;
                                let opponent_punishment = after_data.pvs[0]
                                    .moves
                                    .split_whitespace()
                                    .next()
                                    .unwrap_or("")
                                    .to_string();
                                let opponent_line = after_data.pvs[0].moves.clone();

                                mistakes_found.push(PendingMistakePuzzle {
                                    source: source.to_string(),
                                    username: username.to_string(),
                                    game_id: game_id.clone(),
                                    fen: fen_str.clone(),
                                    player_color: color_str.to_string(),
                                    played_move: played_uci.clone(),
                                    best_move: cloud_best_uci,
                                    best_line: cloud_best_line,
                                    opponent_punishment,
                                    opponent_line,
                                    annotation: annotation.to_string(),
                                    cp_loss,
                                    win_chance_drop,
                                    eval_before: format_eval(&eval_before_score),
                                    eval_after: format_eval(&eval_after_score),
                                    move_number,
                                    engine_depth: cloud_data.depth as i32,
                                    date_analyzed: now.clone(),
                                    predecessor_fen: pred_fen.clone().unwrap_or_default(),
                                    predecessor_move: pred_move.clone().unwrap_or_default(),
                                });
                            }
                        }
                        continue; // Cloud handled both before+after
                    }
                }
                // Cloud had "before" but not "after" — fall through to local engine for "after"
            }
        }

        // Step 2: Local engine fallback
        counters.engine_analyzed.fetch_add(1, Ordering::SeqCst);

        // Lazily spawn engine on first use
        if local_engine.is_none() {
            let ep = PathBuf::from(engine_path);
            let mut proc = BaseEngine::spawn(ep).await?;
            proc.init_uci().await?;
            let rdr = proc.take_reader().ok_or(Error::EngineDisconnected)?;
            for opt in uci_options {
                if opt.name != "MultiPV" && opt.name != "UCI_Chess960" {
                    proc.set_option(&opt.name, &opt.value).await?;
                }
            }
            proc.set_option("MultiPV", "2").await?;
            local_engine = Some((proc, rdr));
        }
        let (ref mut proc, ref mut reader) = local_engine.as_mut().unwrap();

        // Analyze "before" position with local engine
        proc.set_position(initial_fen, moves_before).await?;
        proc.go(go_mode).await?;

        let mut best_lines: Vec<BestMoves> = Vec::new();
        let mut current_batch: Vec<BestMoves> = Vec::new();
        let mut last_depth = 0u32;

        while let Ok(Some(line)) = reader.next_line().await {
            match parse_one(&line) {
                UciMessage::Info(attrs) => {
                    if let Ok(bm) = parse_uci_attrs(attrs, &fen_str.parse()?, moves_before) {
                        if bm.score.lower_bound == Some(true) || bm.score.upper_bound == Some(true)
                        {
                            continue;
                        }
                        let multipv = bm.multipv;
                        let cur_depth = bm.depth;
                        if multipv as usize == current_batch.len() + 1 {
                            current_batch.push(bm);
                            let expected = 2u16.min(
                                Fen::from_ascii(fen_str.as_bytes())
                                    .ok()
                                    .and_then(|f| {
                                        let s = f.into_setup();
                                        let cm = CastlingMode::detect(&s);
                                        Chess::from_setup(s, cm)
                                            .or_else(PositionError::ignore_too_much_material)
                                            .ok()
                                    })
                                    .map(|p| p.legal_moves().len() as u16)
                                    .unwrap_or(2),
                            );
                            if multipv >= expected {
                                if current_batch.iter().all(|x| x.depth == cur_depth)
                                    && cur_depth >= last_depth
                                {
                                    best_lines = current_batch.clone();
                                    last_depth = cur_depth;
                                }
                                current_batch.clear();
                            }
                        }
                    }
                }
                UciMessage::BestMove { .. } => break,
                _ => {}
            }
        }

        if best_lines.is_empty() {
            continue;
        }

        let engine_best_uci = best_lines
            .first()
            .and_then(|b| b.uci_moves.first())
            .cloned()
            .unwrap_or_default();
        let engine_best_line = best_lines
            .first()
            .map(|b| b.uci_moves.join(" "))
            .unwrap_or_default();

        let eval_before_score = &best_lines[0].score;
        let eval_before_cp =
            score_from_player_perspective(eval_before_score, player_color, player_color);

        // Cache the before eval
        {
            let mut cache = fen_cache.lock().await;
            cache.insert(
                fen_str.clone(),
                CachedEval {
                    best_uci: engine_best_uci.clone(),
                    best_line: engine_best_line.clone(),
                    score: eval_before_score.clone(),
                    depth: last_depth,
                },
            );
        }

        if played_uci == &engine_best_uci {
            continue;
        }

        // Analyze "after" position
        let mut moves_after_played = moves_before.clone();
        moves_after_played.push(played_uci.clone());

        proc.set_position(initial_fen, &moves_after_played).await?;
        proc.go(go_mode).await?;

        let fen_after_str = compute_fen_after(fen_str, played_uci)?;

        let mut after_lines: Vec<BestMoves> = Vec::new();
        let mut current_batch2: Vec<BestMoves> = Vec::new();
        let mut last_depth2 = 0u32;

        while let Ok(Some(line)) = reader.next_line().await {
            match parse_one(&line) {
                UciMessage::Info(attrs) => {
                    if let Ok(bm) = parse_uci_attrs(attrs, &fen_after_str.parse()?, &[]) {
                        if bm.score.lower_bound == Some(true) || bm.score.upper_bound == Some(true)
                        {
                            continue;
                        }
                        let multipv = bm.multipv;
                        let cur_depth = bm.depth;
                        if multipv as usize == current_batch2.len() + 1 {
                            current_batch2.push(bm);
                            if multipv >= 1 {
                                if current_batch2.iter().all(|x| x.depth == cur_depth)
                                    && cur_depth >= last_depth2
                                {
                                    after_lines = current_batch2.clone();
                                    last_depth2 = cur_depth;
                                }
                                current_batch2.clear();
                            }
                        }
                    }
                }
                UciMessage::BestMove { .. } => break,
                _ => {}
            }
        }

        if after_lines.is_empty() {
            continue;
        }

        let eval_after_score = &after_lines[0].score;
        let eval_after_cp =
            score_from_player_perspective(eval_after_score, opponent_color, player_color);

        // Cache after eval
        {
            let mut cache = fen_cache.lock().await;
            cache.insert(
                fen_after_str,
                CachedEval {
                    best_uci: after_lines
                        .first()
                        .and_then(|b| b.uci_moves.first())
                        .cloned()
                        .unwrap_or_default(),
                    best_line: after_lines
                        .first()
                        .map(|b| b.uci_moves.join(" "))
                        .unwrap_or_default(),
                    score: eval_after_score.clone(),
                    depth: last_depth2,
                },
            );
        }

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;

        if win_chance_drop < min_win_chance_drop {
            continue;
        }

        let annotation = classify_annotation(win_chance_drop);
        if annotation.is_empty() {
            continue;
        }

        let cp_loss = (eval_before_cp - eval_after_cp).max(0.0) as i32;
        let engine_depth = last_depth as i32;

        let opponent_punishment = after_lines
            .first()
            .and_then(|b| b.uci_moves.first())
            .cloned()
            .unwrap_or_default();
        let opponent_line = after_lines
            .first()
            .map(|b| b.uci_moves.join(" "))
            .unwrap_or_default();

        mistakes_found.push(PendingMistakePuzzle {
            source: source.to_string(),
            username: username.to_string(),
            game_id: game_id.clone(),
            fen: fen_str.clone(),
            player_color: color_str.to_string(),
            played_move: played_uci.clone(),
            best_move: engine_best_uci,
            best_line: engine_best_line,
            opponent_punishment,
            opponent_line,
            annotation: annotation.to_string(),
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(eval_before_score),
            eval_after: format_eval(eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
        });
    }

    // Cleanup local engine if it was spawned
    if let Some((mut proc, _)) = local_engine {
        proc.quit().await.ok();
    }

    Ok(mistakes_found)
}

/// Compute the FEN after applying a single UCI move to a position.
fn compute_fen_after(fen_str: &str, uci_move: &str) -> Result<String, Error> {
    let f: Fen = fen_str.parse()?;
    let s = f.into_setup();
    let cm = CastlingMode::detect(&s);
    let mut pos = Chess::from_setup(s, cm)
        .or_else(PositionError::ignore_too_much_material)
        .map_err(|e| Error::ChessPosition(Box::new(e)))?;
    let uci: UciMove = uci_move.parse()?;
    let m = uci.to_move(&pos)?;
    pos.play_unchecked(&m);
    Ok(Fen::from_position(pos, EnPassantMode::Legal).to_string())
}

// ── CRUD commands ───────────────────────────────────────────────────────────

#[tauri::command]
#[specta::specta]
pub fn get_mistake_puzzles(
    db_path: String,
    filter: MistakePuzzleFilter,
) -> Result<Vec<MistakePuzzle>, Error> {
    let conn = open_db(&db_path)?;

    let mut sql = String::from(
        "SELECT id, source, username, game_id, fen, player_color, played_move,
                best_move, best_line, opponent_punishment, opponent_line,
                annotation, cp_loss, win_chance_drop, eval_before, eval_after,
                move_number, engine_depth, date_analyzed, completed
         FROM mistake_puzzles WHERE username = ?1",
    );
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(filter.username.clone())];
    let mut param_idx = 2;

    if let Some(ref src) = filter.source {
        sql.push_str(&format!(" AND source = ?{}", param_idx));
        params.push(Box::new(src.clone()));
        param_idx += 1;
    }
    if let Some(ref ann) = filter.annotation {
        sql.push_str(&format!(" AND annotation = ?{}", param_idx));
        params.push(Box::new(ann.clone()));
        param_idx += 1;
    }
    if let Some(comp) = filter.completed {
        sql.push_str(&format!(" AND completed = ?{}", param_idx));
        params.push(Box::new(comp));
        param_idx += 1;
    }

    sql.push_str(" ORDER BY id ASC");

    if let Some(limit) = filter.limit {
        sql.push_str(&format!(" LIMIT ?{}", param_idx));
        params.push(Box::new(limit));
        param_idx += 1;
    }
    if let Some(offset) = filter.offset {
        sql.push_str(&format!(" OFFSET ?{}", param_idx));
        params.push(Box::new(offset));
    }

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let puzzles = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(MistakePuzzle {
                id: row.get(0)?,
                source: row.get(1)?,
                username: row.get(2)?,
                game_id: row.get(3)?,
                fen: row.get(4)?,
                player_color: row.get(5)?,
                played_move: row.get(6)?,
                best_move: row.get(7)?,
                best_line: row.get(8)?,
                opponent_punishment: row.get(9)?,
                opponent_line: row.get(10)?,
                annotation: row.get(11)?,
                cp_loss: row.get(12)?,
                win_chance_drop: row.get(13)?,
                eval_before: row.get(14)?,
                eval_after: row.get(15)?,
                move_number: row.get(16)?,
                engine_depth: row.get(17)?,
                date_analyzed: row.get(18)?,
                completed: row.get(19)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(puzzles)
}

#[tauri::command]
#[specta::specta]
pub fn update_mistake_puzzle_completion(
    db_path: String,
    puzzle_id: i64,
    completed: i32,
) -> Result<(), Error> {
    let conn = open_db(&db_path)?;
    conn.execute(
        "UPDATE mistake_puzzles SET completed = ?1 WHERE id = ?2",
        rusqlite::params![completed, puzzle_id],
    )?;
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn get_mistake_stats(
    db_path: String,
    username: String,
    source: Option<String>,
) -> Result<MistakeStats, Error> {
    let conn = open_db(&db_path)?;
    get_stats_from_db(&conn, &username, source.as_deref().unwrap_or(""))
}

fn get_stats_from_db(
    conn: &rusqlite::Connection,
    username: &str,
    source: &str,
) -> Result<MistakeStats, Error> {
    let base_where = if source.is_empty() {
        "WHERE username = ?1"
    } else {
        "WHERE username = ?1 AND source = ?2"
    };

    let count_sql = format!("SELECT COUNT(*) FROM mistake_puzzles {}", base_where);
    let correct_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND completed = 1",
        base_where
    );
    let wrong_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND completed = 2",
        base_where
    );
    let unsolved_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND completed = 0",
        base_where
    );
    let blunders_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND annotation = '??'",
        base_where
    );
    let mistakes_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND annotation = '?'",
        base_where
    );
    let inaccuracies_sql = format!(
        "SELECT COUNT(*) FROM mistake_puzzles {} AND annotation = '?!'",
        base_where
    );

    let query = |sql: &str| -> Result<i64, Error> {
        if source.is_empty() {
            Ok(conn.query_row(sql, rusqlite::params![username], |r| r.get(0))?)
        } else {
            Ok(conn.query_row(sql, rusqlite::params![username, source], |r| r.get(0))?)
        }
    };

    let total = query(&count_sql)?;
    let solved_correct = query(&correct_sql)?;
    let solved_wrong = query(&wrong_sql)?;
    let unsolved = query(&unsolved_sql)?;
    let blunders = query(&blunders_sql)?;
    let mistakes = query(&mistakes_sql)?;
    let inaccuracies = query(&inaccuracies_sql)?;

    let accuracy = if solved_correct + solved_wrong > 0 {
        (solved_correct as f64 / (solved_correct + solved_wrong) as f64) * 100.0
    } else {
        0.0
    };

    Ok(MistakeStats {
        total,
        solved_correct,
        solved_wrong,
        unsolved,
        blunders,
        mistakes,
        inaccuracies,
        accuracy,
    })
}

#[tauri::command]
#[specta::specta]
pub fn delete_mistake_puzzles(
    db_path: String,
    username: String,
    source: Option<String>,
) -> Result<(), Error> {
    let conn = open_db(&db_path)?;
    if let Some(src) = source {
        conn.execute(
            "DELETE FROM mistake_puzzles WHERE username = ?1 AND source = ?2",
            rusqlite::params![username, src],
        )?;
    } else {
        conn.execute(
            "DELETE FROM mistake_puzzles WHERE username = ?1",
            rusqlite::params![username],
        )?;
    }
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn init_mistake_db(db_path: String) -> Result<(), Error> {
    open_db(&db_path)?;
    Ok(())
}

// ── Export mistakes as standard puzzle DB ────────────────────────────────────

const CREATE_PUZZLE_TABLES: &str = "
CREATE TABLE IF NOT EXISTS puzzles (
    id INTEGER PRIMARY KEY,
    fen TEXT NOT NULL,
    moves TEXT NOT NULL,
    rating INTEGER NOT NULL DEFAULT 1500,
    rating_deviation INTEGER NOT NULL DEFAULT 0,
    popularity INTEGER NOT NULL DEFAULT 0,
    nb_plays INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS themes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS puzzle_themes (
    puzzle_id INTEGER NOT NULL,
    theme_id INTEGER NOT NULL,
    PRIMARY KEY (puzzle_id, theme_id),
    FOREIGN KEY (puzzle_id) REFERENCES puzzles(id),
    FOREIGN KEY (theme_id) REFERENCES themes(id)
);
";

fn rating_from_annotation(annotation: &str) -> i32 {
    match annotation {
        "??" => 1000, // Blunders are often obvious to spot
        "?" => 1400,  // Mistakes require moderate skill
        "?!" => 1800, // Inaccuracies are subtle
        _ => 1500,
    }
}

#[tauri::command]
#[specta::specta]
pub fn export_mistakes_to_puzzle_db(
    mistake_db_path: String,
    puzzle_db_path: String,
    username: String,
    source: String,
) -> Result<i32, Error> {
    let mistake_conn = open_db(&mistake_db_path)?;

    // Read all mistakes with predecessor info + fen for fallback
    let mut stmt = mistake_conn.prepare(
        "SELECT predecessor_fen, predecessor_move, best_line, annotation, fen, opponent_punishment, opponent_line
         FROM mistake_puzzles
         WHERE username = ?1 AND source = ?2
         ORDER BY id ASC",
    )?;

    struct ExportRow {
        predecessor_fen: String,
        predecessor_move: String,
        best_line: String,
        annotation: String,
        fen: String,
        opponent_punishment: String,
        opponent_line: String,
    }

    let rows: Vec<ExportRow> = stmt
        .query_map(rusqlite::params![&username, &source], |row| {
            Ok(ExportRow {
                predecessor_fen: row.get(0)?,
                predecessor_move: row.get(1)?,
                best_line: row.get(2)?,
                annotation: row.get(3)?,
                fen: row.get(4)?,
                opponent_punishment: row.get(5)?,
                opponent_line: row.get(6)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Create/open the puzzle DB
    let puzzle_conn = rusqlite::Connection::open(&puzzle_db_path)?;
    puzzle_conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    puzzle_conn.execute_batch(CREATE_PUZZLE_TABLES)?;

    // Insert themes
    let theme_names = ["blunder", "mistake", "inaccuracy", "my-mistakes"];
    for name in &theme_names {
        puzzle_conn.execute(
            "INSERT OR IGNORE INTO themes (name) VALUES (?1)",
            rusqlite::params![name],
        )?;
    }

    // Get theme IDs
    let get_theme_id = |name: &str| -> Result<i64, Error> {
        Ok(puzzle_conn.query_row(
            "SELECT id FROM themes WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )?)
    };
    let blunder_theme_id = get_theme_id("blunder")?;
    let mistake_theme_id = get_theme_id("mistake")?;
    let inaccuracy_theme_id = get_theme_id("inaccuracy")?;
    let my_mistakes_theme_id = get_theme_id("my-mistakes")?;

    // Clear existing puzzles (re-export replaces all)
    puzzle_conn.execute("DELETE FROM puzzle_themes", [])?;
    puzzle_conn.execute("DELETE FROM puzzles", [])?;

    let mut exported = 0i32;
    let mut skipped_no_moves = 0i32;
    let mut skipped_too_short = 0i32;

    for row in &rows {
        // Determine the puzzle FEN and moves
        let (puzzle_fen, puzzle_moves) =
            if !row.predecessor_fen.is_empty() && !row.predecessor_move.is_empty() {
                // Standard Lichess format: FEN before opponent's move,
                // moves = [opponent_move, player_best_move, ...]
                let moves = format!("{} {}", row.predecessor_move, row.best_line);
                (row.predecessor_fen.clone(), moves)
            } else if !row.best_line.is_empty() {
                // No predecessor (first move of game for White, or edge case).
                // Use the mistake FEN directly. The best_line starts with the player's
                // best move. We need to prepend the opponent's punishment move so the
                // puzzle format works: FEN (player's turn) → auto-play opponent_punishment
                // → user finds the refutation from best_line.
                // Actually, for the Lichess format, FEN must have opposite side to move.
                // Since we have the player's FEN (player to move), the PuzzleBoard will
                // flip the orientation wrong. Instead, use the fen directly with the
                // opponent_punishment as setup move, and then best_line as the answer.
                if !row.opponent_punishment.is_empty() && !row.opponent_line.is_empty() {
                    // Use the fen (player's turn). PuzzleBoard will auto-play opponent_punishment
                    // and player responds with best_line moves.
                    // BUT the Lichess convention says: FEN has opponent-to-move, first move is auto-played.
                    // Here, FEN has player-to-move. We can't use opponent_punishment as auto-play
                    // because it would be an illegal move on the player's turn position.
                    // Skip these — they don't map to the standard puzzle format cleanly.
                    info!(
                        "Skipping first-move mistake (no predecessor): fen={}",
                        row.fen
                    );
                    skipped_no_moves += 1;
                    continue;
                } else {
                    skipped_no_moves += 1;
                    continue;
                }
            } else {
                skipped_no_moves += 1;
                continue;
            };

        // Ensure moves has an even number of tokens (ends on user answer)
        let move_tokens: Vec<&str> = puzzle_moves.split_whitespace().collect();
        let trimmed = if move_tokens.len() % 2 == 1 {
            // Odd total: the last move would be auto-played (opponent response, not a user answer)
            // Trim it so the puzzle ends on the user's answer
            move_tokens[..move_tokens.len() - 1].join(" ")
        } else {
            puzzle_moves.clone()
        };

        if trimmed.split_whitespace().count() < 2 {
            skipped_too_short += 1;
            continue; // Need at least setup + answer
        }

        let rating = rating_from_annotation(&row.annotation);

        puzzle_conn.execute(
            "INSERT INTO puzzles (fen, moves, rating, rating_deviation, popularity, nb_plays)
             VALUES (?1, ?2, ?3, 0, 0, 0)",
            rusqlite::params![&puzzle_fen, &trimmed, rating],
        )?;

        let puzzle_id = puzzle_conn.last_insert_rowid();

        // Link to "my-mistakes" theme
        puzzle_conn.execute(
            "INSERT OR IGNORE INTO puzzle_themes (puzzle_id, theme_id) VALUES (?1, ?2)",
            rusqlite::params![puzzle_id, my_mistakes_theme_id],
        )?;

        // Link to annotation-specific theme
        let annotation_theme_id = match row.annotation.as_str() {
            "??" => Some(blunder_theme_id),
            "?" => Some(mistake_theme_id),
            "?!" => Some(inaccuracy_theme_id),
            _ => None,
        };
        if let Some(tid) = annotation_theme_id {
            puzzle_conn.execute(
                "INSERT OR IGNORE INTO puzzle_themes (puzzle_id, theme_id) VALUES (?1, ?2)",
                rusqlite::params![puzzle_id, tid],
            )?;
        }

        exported += 1;
    }

    info!(
        "Export complete: {} puzzles exported, {} skipped (no predecessor), {} skipped (too short), {} total mistakes",
        exported, skipped_no_moves, skipped_too_short, rows.len()
    );

    Ok(exported)
}
