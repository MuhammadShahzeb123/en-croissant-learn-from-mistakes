use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use log::info;
use serde::{Deserialize, Serialize};
use shakmaty::{
    fen::Fen, uci::UciMove, CastlingMode, Chess, Color, EnPassantMode, FromSetup,
    Position, PositionError,
};
use specta::Type;
use vampirc_uci::{parse_one, UciMessage};

use crate::{
    chess::{parse_uci_attrs, BestMoves},
    db::encoding::{decode_move, iter_mainline_move_bytes},
    engine::{BaseEngine, EngineOption, EngineReader, GoMode},
    error::Error,
    progress::update_progress,
    AppState,
};

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
    pub games_analyzed: i32,
    pub total_games: i32,
    pub mistakes_found: i32,
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
    let cp = if side_to_move == player_color { raw } else { -raw };
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
        "Starting mistake analysis: {} games for {} from {}",
        total_games, username, source
    );

    let mut pending_mistakes: Vec<PendingMistakePuzzle> = Vec::new();

    // Spawn the engine
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
                let _ = update_progress(
                    &state.progress_state,
                    &app,
                    id.clone(),
                    overall,
                    false,
                );
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

    let mistake_conn = open_db(&mistake_db_path)?;
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
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>("LOWER(\"Name\") = ").bind::<diesel::sql_types::Text, _>(&lower_username))
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

    for move_byte in iter_mainline_move_bytes(&game.moves) {
        let fen_before =
            Fen::from_position(chess.clone(), EnPassantMode::Legal);
        let turn_before = chess.turn();

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

    let total_positions = positions.len();

    for (pos_idx, (fen_before, moves_before, played_uci, pred_fen, pred_move)) in positions.iter().enumerate() {
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
                        if bm.score.lower_bound == Some(true)
                            || bm.score.upper_bound == Some(true)
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
        let eval_before_cp = score_from_player_perspective(eval_before_score, player_color, player_color);

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
                    if let Ok(bm) = parse_uci_attrs(
                        attrs,
                        &fen_after_str.parse()?,
                        &[],
                    ) {
                        if bm.score.lower_bound == Some(true)
                            || bm.score.upper_bound == Some(true)
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
        let opponent_color = if player_color == Color::White { Color::Black } else { Color::White };
        let eval_after_cp = score_from_player_perspective(eval_after_score, opponent_color, player_color);

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
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
        vec![Box::new(filter.username.clone())];
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
    get_stats_from_db(
        &conn,
        &username,
        source.as_deref().unwrap_or(""),
    )
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
        "??" => 1000,  // Blunders are often obvious to spot
        "?"  => 1400,  // Mistakes require moderate skill
        "?!" => 1800,  // Inaccuracies are subtle
        _    => 1500,
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

    // Read all mistakes with predecessor info
    let mut stmt = mistake_conn.prepare(
        "SELECT predecessor_fen, predecessor_move, best_line, annotation
         FROM mistake_puzzles
         WHERE username = ?1 AND source = ?2
         ORDER BY id ASC",
    )?;

    struct ExportRow {
        predecessor_fen: String,
        predecessor_move: String,
        best_line: String,
        annotation: String,
    }

    let rows: Vec<ExportRow> = stmt
        .query_map(rusqlite::params![&username, &source], |row| {
            Ok(ExportRow {
                predecessor_fen: row.get(0)?,
                predecessor_move: row.get(1)?,
                best_line: row.get(2)?,
                annotation: row.get(3)?,
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

    for row in &rows {
        // Determine the puzzle FEN and moves
        let (puzzle_fen, puzzle_moves) = if !row.predecessor_fen.is_empty()
            && !row.predecessor_move.is_empty()
        {
            // Standard format: FEN before opponent's move, moves = [opponent_move, best_line...]
            let moves = format!("{} {}", row.predecessor_move, row.best_line);
            (row.predecessor_fen.clone(), moves)
        } else if !row.best_line.is_empty() {
            // No predecessor (first move of game) — use FEN directly
            // Put a dummy setup: the player's actual bad move as setup, then opponent punishment
            // Skip these — they don't cleanly map to the puzzle format
            continue;
        } else {
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
            "?"  => Some(mistake_theme_id),
            "?!" => Some(inaccuracy_theme_id),
            _    => None,
        };
        if let Some(tid) = annotation_theme_id {
            puzzle_conn.execute(
                "INSERT OR IGNORE INTO puzzle_themes (puzzle_id, theme_id) VALUES (?1, ?2)",
                rusqlite::params![puzzle_id, tid],
            )?;
        }

        exported += 1;
    }

    Ok(exported)
}
