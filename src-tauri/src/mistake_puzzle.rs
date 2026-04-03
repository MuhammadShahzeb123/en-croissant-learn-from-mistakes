use std::{
    collections::{HashMap, HashSet},
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

// ── Shared engine for hybrid mode (one process, serialized access) ──────────
type SharedEngine = Arc<TokioMutex<Option<(BaseEngine, EngineReader)>>>;

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
#[allow(dead_code)]
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

/// Rate limiter for Lichess cloud API with adaptive backoff.
/// Base interval: 200ms (5 req/s). On HTTP 429, interval doubles (up to 5s)
/// and gradually recovers after successful requests.
struct CloudRateLimiter {
    last_request: TokioMutex<tokio::time::Instant>,
    /// Current interval in ms — starts at 200, increased on 429, decays on success
    interval_ms: TokioMutex<u64>,
}

const CLOUD_BASE_INTERVAL_MS: u64 = 200;
const CLOUD_MAX_INTERVAL_MS: u64 = 5000;

impl CloudRateLimiter {
    fn new() -> Self {
        Self {
            last_request: TokioMutex::new(
                tokio::time::Instant::now() - std::time::Duration::from_secs(2),
            ),
            interval_ms: TokioMutex::new(CLOUD_BASE_INTERVAL_MS),
        }
    }

    async fn wait(&self) {
        let interval = { *self.interval_ms.lock().await };
        let mut last = self.last_request.lock().await;
        let elapsed = last.elapsed();
        let min_interval = std::time::Duration::from_millis(interval);
        if elapsed < min_interval {
            tokio::time::sleep(min_interval - elapsed).await;
        }
        *last = tokio::time::Instant::now();
    }

    /// Call after a successful cloud response — gradually reduce interval back to base.
    async fn on_success(&self) {
        let mut interval = self.interval_ms.lock().await;
        if *interval > CLOUD_BASE_INTERVAL_MS {
            // Halve toward base on each success
            *interval = (*interval / 2).max(CLOUD_BASE_INTERVAL_MS);
        }
    }

    /// Call on HTTP 429 — double the interval (capped).
    async fn on_rate_limited(&self) {
        let mut interval = self.interval_ms.lock().await;
        *interval = (*interval * 2).min(CLOUD_MAX_INTERVAL_MS);
        info!("Cloud rate limiter: interval increased to {}ms after 429", *interval);
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
/// - Depth >= min_depth check (default 16 — sufficient for blunder detection)
/// - HTTP 429 adaptive backoff with retry
/// - Rate limiting via shared CloudRateLimiter (200ms base, adaptive)
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
        // Adaptive backoff: increase the rate limiter interval and wait before retry
        rate_limiter.on_rate_limited().await;
        let backoff = { *rate_limiter.interval_ms.lock().await };
        info!("Cloud eval 429 — backing off {}ms before retry", backoff);
        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
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
        rate_limiter.on_success().await;
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
    rate_limiter.on_success().await;
    Ok(Some(data))
}

/// Minimum player move number to start analyzing (1-based).
/// Set to 2 to skip only the very first player move (e.g. "1.d4 vs 1.e4"
/// opening-preference false positives), but allow all subsequent moves
/// to be analyzed including early opening mistakes.
const MIN_PLAYER_MOVE_NUMBER: i32 = 2;

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
    pub white_player: String,
    pub black_player: String,
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
    pub predecessor_fen: String,
    pub predecessor_move: String,
    pub is_miss: i32,
    pub miss_opportunity_cp: i32,
    pub move_classification: String,
    pub miss_type: String,
    pub eval_delta: i32,
    pub mate_in: i32,
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
    pub misses: i64,
    pub accuracy: f64,
    pub game_accuracy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
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

// ── PGN-based mistake puzzle storage ────────────────────────────────────────

/// Metadata stored alongside puzzles in the PGN file (first entry).
#[derive(Debug, Clone, Default)]
struct AnalysisMetadata {
    username: String,
    source: String,
    game_accuracy: f64,
    total_moves_analyzed: i32,
    date_analyzed: String,
}

/// Escape a PGN header value: replace backslash and quote characters.
fn pgn_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Unescape a PGN header value.
#[allow(dead_code)]
fn pgn_unescape(s: &str) -> String {
    s.replace("\\\"", "\"").replace("\\\\", "\\")
}

/// Write mistake puzzles and analysis metadata to a PGN file.
/// Each puzzle becomes a separate PGN game entry with custom headers.
/// The first entry is a metadata record with [Event "Analysis Metadata"].
fn write_mistakes_to_pgn(
    path: &str,
    items: &[PendingMistakePuzzle],
    metadata: &AnalysisMetadata,
) -> Result<(), Error> {
    use std::fmt::Write as FmtWrite;
    let mut pgn = String::new();

    // Metadata entry
    writeln!(pgn, "[Event \"Analysis Metadata\"]").unwrap();
    writeln!(pgn, "[Username \"{}\"]", pgn_escape(&metadata.username)).unwrap();
    writeln!(pgn, "[Source \"{}\"]", pgn_escape(&metadata.source)).unwrap();
    writeln!(pgn, "[GameAccuracy \"{:.2}\"]", metadata.game_accuracy).unwrap();
    writeln!(pgn, "[TotalMovesAnalyzed \"{}\"]", metadata.total_moves_analyzed).unwrap();
    writeln!(pgn, "[DateAnalyzed \"{}\"]", pgn_escape(&metadata.date_analyzed)).unwrap();
    writeln!(pgn, "\n*\n").unwrap();

    // Puzzle entries
    for (idx, item) in items.iter().enumerate() {
        let puzzle_id = idx as i64 + 1;
        writeln!(pgn, "[Event \"Mistake Puzzle\"]").unwrap();
        writeln!(pgn, "[PuzzleId \"{}\"]", puzzle_id).unwrap();
        writeln!(pgn, "[SetUp \"1\"]").unwrap();
        writeln!(pgn, "[FEN \"{}\"]", pgn_escape(&item.fen)).unwrap();
        writeln!(pgn, "[Source \"{}\"]", pgn_escape(&item.source)).unwrap();
        writeln!(pgn, "[Username \"{}\"]", pgn_escape(&item.username)).unwrap();
        writeln!(pgn, "[GameId \"{}\"]", pgn_escape(&item.game_id)).unwrap();
        writeln!(pgn, "[PlayerColor \"{}\"]", pgn_escape(&item.player_color)).unwrap();
        writeln!(pgn, "[WhitePlayer \"{}\"]", pgn_escape(&item.white_player)).unwrap();
        writeln!(pgn, "[BlackPlayer \"{}\"]", pgn_escape(&item.black_player)).unwrap();
        writeln!(pgn, "[PlayedMove \"{}\"]", pgn_escape(&item.played_move)).unwrap();
        writeln!(pgn, "[BestMove \"{}\"]", pgn_escape(&item.best_move)).unwrap();
        writeln!(pgn, "[BestLine \"{}\"]", pgn_escape(&item.best_line)).unwrap();
        writeln!(pgn, "[OpponentPunishment \"{}\"]", pgn_escape(&item.opponent_punishment)).unwrap();
        writeln!(pgn, "[OpponentLine \"{}\"]", pgn_escape(&item.opponent_line)).unwrap();
        writeln!(pgn, "[Annotation \"{}\"]", pgn_escape(&item.annotation)).unwrap();
        writeln!(pgn, "[CpLoss \"{}\"]", item.cp_loss).unwrap();
        writeln!(pgn, "[WinChanceDrop \"{:.2}\"]", item.win_chance_drop).unwrap();
        writeln!(pgn, "[EvalBefore \"{}\"]", pgn_escape(&item.eval_before)).unwrap();
        writeln!(pgn, "[EvalAfter \"{}\"]", pgn_escape(&item.eval_after)).unwrap();
        writeln!(pgn, "[MoveNumber \"{}\"]", item.move_number).unwrap();
        writeln!(pgn, "[EngineDepth \"{}\"]", item.engine_depth).unwrap();
        writeln!(pgn, "[DateAnalyzed \"{}\"]", pgn_escape(&item.date_analyzed)).unwrap();
        writeln!(pgn, "[Completed \"0\"]").unwrap();
        writeln!(pgn, "[PredecessorFen \"{}\"]", pgn_escape(&item.predecessor_fen)).unwrap();
        writeln!(pgn, "[PredecessorMove \"{}\"]", pgn_escape(&item.predecessor_move)).unwrap();
        writeln!(pgn, "[IsMiss \"{}\"]", if item.is_miss { 1 } else { 0 }).unwrap();
        writeln!(pgn, "[MissType \"{}\"]", pgn_escape(&item.miss_type)).unwrap();
        writeln!(pgn, "[MissOpportunityCp \"{}\"]", item.miss_opportunity_cp).unwrap();
        writeln!(pgn, "[MoveClassification \"{}\"]", pgn_escape(&item.move_classification)).unwrap();
        writeln!(pgn, "[EvalDelta \"{}\"]", item.eval_delta).unwrap();
        writeln!(pgn, "[MateIn \"{}\"]", item.mate_in).unwrap();
        writeln!(pgn, "[Result \"*\"]").unwrap();
        // Comment with human-readable summary
        writeln!(
            pgn,
            "\n{{ Mistake: {}. Best: {}. Punishment: {} }}\n*\n",
            item.played_move, item.best_move, item.opponent_punishment
        ).unwrap();
    }

    std::fs::write(path, pgn)?;
    info!("Wrote {} mistake puzzles to PGN: {}", items.len(), path);
    Ok(())
}

/// Parse PGN header line like `[Key "Value"]` → Some((key, value))
fn parse_pgn_header(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if !line.starts_with('[') || !line.ends_with(']') {
        return None;
    }
    let inner = &line[1..line.len() - 1];
    let quote_start = inner.find('"')?;
    let key = inner[..quote_start].trim().to_string();
    let rest = &inner[quote_start + 1..];
    // Find the closing quote (handle escaped quotes)
    let mut value = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                value.push(next);
            }
        } else if c == '"' {
            break;
        } else {
            value.push(c);
        }
    }
    Some((key, value))
}

/// Read all mistake puzzles and metadata from a PGN file.
/// Returns (puzzles, metadata).
fn read_mistakes_from_pgn(path: &str) -> Result<(Vec<MistakePuzzle>, AnalysisMetadata), Error> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), AnalysisMetadata::default()));
        }
        Err(e) => return Err(Error::Io(Box::new(e))),
    };

    let mut puzzles: Vec<MistakePuzzle> = Vec::new();
    let mut metadata = AnalysisMetadata::default();

    // Split by blank lines followed by * to separate game entries
    // PGN entries are separated by the result marker "*" followed by blank lines
    let mut current_headers: HashMap<String, String> = HashMap::new();

    for line in content.lines() {
        let trimmed = line.trim();

        if let Some((key, val)) = parse_pgn_header(trimmed) {
            current_headers.insert(key, val);
            continue;
        }

        // A line with just "*" or starting with "{" (comment) signals end of an entry
        if trimmed == "*" || trimmed.starts_with("{ ") || (trimmed.ends_with("*") && trimmed.contains('}')) {
            if !current_headers.is_empty() {
                let event = current_headers.get("Event").cloned().unwrap_or_default();

                if event == "Analysis Metadata" {
                    metadata.username = current_headers.get("Username").cloned().unwrap_or_default();
                    metadata.source = current_headers.get("Source").cloned().unwrap_or_default();
                    metadata.game_accuracy = current_headers
                        .get("GameAccuracy")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0.0);
                    metadata.total_moves_analyzed = current_headers
                        .get("TotalMovesAnalyzed")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    metadata.date_analyzed = current_headers
                        .get("DateAnalyzed")
                        .cloned()
                        .unwrap_or_default();
                } else if event == "Mistake Puzzle" {
                    let get = |key: &str| -> String {
                        current_headers.get(key).cloned().unwrap_or_default()
                    };
                    let get_i32 = |key: &str| -> i32 {
                        current_headers
                            .get(key)
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0)
                    };
                    let get_i64 = |key: &str| -> i64 {
                        current_headers
                            .get(key)
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0)
                    };
                    let get_f64 = |key: &str| -> f64 {
                        current_headers
                            .get(key)
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0.0)
                    };

                    puzzles.push(MistakePuzzle {
                        id: get_i64("PuzzleId"),
                        source: get("Source"),
                        username: get("Username"),
                        game_id: get("GameId"),
                        fen: get("FEN"),
                        player_color: get("PlayerColor"),
                        white_player: get("WhitePlayer"),
                        black_player: get("BlackPlayer"),
                        played_move: get("PlayedMove"),
                        best_move: get("BestMove"),
                        best_line: get("BestLine"),
                        opponent_punishment: get("OpponentPunishment"),
                        opponent_line: get("OpponentLine"),
                        annotation: get("Annotation"),
                        cp_loss: get_i32("CpLoss"),
                        win_chance_drop: get_f64("WinChanceDrop"),
                        eval_before: get("EvalBefore"),
                        eval_after: get("EvalAfter"),
                        move_number: get_i32("MoveNumber"),
                        engine_depth: get_i32("EngineDepth"),
                        date_analyzed: get("DateAnalyzed"),
                        completed: get_i32("Completed"),
                        predecessor_fen: get("PredecessorFen"),
                        predecessor_move: get("PredecessorMove"),
                        is_miss: get_i32("IsMiss"),
                        miss_opportunity_cp: get_i32("MissOpportunityCp"),
                        move_classification: get("MoveClassification"),
                        miss_type: get("MissType"),
                        eval_delta: get_i32("EvalDelta"),
                        mate_in: get_i32("MateIn"),
                    });
                }

                current_headers.clear();
            }
        }
    }

    // Handle last entry if file doesn't end with *
    if !current_headers.is_empty() {
        let event = current_headers.get("Event").cloned().unwrap_or_default();
        if event == "Mistake Puzzle" {
            let get = |key: &str| -> String {
                current_headers.get(key).cloned().unwrap_or_default()
            };
            let get_i32 = |key: &str| -> i32 {
                current_headers
                    .get(key)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0)
            };
            let get_i64 = |key: &str| -> i64 {
                current_headers
                    .get(key)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0)
            };
            let get_f64 = |key: &str| -> f64 {
                current_headers
                    .get(key)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0)
            };

            puzzles.push(MistakePuzzle {
                id: get_i64("PuzzleId"),
                source: get("Source"),
                username: get("Username"),
                game_id: get("GameId"),
                fen: get("FEN"),
                player_color: get("PlayerColor"),
                white_player: get("WhitePlayer"),
                black_player: get("BlackPlayer"),
                played_move: get("PlayedMove"),
                best_move: get("BestMove"),
                best_line: get("BestLine"),
                opponent_punishment: get("OpponentPunishment"),
                opponent_line: get("OpponentLine"),
                annotation: get("Annotation"),
                cp_loss: get_i32("CpLoss"),
                win_chance_drop: get_f64("WinChanceDrop"),
                eval_before: get("EvalBefore"),
                eval_after: get("EvalAfter"),
                move_number: get_i32("MoveNumber"),
                engine_depth: get_i32("EngineDepth"),
                date_analyzed: get("DateAnalyzed"),
                completed: get_i32("Completed"),
                predecessor_fen: get("PredecessorFen"),
                predecessor_move: get("PredecessorMove"),
                is_miss: get_i32("IsMiss"),
                miss_opportunity_cp: get_i32("MissOpportunityCp"),
                move_classification: get("MoveClassification"),
                miss_type: get("MissType"),
                eval_delta: get_i32("EvalDelta"),
                mate_in: get_i32("MateIn"),
            });
        }
    }

    Ok((puzzles, metadata))
}

/// Rewrite the PGN file, updating the Completed status of a single puzzle.
fn update_completion_in_pgn(path: &str, puzzle_id: i64, completed: i32) -> Result<(), Error> {
    let content = std::fs::read_to_string(path)?;
    let mut result = String::with_capacity(content.len());
    let mut current_puzzle_id: Option<i64> = None;

    for line in content.lines() {
        let trimmed = line.trim();

        if let Some((key, val)) = parse_pgn_header(trimmed) {
            if key == "PuzzleId" {
                current_puzzle_id = val.parse().ok();
            }
            if key == "Completed" && current_puzzle_id == Some(puzzle_id) {
                result.push_str(&format!("[Completed \"{}\"]\n", completed));
                continue;
            }
        }

        // Reset on new entry
        if trimmed == "*" || (trimmed.ends_with("*") && trimmed.contains('}')) {
            current_puzzle_id = None;
        }

        result.push_str(line);
        result.push('\n');
    }

    std::fs::write(path, result)?;
    Ok(())
}

/// Compute stats from a list of puzzles and metadata.
fn compute_stats_from_puzzles(
    puzzles: &[MistakePuzzle],
    game_accuracy: f64,
) -> MistakeStats {
    let total = puzzles.len() as i64;
    let solved_correct = puzzles.iter().filter(|p| p.completed == 1).count() as i64;
    let solved_wrong = puzzles.iter().filter(|p| p.completed == 2).count() as i64;
    let unsolved = puzzles.iter().filter(|p| p.completed == 0).count() as i64;
    let blunders = puzzles.iter().filter(|p| p.annotation == "??").count() as i64;
    let mistakes = puzzles.iter().filter(|p| p.annotation == "?").count() as i64;
    let inaccuracies = puzzles.iter().filter(|p| p.annotation == "?!").count() as i64;
    let misses = puzzles.iter().filter(|p| p.is_miss == 1).count() as i64;
    let accuracy = if solved_correct + solved_wrong > 0 {
        (solved_correct as f64 / (solved_correct + solved_wrong) as f64) * 100.0
    } else {
        0.0
    };

    MistakeStats {
        total,
        solved_correct,
        solved_wrong,
        unsolved,
        blunders,
        mistakes,
        inaccuracies,
        misses,
        accuracy,
        game_accuracy,
    }
}

/// Open a puzzle DB (for export to standard format). Uses rusqlite.
/// Uses DELETE journal mode to avoid WAL/SHM lock files that can cause
/// "database is locked" errors on Windows when the frontend reads the DB.
fn open_puzzle_db(path: &str) -> Result<rusqlite::Connection, Error> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=NORMAL;")?;
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

// ── Enhanced classification & miss detection ────────────────────────────────

/// Extract mate-in count from engine score.
/// Returns Some(n) where n > 0 means forced mate FOR side-to-move in n moves,
/// n < 0 means opponent has forced mate.
fn extract_mate_in(score: &vampirc_uci::uci::Score) -> Option<i32> {
    use vampirc_uci::uci::ScoreValue;
    match score.value {
        ScoreValue::Mate(m) => Some(m),
        _ => None,
    }
}

/// Classify a move by centipawn eval delta (positive = player lost value).
/// Returns one of: "BEST", "EXCELLENT", "GOOD", "INACCURACY", "MISTAKE", "BLUNDER", "MISS"
fn classify_move_by_cp(
    eval_delta: f64,
    was_mate_available: bool,
    is_mate_allowed_after: bool,
) -> &'static str {
    if was_mate_available       { return "MISS"; }
    if is_mate_allowed_after    { return "BLUNDER"; }
    if eval_delta <= 10.0       { return "BEST"; }
    if eval_delta <= 25.0       { return "EXCELLENT"; }
    if eval_delta <= 50.0       { return "GOOD"; }
    if eval_delta <= 100.0      { return "INACCURACY"; }
    if eval_delta <= 300.0      { return "MISTAKE"; }
    "BLUNDER"
}

/// Map the new CP-based classification back to legacy annotation for backward compat.
fn classification_to_annotation(classification: &str) -> &'static str {
    match classification {
        "MISS" => "miss",
        "BLUNDER" => "??",
        "MISTAKE" => "?",
        "INACCURACY" => "?!",
        _ => "",
    }
}

/// Enhanced miss detection using the briefing's algorithm.
/// Evaluates both mate-based misses and winning-opportunity misses.
/// All evals are from the PLAYER's perspective (positive = good for player).
///
/// Returns (is_miss, miss_type, miss_opportunity_cp).
fn detect_miss_enhanced(
    best_eval_player_cp: f64,
    eval_delta: f64,
    best_mate_in: Option<i32>,
    actual_move: &str,
    best_move: &str,
) -> (bool, &'static str, i32) {
    // Player played the best move — no miss
    if actual_move == best_move {
        return (false, "", 0);
    }

    // Condition: Forced mate was available for the player and not played
    if let Some(mate) = best_mate_in {
        if mate > 0 {
            return (true, "MATE_MISSED", 100_000);
        }
    }

    // Condition: Large opportunity existed (player had ≥150cp advantage)
    //            AND the advantage was wasted (eval delta ≥ 100cp)
    let opportunity_threshold = 150.0;
    let advantage_lost_threshold = 100.0;
    if best_eval_player_cp >= opportunity_threshold && eval_delta >= advantage_lost_threshold {
        return (true, "WINNING_OPPORTUNITY_MISSED", eval_delta as i32);
    }

    (false, "", 0)
}

/// Compute per-move accuracy from eval delta (centipawns).
/// Uses Chess.com's publicly reverse-engineered formula.
fn move_accuracy_from_delta(eval_delta: f64) -> f64 {
    let cpl = eval_delta.max(0.0) / 100.0; // convert to pawns
    let accuracy = 103.1668 * (-0.04354 * cpl).exp() - 3.1669;
    accuracy.clamp(0.0, 100.0)
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
    let mut all_eval_deltas: Vec<f64> = Vec::new();

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
                &mut all_eval_deltas,
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
        // ── Hybrid analysis: cloud → shared local engine fallback ────
        let client = Arc::new(reqwest::Client::new());
        let rate_limiter = Arc::new(CloudRateLimiter::new());
        let fen_cache: FenCache = Arc::new(TokioMutex::new(HashMap::new()));
        let counters = Arc::new(HybridCounters::new());
        let cancel_flag_clone = cancel_flag.clone();

        // Wrap games in Arc for sharing across tasks
        let games: Arc<Vec<GameRecord>> = Arc::new(games);
        let total_games_u32 = total_games as u32;

        // CRITICAL FIX: Single shared engine behind a mutex.
        // Previously each of 4 parallel tasks spawned its own engine process,
        // causing CPU saturation and system freeze on a 6-core CPU.
        let shared_engine: SharedEngine = Arc::new(TokioMutex::new(None));

        // Concurrency: up to 2 games at a time (reduced from 4).
        // Cloud requests remain parallel; engine access is serialized via mutex.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(2));
        let all_mistakes: Arc<TokioMutex<Vec<PendingMistakePuzzle>>> =
            Arc::new(TokioMutex::new(Vec::new()));
        let hybrid_eval_deltas: Arc<TokioMutex<Vec<f64>>> =
            Arc::new(TokioMutex::new(Vec::new()));

        let start_time = tokio::time::Instant::now();

        // ── BATCH PRE-FETCH: Collect all unique FENs, then bulk cloud-lookup ──
        // This mirrors how Lichess itself works: pure database lookups for most
        // positions, engine only for rare positions not in the cloud DB.
        {
            let mut unique_fens: HashSet<String> = HashSet::new();

            for game in games.iter() {
                let initial_fen = game
                    .fen
                    .as_deref()
                    .unwrap_or("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");

                let fen_parsed: Result<Fen, _> = initial_fen.parse();
                let Ok(fen_parsed) = fen_parsed else { continue };
                let setup = fen_parsed.as_setup().clone();
                let castling_mode = CastlingMode::detect(&setup);
                let chess_result = Chess::from_setup(setup, castling_mode)
                    .or_else(PositionError::ignore_too_much_material);
                let Ok(mut chess) = chess_result else { continue };

                let player_is_white = username.eq_ignore_ascii_case(&game.white);
                let player_color = if player_is_white { Color::White } else { Color::Black };

                let mut player_move_idx = 0i32;
                for move_byte in iter_mainline_move_bytes(&game.moves) {
                    let fen_before = Fen::from_position(chess.clone(), EnPassantMode::Legal);
                    let turn_before = chess.turn();

                    if let Some(m) = decode_move(move_byte, &chess) {
                        let _uci = UciMove::from_move(&m, castling_mode).to_string();

                        if turn_before == player_color {
                            player_move_idx += 1;
                            if player_move_idx >= MIN_PLAYER_MOVE_NUMBER {
                                // "Before" position (player to move)
                                unique_fens.insert(fen_before.to_string());

                                // "After" position (position after player's move)
                                let mut chess_after = chess.clone();
                                chess_after.play_unchecked(&m);
                                let fen_after = Fen::from_position(chess_after, EnPassantMode::Legal);
                                unique_fens.insert(fen_after.to_string());
                            }
                        }

                        chess.play_unchecked(&m);
                    } else {
                        break;
                    }
                }
            }

            let total_fens = unique_fens.len();
            info!(
                "Batch pre-fetch: {} unique FENs from {} games",
                total_fens, total_games
            );

            // Fetch cloud evals for all unique FENs into the cache
            let mut fetched = 0u32;
            let mut cloud_found = 0u32;
            for fen_str in &unique_fens {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }

                // Skip FENs already in cache (shouldn't happen on first run, but safe)
                {
                    let cache = fen_cache.lock().await;
                    if cache.contains_key(fen_str) {
                        fetched += 1;
                        continue;
                    }
                }

                // Cloud lookup only (no engine fallback during pre-fetch)
                let cloud_result = fetch_cloud_eval_hybrid(
                    &client, fen_str, 3, 16, &rate_limiter,
                ).await;

                if let Ok(Some(ref cloud_data)) = cloud_result {
                    if !cloud_data.pvs.is_empty() {
                        let score = cloud_pv_to_score(&cloud_data.pvs[0]);
                        let best_uci = cloud_data.pvs[0]
                            .moves
                            .split_whitespace()
                            .next()
                            .unwrap_or("")
                            .to_string();
                        let best_line = cloud_data.pvs[0].moves.clone();

                        let mut cache = fen_cache.lock().await;
                        cache.insert(
                            fen_str.clone(),
                            CachedEval {
                                best_uci,
                                best_line,
                                score,
                                depth: cloud_data.depth,
                            },
                        );
                        cloud_found += 1;
                    }
                }

                fetched += 1;

                // Emit progress during pre-fetch phase (0-50% of total progress)
                if fetched % 50 == 0 || fetched == total_fens as u32 {
                    let pct = (fetched as f32 / total_fens as f32) * 50.0;
                    let _ = update_progress(
                        &state.progress_state, &app, id.clone(), pct, false,
                    );
                }
            }

            info!(
                "Batch pre-fetch complete: {}/{} FENs found in cloud ({:.1}% hit rate) in {:.1}s",
                cloud_found,
                total_fens,
                if total_fens > 0 { cloud_found as f64 / total_fens as f64 * 100.0 } else { 0.0 },
                start_time.elapsed().as_secs_f64()
            );
        }

        let mut handles = Vec::new();

        for (game_idx, _) in games.iter().enumerate() {
            let sem = semaphore.clone();
            let client = client.clone();
            let rate_limiter = rate_limiter.clone();
            let fen_cache = fen_cache.clone();
            let counters = counters.clone();
            let cancel = cancel_flag_clone.clone();
            let all_mistakes = all_mistakes.clone();
            let hybrid_eval_deltas = hybrid_eval_deltas.clone();
            let games = games.clone();
            let username = username.clone();
            let source = source.clone();
            let engine_path = engine.clone();
            let go_mode = go_mode.clone();
            let uci_options = uci_options.clone();
            let progress_state = state.progress_state.clone();
            let app_handle = app.clone();
            let progress_id = id.clone();
            let shared_engine = shared_engine.clone();

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
                    &shared_engine,
                )
                .await;

                match result {
                    Ok((game_mistakes, game_deltas)) => {
                        info!(
                            "Game {} (hybrid): found {} mistakes",
                            game.id,
                            game_mistakes.len()
                        );
                        all_mistakes.lock().await.extend(game_mistakes);
                        hybrid_eval_deltas.lock().await.extend(game_deltas);
                    }
                    Err(e) => {
                        info!("Skipping game {} (hybrid): {}", game.id, e);
                    }
                }

                let done = counters.games_done.fetch_add(1, Ordering::SeqCst) + 1;
                // Pre-fetch used 0-50%; per-game analysis uses 50-100%
                let overall = 50.0 + (done as f32 / total_games_u32 as f32) * 50.0;

                let elapsed_secs = start_time.elapsed().as_secs() as u32;
                let _est_left = if done > 0 {
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

        // Cleanup shared engine
        {
            let mut engine_guard = shared_engine.lock().await;
            if let Some((mut proc, _)) = engine_guard.take() {
                proc.quit().await.ok();
                info!("Shared hybrid engine process terminated cleanly.");
            }
        }

        pending_mistakes = all_mistakes.lock().await.clone();
        all_eval_deltas = hybrid_eval_deltas.lock().await.clone();

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
        // Force MultiPV=3 for top 3 move analysis
        proc.set_option("MultiPV", "3").await?;

        // Track all eval deltas for game accuracy computation
        let mut all_eval_deltas: Vec<f64> = Vec::new();

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
                &mut all_eval_deltas,
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

    // Write mistakes to PGN file
    info!(
        "Writing {} mistake puzzles for {} / {} to PGN: {}",
        pending_mistakes.len(), username, source, mistake_db_path
    );

    // Log annotation distribution for debugging classification issues
    {
        let mut blunders = 0;
        let mut mistakes = 0;
        let mut inaccuracies = 0;
        let mut misses_only = 0;
        let mut misses_total = 0;
        for p in &pending_mistakes {
            match p.annotation.as_str() {
                "??" => blunders += 1,
                "?" => mistakes += 1,
                "?!" => inaccuracies += 1,
                "miss" => misses_only += 1,
                _ => {}
            }
            if p.is_miss {
                misses_total += 1;
            }
        }
        info!(
            "Annotation breakdown: {} blunders (??), {} mistakes (?), {} inaccuracies (?!), {} miss-only, {} total misses (including dual)",
            blunders, mistakes, inaccuracies, misses_only, misses_total
        );
    }

    // Compute and store game accuracy from all eval deltas
    let game_accuracy = if !all_eval_deltas.is_empty() {
        let sum: f64 = all_eval_deltas.iter().map(|d| move_accuracy_from_delta(*d)).sum();
        (sum / all_eval_deltas.len() as f64).clamp(0.0, 100.0)
    } else {
        0.0
    };
    info!(
        "Game accuracy: {:.1}% ({} total moves analyzed)",
        game_accuracy,
        all_eval_deltas.len()
    );

    // Store game accuracy and puzzles in PGN file
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = AnalysisMetadata {
        username: username.clone(),
        source: source.clone(),
        game_accuracy,
        total_moves_analyzed: all_eval_deltas.len() as i32,
        date_analyzed: now,
    };

    write_mistakes_to_pgn(&mistake_db_path, &pending_mistakes, &metadata)?;

    // Build and return stats from in-memory data
    let puzzles_for_stats: Vec<MistakePuzzle> = pending_mistakes
        .iter()
        .enumerate()
        .map(|(idx, p)| MistakePuzzle {
            id: idx as i64 + 1,
            source: p.source.clone(),
            username: p.username.clone(),
            game_id: p.game_id.clone(),
            fen: p.fen.clone(),
            player_color: p.player_color.clone(),
            white_player: p.white_player.clone(),
            black_player: p.black_player.clone(),
            played_move: p.played_move.clone(),
            best_move: p.best_move.clone(),
            best_line: p.best_line.clone(),
            opponent_punishment: p.opponent_punishment.clone(),
            opponent_line: p.opponent_line.clone(),
            annotation: p.annotation.clone(),
            cp_loss: p.cp_loss,
            win_chance_drop: p.win_chance_drop,
            eval_before: p.eval_before.clone(),
            eval_after: p.eval_after.clone(),
            move_number: p.move_number,
            engine_depth: p.engine_depth,
            date_analyzed: p.date_analyzed.clone(),
            completed: 0,
            predecessor_fen: p.predecessor_fen.clone(),
            predecessor_move: p.predecessor_move.clone(),
            is_miss: if p.is_miss { 1 } else { 0 },
            miss_opportunity_cp: p.miss_opportunity_cp,
            move_classification: p.move_classification.clone(),
            miss_type: p.miss_type.clone(),
            eval_delta: p.eval_delta,
            mate_in: p.mate_in,
        })
        .collect();
    Ok(compute_stats_from_puzzles(&puzzles_for_stats, game_accuracy))
}

// ── Game data from the en-croissant DB ──────────────────────────────────────

use diesel::prelude::*;

#[allow(dead_code)]
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
    white_player: String,
    black_player: String,
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
    is_miss: bool,
    miss_opportunity_cp: i32,
    move_classification: String,
    miss_type: String,
    eval_delta: i32,
    mate_in: i32,
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
    eval_deltas: &mut Vec<f64>,
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

        // Skip the very first player move (opening choice, e.g. 1.e4 vs 1.d4)
        if move_number < MIN_PLAYER_MOVE_NUMBER {
            continue;
        }

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
                        parse_uci_attrs(attrs, &fen_before.to_string().parse()?, &[])
                    {
                        if bm.score.lower_bound == Some(true) || bm.score.upper_bound == Some(true)
                        {
                            continue;
                        }
                        let multipv = bm.multipv;
                        let cur_depth = bm.depth;
                        if multipv as usize == current_batch.len() + 1 {
                            current_batch.push(bm);
                            let expected = 3u16.min(
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
                                    .unwrap_or(3),
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
        // NOTE: parse_uci_attrs normalizes all engine scores to White's absolute perspective
        // (inverts when Black to move), so we always use Color::White as side_to_move here.
        let eval_before_score = &best_lines[0].score;
        let eval_before_cp =
            score_from_player_perspective(eval_before_score, Color::White, player_color);
        let best_mate_in = extract_mate_in(eval_before_score);

        // Did the player play the engine's best move?
        if played_uci == &engine_best_uci {
            // Good move — record zero eval delta for accuracy computation
            eval_deltas.push(0.0);
            continue;
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

        // Eval after the player's move: parse_uci_attrs normalizes to White's absolute perspective.
        let eval_after_score = &after_lines[0].score;
        let eval_after_cp =
            score_from_player_perspective(eval_after_score, Color::White, player_color);
        let after_mate_in = extract_mate_in(eval_after_score);

        // Compute eval delta (centipawns lost by the player's move)
        let eval_delta_f = (eval_before_cp - eval_after_cp).max(0.0);
        let eval_delta_i = eval_delta_f as i32;

        // Track eval delta for game accuracy computation
        eval_deltas.push(eval_delta_f);

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;
        let cp_loss = eval_delta_i;

        // Enhanced miss detection
        let (is_miss, miss_type, miss_opportunity_cp) = detect_miss_enhanced(
            eval_before_cp,
            eval_delta_f,
            best_mate_in,
            played_uci,
            &engine_best_uci,
        );

        // CP-based move classification
        let was_mate_available = best_mate_in.map_or(false, |m| m > 0);
        let is_mate_allowed_after = after_mate_in.map_or(false, |m| m > 0);
        let move_classification = classify_move_by_cp(eval_delta_f, was_mate_available, is_mate_allowed_after);
        let mate_in_val = best_mate_in.unwrap_or(0);

        // Legacy annotation (win-chance-drop based)
        let annotation = classify_annotation(win_chance_drop);

        // Determine final annotation: CP-based classification is primary,
        // legacy win-chance-drop annotation is fallback only when CP says move is fine.
        let cp_annotation = classification_to_annotation(move_classification);
        let final_annotation = if is_miss {
            "miss".to_string()
        } else if !cp_annotation.is_empty() {
            cp_annotation.to_string()
        } else {
            annotation.to_string()
        };
        if final_annotation.is_empty() {
            continue; // Neither system flagged this as notable
        }

        // Log every non-trivial eval drop for debugging classification issues
        if win_chance_drop > 2.0 || is_miss {
            info!(
                "Local move {}: eval_before={:.0}cp eval_after={:.0}cp delta={}cp win_drop={:.1}% class={} annotation={} is_miss={} miss_type={}",
                move_number, eval_before_cp, eval_after_cp, eval_delta_i, win_chance_drop,
                move_classification,
                if final_annotation.is_empty() { "none" } else { &final_annotation },
                is_miss, miss_type
            );
        }

        // Skip if below threshold AND not a miss
        if win_chance_drop < min_win_chance_drop && !is_miss {
            // Also check CP classification — if it's INACCURACY or worse, keep it
            if !matches!(move_classification, "INACCURACY" | "MISTAKE" | "BLUNDER" | "MISS") {
                continue;
            }
        }

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
            white_player: game.white.clone(),
            black_player: game.black.clone(),
            played_move: played_uci.clone(),
            best_move: engine_best_uci,
            best_line: engine_best_line,
            opponent_punishment,
            opponent_line,
            annotation: final_annotation,
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(eval_before_score),
            eval_after: format_eval(eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
            is_miss,
            miss_opportunity_cp,
            move_classification: move_classification.to_string(),
            miss_type: miss_type.to_string(),
            eval_delta: eval_delta_i,
            mate_in: mate_in_val,
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
    eval_deltas: &mut Vec<f64>,
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

        // Skip the first player move (opening choice preference, not a real mistake)
        if move_number < MIN_PLAYER_MOVE_NUMBER {
            continue;
        }

        if total_positions > 0 {
            on_position_progress(pos_idx as f32 / total_positions as f32);
        }

        // Fetch cloud eval for the position BEFORE the player's move
        let before_eval = match fetch_cloud_eval(client, fen_str, 3).await? {
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
        let best_mate_in = extract_mate_in(&eval_before_score);

        // Did the player play the cloud's best move?
        if played_uci == &cloud_best_uci {
            // Good move — record zero eval delta for accuracy computation
            eval_deltas.push(0.0);
            continue;
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
        let after_mate_in = extract_mate_in(&eval_after_score);

        // Compute eval delta (centipawns lost)
        let eval_delta_f = (eval_before_cp - eval_after_cp).max(0.0);
        let eval_delta_i = eval_delta_f as i32;

        // Track eval delta for game accuracy computation
        eval_deltas.push(eval_delta_f);

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;
        let cp_loss = eval_delta_i;

        // Enhanced miss detection
        let (is_miss, miss_type, miss_opportunity_cp) = detect_miss_enhanced(
            eval_before_cp,
            eval_delta_f,
            best_mate_in,
            played_uci,
            &cloud_best_uci,
        );

        // CP-based move classification
        let was_mate_available = best_mate_in.map_or(false, |m| m > 0);
        let is_mate_allowed_after = after_mate_in.map_or(false, |m| m > 0);
        let move_classification = classify_move_by_cp(eval_delta_f, was_mate_available, is_mate_allowed_after);
        let mate_in_val = best_mate_in.unwrap_or(0);

        // Legacy annotation
        let annotation = classify_annotation(win_chance_drop);

        // Determine final annotation: CP-based classification is primary,
        // legacy win-chance-drop annotation is fallback only when CP says move is fine.
        let cp_annotation = classification_to_annotation(move_classification);
        let final_annotation = if is_miss {
            "miss".to_string()
        } else if !cp_annotation.is_empty() {
            cp_annotation.to_string()
        } else {
            annotation.to_string()
        };
        if final_annotation.is_empty() {
            continue;
        }

        // Log every non-trivial eval drop for debugging
        if win_chance_drop > 2.0 || is_miss {
            info!(
                "Cloud move {}: eval_before={:.0}cp eval_after={:.0}cp delta={}cp win_drop={:.1}% class={} annotation={} is_miss={} miss_type={}",
                move_number, eval_before_cp, eval_after_cp, eval_delta_i, win_chance_drop,
                move_classification,
                if final_annotation.is_empty() { "none" } else { &final_annotation },
                is_miss, miss_type
            );
        }

        // Skip if below threshold AND not a miss AND not flagged by CP classification
        if win_chance_drop < min_win_chance_drop && !is_miss {
            if !matches!(move_classification, "INACCURACY" | "MISTAKE" | "BLUNDER" | "MISS") {
                continue;
            }
        }

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
            white_player: game.white.clone(),
            black_player: game.black.clone(),
            played_move: played_uci.clone(),
            best_move: cloud_best_uci,
            best_line: cloud_best_line,
            opponent_punishment,
            opponent_line,
            annotation: final_annotation,
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(&eval_before_score),
            eval_after: format_eval(&eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
            is_miss,
            miss_opportunity_cp,
            move_classification: move_classification.to_string(),
            miss_type: miss_type.to_string(),
            eval_delta: eval_delta_i,
            mate_in: mate_in_val,
        });
    }

    Ok(mistakes_found)
}

// ── Hybrid single-game analysis (cloud → shared local engine fallback) ───────

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
    shared_engine: &SharedEngine,
) -> Result<(Vec<PendingMistakePuzzle>, Vec<f64>), Error> {
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
    let _opponent_color = if player_is_white {
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
    let mut game_eval_deltas: Vec<f64> = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();

    // Track the previous position's eval for legacy miss detection (E0/E1/E2).
    // The enhanced miss detection (mate + opportunity based) is now primary.
    let mut prev_eval_before_opponent_move: Option<f64> = None;

    for (pos_idx, (fen_str, played_uci, moves_before, pred_fen, pred_move)) in
        positions.iter().enumerate()
    {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }

        let move_number = (pos_idx as i32) + 1;

        // Skip the first player move (opening choice preference, not a real mistake)
        if move_number < MIN_PLAYER_MOVE_NUMBER {
            continue;
        }

        counters.positions_analyzed.fetch_add(1, Ordering::SeqCst);

        // ── Helper: get eval for a FEN (cache → cloud → engine) ──
        // Returns (best_uci, best_line, score, depth) or None
        async fn get_eval_for_fen(
            fen_str: &str,
            moves_before: &[String],
            initial_fen: &str,
            multipv: u16,
            client: &reqwest::Client,
            rate_limiter: &CloudRateLimiter,
            fen_cache: &FenCache,
            counters: &HybridCounters,
            shared_engine: &SharedEngine,
            engine_path: &str,
            go_mode: &GoMode,
            uci_options: &[EngineOption],
        ) -> Result<Option<(String, String, vampirc_uci::uci::Score, u32)>, Error> {
            // Check cache first
            {
                let cache = fen_cache.lock().await;
                if let Some(cached) = cache.get(fen_str) {
                    counters.cache_hits.fetch_add(1, Ordering::SeqCst);
                    return Ok(Some((
                        cached.best_uci.clone(),
                        cached.best_line.clone(),
                        cached.score.clone(),
                        cached.depth,
                    )));
                }
            }

            // Try cloud (depth 16 is sufficient for detecting mistakes/blunders)
            let cloud_result =
                fetch_cloud_eval_hybrid(client, fen_str, multipv, 16, rate_limiter).await?;
            if let Some(ref cloud_data) = cloud_result {
                if !cloud_data.pvs.is_empty() {
                    counters.cloud_hits.fetch_add(1, Ordering::SeqCst);
                    let raw_score = cloud_pv_to_score(&cloud_data.pvs[0]);

                    // Lichess Cloud Eval reports cp from the side-to-move's perspective.
                    // Normalize to White's absolute perspective (same as parse_uci_attrs
                    // does for UCI engine output) so callers can uniformly use
                    // score_from_player_perspective(…, Color::White, player_color).
                    let is_black_to_move =
                        fen_str.split_whitespace().nth(1) == Some("b");
                    let score = if is_black_to_move {
                        use vampirc_uci::uci::ScoreValue;
                        vampirc_uci::uci::Score {
                            value: match raw_score.value {
                                ScoreValue::Cp(cp) => ScoreValue::Cp(-cp),
                                ScoreValue::Mate(m) => ScoreValue::Mate(-m),
                            },
                            lower_bound: raw_score.lower_bound,
                            upper_bound: raw_score.upper_bound,
                            wdl: raw_score.wdl,
                        }
                    } else {
                        raw_score
                    };

                    let best_uci = cloud_data.pvs[0]
                        .moves
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_string();
                    let best_line = cloud_data.pvs[0].moves.clone();

                    // Cache it
                    {
                        let mut cache = fen_cache.lock().await;
                        cache.insert(
                            fen_str.to_string(),
                            CachedEval {
                                best_uci: best_uci.clone(),
                                best_line: best_line.clone(),
                                score: score.clone(),
                                depth: cloud_data.depth,
                            },
                        );
                    }

                    return Ok(Some((best_uci, best_line, score, cloud_data.depth)));
                }
            }

            // Fall back to shared local engine (serialized access)
            counters.engine_analyzed.fetch_add(1, Ordering::SeqCst);
            let mut engine_guard = shared_engine.lock().await;

            // Lazily spawn engine on first use
            if engine_guard.is_none() {
                info!("Spawning shared local engine for hybrid fallback: {}", engine_path);
                let ep = PathBuf::from(engine_path);
                let mut proc = BaseEngine::spawn(ep).await?;
                proc.init_uci().await?;
                let rdr = proc.take_reader().ok_or(Error::EngineDisconnected)?;
                for opt in uci_options {
                    if opt.name != "MultiPV" && opt.name != "UCI_Chess960" {
                        proc.set_option(&opt.name, &opt.value).await?;
                    }
                }
                proc.set_option("MultiPV", &multipv.to_string()).await?;
                *engine_guard = Some((proc, rdr));
            }
            let (ref mut proc, ref mut reader) = engine_guard.as_mut().unwrap();

            // Ensure MultiPV is set correctly for this call
            proc.set_option("MultiPV", &multipv.to_string()).await?;

            proc.set_position(initial_fen, moves_before).await?;
            proc.go(go_mode).await?;

            let mut best_lines: Vec<BestMoves> = Vec::new();
            let mut current_batch: Vec<BestMoves> = Vec::new();
            let mut last_depth = 0u32;

            while let Ok(Some(line)) = reader.next_line().await {
                match parse_one(&line) {
                    UciMessage::Info(attrs) => {
                        if let Ok(bm) = parse_uci_attrs(attrs, &fen_str.parse()?, &[]) {
                            if bm.score.lower_bound == Some(true)
                                || bm.score.upper_bound == Some(true)
                            {
                                continue;
                            }
                            let mpv = bm.multipv;
                            let cur_depth = bm.depth;
                            if mpv as usize == current_batch.len() + 1 {
                                current_batch.push(bm);
                                let expected = multipv.min(
                                    Fen::from_ascii(fen_str.as_bytes())
                                        .ok()
                                        .and_then(|f| {
                                            let s = f.into_setup();
                                            let cm = CastlingMode::detect(&s);
                                            Chess::from_setup(s, cm)
                                                .or_else(
                                                    PositionError::ignore_too_much_material,
                                                )
                                                .ok()
                                        })
                                        .map(|p| p.legal_moves().len() as u16)
                                        .unwrap_or(multipv),
                                );
                                if mpv >= expected {
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

            // Release engine lock before returning
            drop(engine_guard);

            if best_lines.is_empty() {
                return Ok(None);
            }

            let best_uci = best_lines
                .first()
                .and_then(|b| b.uci_moves.first())
                .cloned()
                .unwrap_or_default();
            let best_line = best_lines
                .first()
                .map(|b| b.uci_moves.join(" "))
                .unwrap_or_default();
            let score = best_lines[0].score.clone();

            // Cache
            {
                let mut cache = fen_cache.lock().await;
                cache.insert(
                    fen_str.to_string(),
                    CachedEval {
                        best_uci: best_uci.clone(),
                        best_line: best_line.clone(),
                        score: score.clone(),
                        depth: last_depth,
                    },
                );
            }

            Ok(Some((best_uci, best_line, score, last_depth)))
        }

        // ── Get eval BEFORE player's move ──
        let before_result = get_eval_for_fen(
            fen_str,
            moves_before,
            initial_fen,
            1, // MultiPV 1 (requesting 3 causes Lichess Cloud 404 misses on standard depth-cached positions!)
            client,
            rate_limiter,
            fen_cache,
            counters,
            shared_engine,
            engine_path,
            go_mode,
            uci_options,
        )
        .await?;

        let (engine_best_uci, engine_best_line, eval_before_score, before_depth) =
            match before_result {
                Some(r) => r,
                None => continue,
            };

        let eval_before_cp =
            // get_eval_for_fen normalizes all scores (cloud & engine) to White's
            // absolute perspective — always use Color::White as side_to_move.
            score_from_player_perspective(&eval_before_score, Color::White, player_color);
        let best_mate_in = extract_mate_in(&eval_before_score);

        // Did the player play the engine's best move?
        if played_uci == &engine_best_uci {
            // Good move — record zero eval delta for accuracy and update prev eval
            game_eval_deltas.push(0.0);
            prev_eval_before_opponent_move = Some(eval_before_cp);
            continue;
        }

        // ── Get eval AFTER player's move ──
        // Use reduced engine depth for "after" positions — we only need the eval
        // confirmation, not full analysis. Cloud evals are still used at normal depth.
        let fen_after_str = compute_fen_after(fen_str, played_uci)?;
        let mut moves_after = moves_before.to_vec();
        moves_after.push(played_uci.clone());

        let after_go_mode = GoMode::Depth(8); // Reduced depth for after-position engine fallback
        let after_result = get_eval_for_fen(
            &fen_after_str,
            &moves_after,
            initial_fen,
            1,
            client,
            rate_limiter,
            fen_cache,
            counters,
            shared_engine,
            engine_path,
            &after_go_mode,
            uci_options,
        )
        .await?;

        let (after_best_uci, after_best_line, eval_after_score, _after_depth) =
            match after_result {
                Some(r) => r,
                None => continue,
            };

        let eval_after_cp =
            // get_eval_for_fen normalizes to White's absolute perspective.
            score_from_player_perspective(&eval_after_score, Color::White, player_color);
        let after_mate_in = extract_mate_in(&eval_after_score);

        // Compute eval delta (centipawns lost by the player's move)
        let eval_delta_f = (eval_before_cp - eval_after_cp).max(0.0);
        let eval_delta_i = eval_delta_f as i32;

        // Track eval delta for game accuracy computation
        game_eval_deltas.push(eval_delta_f);

        let win_before = get_win_chance(eval_before_cp);
        let win_after = get_win_chance(eval_after_cp);
        let win_chance_drop = win_before - win_after;
        let cp_loss = eval_delta_i;

        // ── Enhanced miss detection ──
        // Primary: mate-based + opportunity-based (from briefing)
        let (enhanced_miss, enhanced_miss_type, enhanced_miss_cp) = detect_miss_enhanced(
            eval_before_cp,
            eval_delta_f,
            best_mate_in,
            played_uci,
            &engine_best_uci,
        );

        // Secondary: legacy E0/E1/E2 miss detection (opponent blundered, player didn't capitalize)
        let mut legacy_miss = false;
        if let Some(e0) = prev_eval_before_opponent_move {
            let opportunity = eval_before_cp - e0;
            let given_back = eval_before_cp - eval_after_cp;
            if opportunity >= 100.0 && given_back >= 30.0 {
                legacy_miss = true;
            }
        }

        // Combine: either detection method flags a miss
        let is_miss = enhanced_miss || legacy_miss;
        let miss_type = if enhanced_miss {
            enhanced_miss_type
        } else if legacy_miss {
            "WINNING_OPPORTUNITY_MISSED"
        } else {
            ""
        };
        let miss_opportunity_cp = if enhanced_miss {
            enhanced_miss_cp
        } else if legacy_miss {
            eval_delta_i
        } else {
            0
        };

        if is_miss {
            info!(
                "Move {}: MISS detected — type={}, eval_before={:.0}cp eval_after={:.0}cp delta={}cp",
                move_number, miss_type, eval_before_cp, eval_after_cp, eval_delta_i
            );
        }

        // CP-based move classification
        let was_mate_available = best_mate_in.map_or(false, |m| m > 0);
        let is_mate_allowed_after = after_mate_in.map_or(false, |m| m > 0);
        let move_classification = classify_move_by_cp(eval_delta_f, was_mate_available, is_mate_allowed_after);
        let mate_in_val = best_mate_in.unwrap_or(0);

        // Legacy annotation (win-chance-drop based)
        let annotation = classify_annotation(win_chance_drop);

        // Log every non-trivial eval drop for debugging
        if win_chance_drop > 2.0 || is_miss {
            info!(
                "Hybrid move {}: eval_before={:.0}cp eval_after={:.0}cp delta={}cp win_drop={:.1}% class={} annotation={} is_miss={} miss_type={}",
                move_number, eval_before_cp, eval_after_cp, eval_delta_i, win_chance_drop,
                move_classification,
                if annotation.is_empty() { "none" } else { annotation },
                is_miss, miss_type
            );
        }

        // Decide whether to create a puzzle:
        // - Standard mistakes: win_chance_drop >= threshold AND annotation non-empty
        // - Miss: detected by enhanced or legacy algorithm
        // - CP-based: classification is INACCURACY or worse
        let is_standard_mistake =
            win_chance_drop >= min_win_chance_drop && !annotation.is_empty();
        let is_cp_notable = matches!(move_classification, "INACCURACY" | "MISTAKE" | "BLUNDER" | "MISS");

        if !is_standard_mistake && !is_miss && !is_cp_notable {
            prev_eval_before_opponent_move = Some(eval_before_cp);
            continue;
        }

        // Determine final annotation: CP-based classification is primary,
        // legacy win-chance-drop annotation is fallback only when CP says move is fine.
        let cp_annotation = classification_to_annotation(move_classification);
        let final_annotation = if is_miss {
            "miss".to_string()
        } else if !cp_annotation.is_empty() {
            cp_annotation.to_string()
        } else {
            annotation.to_string()
        };

        let engine_depth = before_depth as i32;
        let opponent_punishment = after_best_uci;
        let opponent_line = after_best_line;

        mistakes_found.push(PendingMistakePuzzle {
            source: source.to_string(),
            username: username.to_string(),
            game_id: game_id.clone(),
            fen: fen_str.clone(),
            player_color: color_str.to_string(),
            white_player: game.white.clone(),
            black_player: game.black.clone(),
            played_move: played_uci.clone(),
            best_move: engine_best_uci,
            best_line: engine_best_line,
            opponent_punishment,
            opponent_line,
            annotation: final_annotation,
            cp_loss,
            win_chance_drop,
            eval_before: format_eval(&eval_before_score),
            eval_after: format_eval(&eval_after_score),
            move_number,
            engine_depth,
            date_analyzed: now.clone(),
            predecessor_fen: pred_fen.clone().unwrap_or_default(),
            predecessor_move: pred_move.clone().unwrap_or_default(),
            is_miss,
            miss_opportunity_cp,
            move_classification: move_classification.to_string(),
            miss_type: miss_type.to_string(),
            eval_delta: eval_delta_i,
            mate_in: mate_in_val,
        });

        // Update prev eval for next iteration
        prev_eval_before_opponent_move = Some(eval_after_cp);
    }

    // No local engine cleanup here — shared engine is cleaned up by the caller

    Ok((mistakes_found, game_eval_deltas))
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

// ── CRUD commands (PGN-based) ───────────────────────────────────────────────

#[tauri::command]
#[specta::specta]
pub fn get_mistake_puzzles(
    db_path: String,
    filter: MistakePuzzleFilter,
) -> Result<Vec<MistakePuzzle>, Error> {
    let (puzzles, _metadata) = read_mistakes_from_pgn(&db_path)?;

    let mut filtered: Vec<MistakePuzzle> = puzzles
        .into_iter()
        .filter(|p| p.username == filter.username)
        .filter(|p| {
            filter.source.as_ref().map_or(true, |s| &p.source == s)
        })
        .filter(|p| {
            filter.annotation.as_ref().map_or(true, |a| &p.annotation == a)
        })
        .filter(|p| {
            filter.completed.map_or(true, |c| p.completed == c)
        })
        .collect();

    // Apply offset
    if let Some(offset) = filter.offset {
        let offset = offset.max(0) as usize;
        if offset < filtered.len() {
            filtered = filtered[offset..].to_vec();
        } else {
            filtered.clear();
        }
    }

    // Apply limit
    if let Some(limit) = filter.limit {
        filtered.truncate(limit.max(0) as usize);
    }

    Ok(filtered)
}

#[tauri::command]
#[specta::specta]
pub fn update_mistake_puzzle_completion(
    db_path: String,
    puzzle_id: i64,
    completed: i32,
) -> Result<(), Error> {
    update_completion_in_pgn(&db_path, puzzle_id, completed)
}

#[tauri::command]
#[specta::specta]
pub fn get_mistake_stats(
    db_path: String,
    username: String,
    source: Option<String>,
) -> Result<MistakeStats, Error> {
    let (puzzles, metadata) = read_mistakes_from_pgn(&db_path)?;

    let filtered: Vec<&MistakePuzzle> = puzzles
        .iter()
        .filter(|p| p.username == username)
        .filter(|p| {
            source.as_ref().map_or(true, |s| &p.source == s)
        })
        .collect();

    let total = filtered.len() as i64;
    let solved_correct = filtered.iter().filter(|p| p.completed == 1).count() as i64;
    let solved_wrong = filtered.iter().filter(|p| p.completed == 2).count() as i64;
    let unsolved = filtered.iter().filter(|p| p.completed == 0).count() as i64;
    let blunders = filtered.iter().filter(|p| p.annotation == "??").count() as i64;
    let mistakes = filtered.iter().filter(|p| p.annotation == "?").count() as i64;
    let inaccuracies = filtered.iter().filter(|p| p.annotation == "?!").count() as i64;
    let misses = filtered.iter().filter(|p| p.is_miss == 1).count() as i64;
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
        misses,
        accuracy,
        game_accuracy: metadata.game_accuracy,
    })
}

#[tauri::command]
#[specta::specta]
pub fn delete_mistake_puzzles(
    db_path: String,
    _username: String,
    _source: Option<String>,
) -> Result<(), Error> {
    // Delete the PGN file entirely (each file is per-analysis run)
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path)?;
    }
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn init_mistake_db(db_path: String) -> Result<(), Error> {
    // Ensure the parent directory exists; no DB initialization needed for PGN
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
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
        "??" => 1000,   // Blunders are often obvious to spot
        "?" => 1400,    // Mistakes require moderate skill
        "?!" => 1800,   // Inaccuracies are subtle
        "miss" => 1200, // Missed opportunities — often tactical
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
    // Read mistakes from PGN file
    let (puzzles, _metadata) = read_mistakes_from_pgn(&mistake_db_path)?;

    let filtered: Vec<&MistakePuzzle> = puzzles
        .iter()
        .filter(|p| p.username == username && p.source == source)
        .collect();

    // Create/open the puzzle DB (standard Lichess puzzle format)
    let puzzle_conn = open_puzzle_db(&puzzle_db_path)?;
    puzzle_conn.execute_batch(CREATE_PUZZLE_TABLES)?;

    // Insert themes
    let theme_names = ["blunder", "mistake", "inaccuracy", "miss", "my-mistakes"];
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
    let miss_theme_id = get_theme_id("miss")?;
    let my_mistakes_theme_id = get_theme_id("my-mistakes")?;

    // Clear existing puzzles (re-export replaces all)
    puzzle_conn.execute("DELETE FROM puzzle_themes", [])?;
    puzzle_conn.execute("DELETE FROM puzzles", [])?;

    let mut exported = 0i32;
    let mut skipped_no_moves = 0i32;
    let mut skipped_too_short = 0i32;

    for row in &filtered {
        // Determine the puzzle FEN and moves.
        // Standard Lichess puzzle format: FEN has opponent-to-move, first move
        // in `moves` is auto-played (opponent's move), then user must find the
        // correct reply.
        //
        // Case 1: We have predecessor info → standard format:
        //   FEN = position before opponent's last move
        //   moves = [opponent_move, player_best_move, ...]
        //
        // Case 2: No predecessor (e.g. first move) but we have the mistake FEN
        //   and best_line → construct from the mistake's played move:
        //   Compute FEN after player's bad move (opponent to move).
        //   moves = [opponent_punishment, player's best response...]
        //   The puzzle becomes: "opponent punishes your mistake — find the defense"
        let best_move = row.best_line.split_whitespace().next().unwrap_or_default();

        let (puzzle_fen, puzzle_moves) =
            if !row.predecessor_fen.is_empty() && !row.predecessor_move.is_empty() {
                // Standard: predecessor FEN + opponent's move + best continuation
                let moves = format!("{} {}", row.predecessor_move, best_move);
                (row.predecessor_fen.clone(), moves)
            } else if !row.opponent_punishment.is_empty() && !row.best_line.is_empty() {
                // Fallback: compute FEN after player's bad move, use opponent's
                // punishment as the auto-played move, and best_line as the solution
                match compute_fen_after(&row.fen, &row.played_move) {
                    Ok(fen_after_mistake) => {
                        // fen_after_mistake has opponent to move — correct for puzzle format
                        // moves: opponent_punishment (auto-played), then best reply from best_line
                        let moves = format!("{} {}", row.opponent_punishment, best_move);
                        (fen_after_mistake, moves)
                    }
                    Err(e) => {
                        info!("Skipping mistake (can't compute fen after): {} — {}", row.fen, e);
                        skipped_no_moves += 1;
                        continue;
                    }
                }
            } else if !row.best_line.is_empty() {
                // Last resort: use mistake FEN directly with best_line.
                // This only works if the FEN has the right side-to-move for puzzles.
                // The player made a mistake here → best_line starts with player's
                // correct move. But Lichess format expects opponent-to-move FEN.
                // Skip these as they don't fit the standard format.
                skipped_no_moves += 1;
                continue;
            } else {
                skipped_no_moves += 1;
                continue;
            };

        // Ensure moves has an even number of tokens (ends on user answer)
        let move_tokens: Vec<&str> = puzzle_moves.split_whitespace().collect();
        let trimmed = if move_tokens.len() % 2 == 1 {
            move_tokens[..move_tokens.len() - 1].join(" ")
        } else {
            puzzle_moves.clone()
        };

        if trimmed.split_whitespace().count() < 2 {
            skipped_too_short += 1;
            continue;
        }

        let rating = rating_from_annotation(&row.annotation);
        let is_miss = row.is_miss != 0;

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
            "miss" => Some(miss_theme_id),
            _ => None,
        };
        if let Some(tid) = annotation_theme_id {
            puzzle_conn.execute(
                "INSERT OR IGNORE INTO puzzle_themes (puzzle_id, theme_id) VALUES (?1, ?2)",
                rusqlite::params![puzzle_id, tid],
            )?;
        }

        // Also link to miss theme if it's a miss (can be both mistake + miss)
        if is_miss && row.annotation != "miss" {
            puzzle_conn.execute(
                "INSERT OR IGNORE INTO puzzle_themes (puzzle_id, theme_id) VALUES (?1, ?2)",
                rusqlite::params![puzzle_id, miss_theme_id],
            )?;
        }

        exported += 1;
    }

    info!(
        "Export complete: {} puzzles exported, {} skipped (no moves/predecessor), {} skipped (too short), {} total mistakes",
        exported, skipped_no_moves, skipped_too_short, filtered.len()
    );

    // Explicitly close the connection to release file handles before the
    // frontend tries to read the DB (especially important on Windows).
    drop(puzzle_conn);

    Ok(exported)
}

#[derive(Debug, Serialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct AlternativeEvalResult {
    pub cp_loss: f64,
    pub is_acceptable: bool,
}

#[tauri::command]
#[specta::specta]
pub async fn evaluate_puzzle_move_alternative(
    engine_path: String,
    fen_before: String,
    uci_move: String,
) -> Result<AlternativeEvalResult, Error> {
    let client = reqwest::Client::new();
    let rate_limiter = CloudRateLimiter::new();
    
    // 1. Evaluate fen_before
    let mut best_cp = 0.0;
    let mut use_cloud = false;
    
    if let Ok(Some(cloud_res)) = fetch_cloud_eval_hybrid(&client, &fen_before, 1, 10, &rate_limiter).await {
        if !cloud_res.pvs.is_empty() {
             best_cp = score_from_player_perspective(&cloud_pv_to_score(&cloud_res.pvs[0]), Color::White, Color::White);
             use_cloud = true;
        }
    }
    
    let fen_after = compute_fen_after(&fen_before, &uci_move)?;
    let mut actual_cp = 0.0;

    // Try cloud for after as well
    let mut after_cloud_success = false;
    if let Ok(Some(cloud_res)) = fetch_cloud_eval_hybrid(&client, &fen_after, 1, 10, &rate_limiter).await {
        if !cloud_res.pvs.is_empty() {
             actual_cp = score_from_player_perspective(&cloud_pv_to_score(&cloud_res.pvs[0]), Color::White, Color::White);
             after_cloud_success = true;
        }
    }

    // Fall back to local engine if cloud missed either
    if !use_cloud || !after_cloud_success {
        if PathBuf::from(&engine_path).exists() {
            let mut p = BaseEngine::spawn(PathBuf::from(&engine_path)).await?;
            p.init_uci().await?;
            if let Some(mut reader) = p.take_reader() {
                if !use_cloud {
                    p.set_position(&fen_before, &[]).await?;
                    p.go(&GoMode::Depth(12)).await?;
                    
                    let mut last_depth = 0;
                    while let Ok(Some(line)) = reader.next_line().await {
                        match vampirc_uci::parse_one(&line) {
                            UciMessage::Info(attrs) => {
                                if let Ok(bm) = parse_uci_attrs(attrs, &fen_before.parse()?, &[]) {
                                    if bm.depth >= last_depth {
                                        best_cp = score_from_player_perspective(&bm.score, Color::White, Color::White);
                                        last_depth = bm.depth;
                                    }
                                }
                            }
                            UciMessage::BestMove { .. } => break,
                            _ => {}
                        }
                    }
                }

                if !after_cloud_success {
                    p.set_position(&fen_after, &[]).await?;
                    p.go(&GoMode::Depth(12)).await?;
                    
                    let mut last_depth = 0;
                    while let Ok(Some(line)) = reader.next_line().await {
                        match vampirc_uci::parse_one(&line) {
                            UciMessage::Info(attrs) => {
                                if let Ok(bm) = parse_uci_attrs(attrs, &fen_after.parse()?, &[]) {
                                    if bm.depth >= last_depth {
                                        actual_cp = score_from_player_perspective(&bm.score, Color::White, Color::White);
                                        last_depth = bm.depth;
                                    }
                                }
                            }
                            UciMessage::BestMove { .. } => break,
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // cp loss is the difference. The evaluations are normalized to white's absolute perspective,
    // but wait! If fen_before is black's turn, we must negate to get the active player's cp!
    let is_black = fen_before.split_whitespace().nth(1).unwrap_or("w") == "b";
    
    let mut cp_loss = if is_black {
        actual_cp - best_cp
    } else {
        best_cp - actual_cp
    };
    
    cp_loss = cp_loss.max(0.0);
    
    // Accept if loss <= 50cp
    Ok(AlternativeEvalResult {
        cp_loss,
        is_acceptable: cp_loss <= 50.0,
    })
}
