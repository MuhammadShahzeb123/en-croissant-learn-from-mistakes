# Context: Learn from Mistakes Feature

## Project
En Croissant — A Modern Chess Database (Tauri + React + Mantine + Rust)

## Feature: Learn from Mistakes
A feature inspired by Lichess's "Learn from Mistakes." It imports all games from a user's Chess.com or Lichess account, analyzes them with either a **local UCI engine** or the **Lichess Cloud Evaluation API**, extracts positions where the player made inaccuracies/mistakes/blunders, and exports them as puzzles in the standard Lichess puzzle DB format — accessible from the existing Puzzles tab on the home page.

### Engine Options
- **Local Engine**: Select any installed UCI engine (Stockfish, etc.). Requires CPU, configurable depth/threads/hash.
- **Lichess Cloud**: Uses pre-computed Lichess cloud evaluations (https://lichess.org/api/cloud-eval). No local engine needed. Positions not in the cloud DB are skipped. Much faster for common positions.
- **Hybrid (Cloud + Local)**: Tries Lichess Cloud Eval first (free, fast), then falls back to a local engine for positions not in the cloud. Best of both worlds. Features:
  - Waterfall pattern: cloud first → shared local engine fallback per position
  - Smart position filtering: skips first 4 player moves (opening theory)
  - In-memory FEN cache: avoids re-analyzing identical positions across games
  - Parallel analysis: up to 2 games concurrently via tokio semaphore (reduced from 4)
  - **Single shared engine** behind `Arc<TokioMutex>` — prevents multiple engine processes
  - Cloud rate limiting: ≥1s between Lichess API requests, HTTP 429 retry (60s wait)
  - Cloud depth threshold: only accepts cloud evals with depth ≥ 20
  - Miss detection: tracks eval before opponent's move to detect missed opportunities

### Puzzle Flow
1. User configures analysis on the **Learn** page (account, engine, depth, mistake types)
2. Backend analyzes all games, finds mistakes, stores metadata in `mistake_puzzles.db3`
3. After analysis, mistakes are **exported** to a standard puzzle DB (e.g., `my_mistakes_username_lichess.db3`) in the puzzles directory
4. User is automatically navigated to the **Puzzles tab** with the new DB selected
5. Puzzles work through the existing PuzzleBoard system with full Chessground board, move validation, streaks, and timing

## Architecture

### Backend (Rust)
- **`src-tauri/src/mistake_puzzle.rs`** — Main module containing:
  - SQLite table `mistake_puzzles` (created via rusqlite, not diesel)
  - `analyze_games_for_mistakes` — Tauri command that:
    - Queries games from a user's database (using diesel)
    - Decodes binary-encoded moves using `iter_mainline_move_bytes` + `decode_move`
    - Runs UCI engine analysis (MultiPV=2) at each position where it's the player's turn
    - Compares player's move against engine's best move using win chance drop thresholds
    - For mistakes: stores FEN, played move, best move, best line, opponent punishment line
    - Supports cancellation via existing `analysis_cancel_flags` pattern
    - Emits progress events
  - `get_mistake_puzzles` — CRUD retrieval with filters (username, source, annotation, completion)
  - `update_mistake_puzzle_completion` — Mark puzzle solved/failed
  - `get_mistake_stats` — Aggregate stats (total, by type, accuracy)
  - `delete_mistake_puzzles` — Clear puzzles
  - `init_mistake_db` — Create the database/table

- **Modified `src-tauri/src/chess.rs`**:
  - Made `BestMoves` fields public
  - Made `parse_uci_attrs` function public

- **Modified `src-tauri/src/main.rs`**:
  - Registered `mistake_puzzle` module
  - Added all 6 commands to `collect_commands!` macro

### Frontend (TypeScript/React)
- **`src/routes/learn-from-mistakes.tsx`** — Route at `/learn-from-mistakes`
- **`src/components/Sidebar.tsx`** — Added `IconTargetArrow` sidebar entry "Learn"
- **`src/components/learn-from-mistakes/LearnFromMistakes.tsx`** — Main orchestrator page with 3 views: Setup → Analyzing → Puzzles
- **`src/components/learn-from-mistakes/SetupPanel.tsx`** — Account selector, engine picker, depth slider, annotation checkboxes
- **`src/components/learn-from-mistakes/AnalysisProgress.tsx`** — Progress bar, cancel button, listens to Tauri progress events
- **`src/components/learn-from-mistakes/MistakePuzzleBoard.tsx`** — Interactive puzzle board:
  - Uses Chessground for board display
  - Two modes: "Find Correct" / "Punish Mistake"
  - Hint (highlights source square), solution reveal, retry
  - Tracks completion (correct/incorrect)
  - Shows eval before/after, centipawn loss, annotation badge
- **`src/components/learn-from-mistakes/StatsPanel.tsx`** — Ring progress chart for accuracy, stat breakdown
- **`src/bindings/generated.ts`** — Added commands + types for MistakePuzzle, MistakeStats, MistakePuzzleFilter
- **`src/routeTree.gen.ts`** — Registered the new route
- **`src/translation/en-US.json`** — 40+ translation keys added
- **All other translation files** — `SideBar.Learn` key added

## Annotation Thresholds
### Legacy (Win Chance Drop — still used for backward compat)
- `??` (Blunder) — Win chance drop > 20%
- `?` (Mistake) — Win chance drop > 10%
- `?!` (Inaccuracy) — Win chance drop > 5%
- `miss` (Missed Opportunity) — Enhanced detection (see below)
- User can filter which types generate puzzles

### New: Centipawn-Based Classification (move_classification field)
- `BEST` — Eval delta ≤ 10cp
- `EXCELLENT` — Eval delta ≤ 25cp
- `GOOD` — Eval delta ≤ 50cp
- `INACCURACY` — Eval delta ≤ 100cp
- `MISTAKE` — Eval delta ≤ 300cp
- `BLUNDER` — Eval delta > 300cp OR allows opponent forced mate
- `MISS` — Forced mate was available and not played

### Enhanced Miss Detection (all analysis paths)
- **MATE_MISSED**: Forced mate available (mateIn > 0) but player didn't play it
- **WINNING_OPPORTUNITY_MISSED**: Position had ≥150cp advantage from player's perspective AND eval delta ≥ 100cp AND player didn't play best move
- **Legacy E0/E1/E2** (hybrid): Opponent blundered ≥100cp AND player gave back ≥30cp
- Both systems run in parallel; either detection flags a miss

### Game Accuracy (Chess.com-style)
- Formula: `103.1668 * exp(-0.04354 * (evalDelta/100)) - 3.1669` per move
- All player moves tracked (best moves = 0cp delta, non-best = computed delta)
- Stored in `analysis_metadata` table, returned in MistakeStats.gameAccuracy
- Displayed as a separate ring in StatsPanel

## Data Flow
1. User selects account (Lichess/Chess.com from sessionsAtom)
2. User selects engine, depth, mistake types
3. App checks if user's games database exists (from previous import)
4. Start `analyze_games_for_mistakes` command
5. Backend queries all player games from DB, runs engine analysis per position
6. Mistakes stored in `mistake_puzzles.db3` (app data dir) with metadata
7. After analysis, `export_mistakes_to_puzzle_db` creates a standard puzzle DB
8. User redirected to Puzzles tab with new DB auto-selected
9. Puzzles rendered by the existing PuzzleBoard system (full Chessground board)

## Files Modified
- `src-tauri/src/chess.rs` (made BestMoves fields + parse_uci_attrs public)
- `src-tauri/src/error.rs` (added Rusqlite error variant + From impl)
- `src-tauri/src/main.rs` (registered module + commands)
- `src/components/Sidebar.tsx` (added sidebar entry)
- `src/bindings/generated.ts` (added commands + types)
- `src/routeTree.gen.ts` (registered route)

## Build Status
- Frontend: ✅ No TypeScript errors in any modified files
- Backend (Cargo): ✅ `cargo check` passes. Only non-blocking warnings: unused `est_left`, dead code fields

## Latest Fixes

### Session 7: Engine Freeze Fix + Miss Detection + Puzzle Visibility + Depth Optimization
- **CRITICAL FIX: Hybrid mode no longer spawns multiple engine processes** (mistake_puzzle.rs):
  - Root cause: Each of 4 parallel `tokio::spawn` tasks lazily spawned its own `BaseEngine` process when cloud eval failed, causing 4 × 6-thread engines = 24 threads fighting over 12 logical cores → system freeze.
  - Fix: Introduced `SharedEngine = Arc<TokioMutex<Option<(BaseEngine, EngineReader)>>>` — a single engine instance shared across all parallel tasks. Engine mutex is acquired only when cloud fails.
  - Reduced parallel semaphore from `Semaphore(4)` → `Semaphore(2)`. Cloud requests remain parallel; engine access is serialized.
  - Added explicit shared engine cleanup after all tasks complete: `proc.quit().await.ok()`.
  - Refactored `analyze_single_game_hybrid` — replaced per-task lazy spawn with shared engine parameter.
  - Extracted `get_eval_for_fen()` helper: cache → cloud → shared engine fallback in one unified flow.

- **Added "Miss" detection** (mistake_puzzle.rs):
  - A "miss" = opponent blundered (position improved for player) but player didn't capitalize (gave back the advantage).
  - Detection: E0 = eval before opponent's move, E1 = eval before player's move, E2 = eval after player's move. Miss conditions: `(E1 - E0) >= 100cp` AND `(E1 - E2) >= 30cp`.
  - New DB columns: `is_miss INTEGER DEFAULT 0`, `miss_opportunity_cp INTEGER DEFAULT 0`.
  - Miss-only positions (not a standard mistake) get annotation `"miss"`.
  - A position CAN be both a mistake and a miss simultaneously (`is_miss=1` + annotation="?" etc.).
  - New "miss" theme in puzzle export. Rating for miss puzzles: 1200.
  - Frontend: "Missed Opportunity" checkbox in SetupPanel, cyan badge in StatsPanel, `misses` field in MistakeStats.
  - DB migration: `ALTER TABLE` for `is_miss` and `miss_opportunity_cp` columns with index.

- **Fixed classification logging** (mistake_puzzle.rs):
  - Added `info!()` logging for every non-trivial eval drop (>2% win chance) in all three analysis paths (local, cloud, hybrid).
  - Logs: move number, eval_before, eval_after, win_chance_drop%, annotation, cp_loss, is_miss.
  - Added annotation distribution summary after all analysis completes (blunders, mistakes, inaccuracies, misses).
  - Helps diagnose issues where only inaccuracies were being detected (the thresholds are correct at >5%, >10%, >20%).

- **Fixed puzzle visibility after export** (Puzzles.tsx):
  - Root cause: `puzzleDbs` list was loaded ONCE on mount via `getPuzzleDatabases()`. Newly exported DB files weren't in this list → `puzzleDbs.some(db => db.path === selectedDb)` guard failed → no puzzle loaded.
  - Fix: Re-scan puzzle databases whenever `selectedDb` changes (added `selectedDb` as dependency).
  - Relaxed auto-generate guard: if `selectedDb` is set but not in `puzzleDbs`, still attempt `generatePuzzle(selectedDb)` directly.

- **Depth & UCI optimization for Ryzen 5 3600** (SetupPanel.tsx):
  - Default depth: 18 → **10** (much faster per-position analysis).
  - Default threads: 1 → **6** (half the 12 logical threads, leaves headroom).
  - Default hash: 128MB → **256MB** (optimal for depth 10 with 16GB+ RAM).
  - Slider max: 30 → **20** (depth 20+ impractical for batch analysis of hundreds of positions).
  - Annotations default now includes `"miss"` alongside `"??"`, `"?"`, `"?!"`.

## Build Status
- Frontend: ✅ No TypeScript errors in any modified files.
- Backend (Cargo): ✅ `cargo check` passes. Only non-blocking warnings: unused `est_left`, dead code in `CloudEvalResponse`, `MistakeAnalysisProgress`, `GameRecord` fields.

## Latest Changes

### Session 14+: Database Migration Fix
Fixed database initialization sequence to prevent index creation on non-existent columns in existing databases.

- **Problem**: CREATE_MISTAKE_PUZZLES_TABLE included index creation statements that executed before ALTER TABLE migrations added new columns to old databases
- **Solution**:
  - Removed all CREATE INDEX statements from CREATE_MISTAKE_PUZZLES_TABLE constant
  - Moved index creation to `ensure_table()` function AFTER all ALTER TABLE migrations complete
  - Changed final index creation to use `execute_batch()` with error propagation instead of ignoring errors
  - This ensures old databases have columns added before any indexes are created
- **Impact**: Fixes "no such column: is_miss" error when running analysis on databases from previous versions

### Session 8: Enhanced Missed Opportunity Detection Pipeline
Replaced naive CPL-only classification with a proper miss detection system across all analysis paths.

- **MultiPV upgraded from 2 → 3** in all analysis paths (local, cloud, hybrid):
  - Local engine: `proc.set_option("MultiPV", "3")`
  - Cloud API: `fetch_cloud_eval(client, fen, 3)`
  - Hybrid: `get_eval_for_fen(..., multipv=3, ...)`

- **New helper functions** (mistake_puzzle.rs):
  - `extract_mate_in(score)` — extracts forced mate count from engine score
  - `classify_move_by_cp(eval_delta, was_mate_available, is_mate_allowed_after)` — centipawn-threshold classification (BEST/EXCELLENT/GOOD/INACCURACY/MISTAKE/BLUNDER/MISS)
  - `classification_to_annotation(classification)` — maps new classification to legacy annotation
  - `detect_miss_enhanced(best_eval, eval_delta, best_mate_in, actual_move, best_move)` — mate-based + opportunity-based miss detection
  - `move_accuracy_from_delta(eval_delta)` — Chess.com accuracy formula per move

- **Enhanced miss detection in ALL 3 analysis paths** (previously hybrid-only):
  - Local engine path: now runs `detect_miss_enhanced()` for every analyzed position
  - Cloud path: now runs `detect_miss_enhanced()` for every analyzed position
  - Hybrid path: runs both enhanced detection AND legacy E0/E1/E2 detection, combines results

- **Eval delta tracking for game accuracy**:
  - All paths now track eval deltas for every player move (including best moves at 0cp delta)
  - After analysis, computes game accuracy: `avg(103.1668 * exp(-0.04354 * delta/100) - 3.1669)`
  - Stored in new `analysis_metadata` table (username, source, game_accuracy, total_moves_analyzed)
  - Returned via `MistakeStats.gameAccuracy` field

- **New DB columns** (with migration for existing DBs):
  - `move_classification TEXT` — BEST/EXCELLENT/GOOD/INACCURACY/MISTAKE/BLUNDER/MISS
  - `miss_type TEXT` — MATE_MISSED/WINNING_OPPORTUNITY_MISSED or empty
  - `eval_delta INTEGER` — centipawn delta (positive = player lost value)
  - `mate_in INTEGER` — forced mate count (0 if none, positive = for player)
  - `analysis_metadata` table for game-level accuracy stats

- **Dual classification system** — Both systems coexist for backward compat:
  - `annotation` field: legacy win-chance-drop annotation ("??", "?", "?!", "miss")
  - `move_classification` field: new centipawn-threshold classification
  - Puzzle creation criteria: OR of win-chance threshold, miss detection, AND CP classification

- **Frontend updates**:
  - `MistakePuzzle` type: added `moveClassification`, `missType`, `evalDelta`, `mateIn`
  - `MistakeStats` type: added `gameAccuracy`
  - `score.ts`: added `moveAccuracyFromDelta()`, `gameAccuracyFromDeltas()`, `classifyMoveByCp()`, `getMissTypeLabel()`, `getClassificationColor()`
  - `StatsPanel.tsx`: displays game accuracy ring alongside puzzle accuracy ring
  - `MistakePuzzleBoard.tsx`: shows miss type badge (MATE_MISSED/WINNING_OPPORTUNITY_MISSED), classification badge with eval delta, "miss" annotation badge in cyan
