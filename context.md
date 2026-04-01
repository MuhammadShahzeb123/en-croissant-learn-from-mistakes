# Context: Learn from Mistakes Feature

## Project
En Croissant — A Modern Chess Database (Tauri + React + Mantine + Rust)

## Feature: Learn from Mistakes
A feature inspired by Lichess's "Learn from Mistakes." It imports all games from a user's Chess.com or Lichess account, analyzes them with either a **local UCI engine** or the **Lichess Cloud Evaluation API**, extracts positions where the player made inaccuracies/mistakes/blunders, and exports them as puzzles in the standard Lichess puzzle DB format — accessible from the existing Puzzles tab on the home page.

### Engine Options
- **Local Engine**: Select any installed UCI engine (Stockfish, etc.). Requires CPU, configurable depth/threads/hash.
- **Lichess Cloud**: Uses pre-computed Lichess cloud evaluations (https://lichess.org/api/cloud-eval). No local engine needed. Positions not in the cloud DB are skipped. Much faster for common positions.

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
- `??` (Blunder) — Win chance drop > 20%
- `?` (Mistake) — Win chance drop > 10%
- `?!` (Inaccuracy) — Win chance drop > 5%
- User can filter which types generate puzzles

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
- Frontend (Vite): ✅ Builds successfully (8945 modules, ~15s)
- Backend (Cargo): ✅ Builds successfully (release, ~2m 07s)
  - Only non-blocking warnings: unused struct `MistakeAnalysisProgress`, unused fields in `GameRecord`

## Latest Fixes

### Session 4: Lichess Cloud Engine + Puzzle Loading Fix
- **Added Lichess Cloud Eval for mistake analysis**: Users can now choose "Lichess Cloud Eval" in the engine selector.
  - Rust: `fetch_cloud_eval(client, fen, multipv)` calls `https://lichess.org/api/cloud-eval` via `reqwest`.
  - Rust: `analyze_single_game_cloud()` — cloud variant of single-game analysis (no UCI process, HTTP only).
  - Rust: `cloud_pv_to_score()` converts cloud API response PVs to `vampirc_uci::Score` for reuse with `score_from_player_perspective()`.
  - Rust: `AnalyzeGamesRequest` gained `engine_type: String` ("local" | "lichess") field.
  - Rust: `analyze_games_for_mistakes()` branches on `engine_type` — spawns local engine OR uses `reqwest::Client` for cloud.
  - Rust: `Error::HttpError(String)` variant added for cloud API errors.
  - TS: `AnalysisConfig.engineType` added ("local" | "lichess").
  - TS: `AnalyzeGamesRequest` type updated with `engineType` field in `generated.ts`.
  - TS: `SetupPanel.tsx` — engine selector now has grouped options: "Cloud" (Lichess Cloud Eval) and "Local" (installed engines). Depth/Threads/Hash hidden for cloud. Info alert shown when cloud selected.
  - TS: `AnalysisProgress.tsx` — passes `engineType` to backend; shows "Cloud Evaluation" instead of depth for cloud.
- **Fixed puzzle DB not loading after analysis**: After analysis completed, the Puzzle Training tab opened but showed an empty board with no DB selected.
  - Root cause: `onComplete()` in AnalysisProgress set `view="setup"` which unmounted the component. `navigateToPuzzles()` executed after unmount but atom/route updates were unreliable.
  - Fix: Removed `await` on `navigateToPuzzles()` — it's fire-and-forget since the Jotai atom writes and router navigate work independently of component lifecycle.
  - Added auto-puzzle generation in `Puzzles.tsx`: when a DB is selected and no puzzles are loaded, the first puzzle is auto-generated (via `useEffect` + `useRef` guard to prevent re-triggers).
- **Deleted old `mistake_puzzles.db3`** — stale data from broken eval logic.

### Session 3: Puzzle Integration + Eval Bug Fix
- **Fixed Critical Eval Bug for Black Players**: `normalize_cp()` was color-blind for the "after" position.
  - For Black players, `eval_after_cp = -normalize_cp(score, player_color)` double-inverted, making `win_chance_drop` always negative → zero mistakes detected for Black games.
  - Replaced with `score_from_player_perspective(score, side_to_move, player_color)` that correctly handles both before (side_to_move=player) and after (side_to_move=opponent) positions.
  - White games always worked; Black games had 0 detections → explains "only 14 dubious from 54+ games."
- **Added Predecessor Tracking**: Analysis now tracks the opponent's FEN and move before each player position.
  - `predecessor_fen` and `predecessor_move` stored in `PendingMistakePuzzle` struct + DB columns.
  - Used to construct Lichess-format puzzles: `fen=predecessor_fen`, `moves=predecessor_move + " " + best_line`.
- **New Command: `export_mistakes_to_puzzle_db`**: Converts mistake_puzzles to standard Lichess puzzle DB format.
  - Creates `puzzles`, `themes`, `puzzle_themes` tables.
  - Assigns ratings: blunder=1000, mistake=1400, inaccuracy=1800.
  - Themes: "blunder", "mistake", "inaccuracy", "my-mistakes".
  - Trims moves to even length (so puzzle ends on user answer, not auto-play).
- **Removed Custom Puzzle Board**: `MistakePuzzleBoard.tsx` no longer imported (was broken — board didn't render).
  - After analysis, user is redirected to the existing Puzzles tab with the new DB auto-selected.
  - `LearnFromMistakes.tsx` simplified to setup + analyzing views only.
  - `AnalysisProgress.tsx` calls `exportMistakesToPuzzleDb` then `navigateToPuzzles()`.

### Session 2: Progress + Navigation Fixes
- Fixed progress stuck at 0%: position-level progress via `on_position_progress` callback.
- Fixed timer: `setInterval(1000)` + Jotai atom for start time.
- Fixed navigation loss: all state in Jotai atoms (survive route changes).
- Fixed UCI options forwarding: Threads/Hash passed to engine.
- Fixed Tauri async Send: `analyze_single_game` returns in-memory rows.

## Files Created
- `src-tauri/src/mistake_puzzle.rs`
- `src/routes/learn-from-mistakes.tsx`
- `src/components/learn-from-mistakes/LearnFromMistakes.tsx`
- `src/components/learn-from-mistakes/SetupPanel.tsx`
- `src/components/learn-from-mistakes/AnalysisProgress.tsx`
- `src/components/learn-from-mistakes/MistakePuzzleBoard.tsx` (dead code — unused but retained)
- `src/components/learn-from-mistakes/StatsPanel.tsx`

## Important Notes
- **Delete old mistake data**: Old `mistake_puzzles.db3` was generated with the broken eval logic (only White game mistakes). User should re-analyze after this fix.
- **Puzzle DB location**: Exported to `puzzles/my_mistakes_<username>_<source>.db3`
