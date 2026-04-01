# Context: Learn from Mistakes Feature

## Project
En Croissant — A Modern Chess Database (Tauri + React + Mantine + Rust)

## Feature: Learn from Mistakes
A feature inspired by Lichess's "Learn from Mistakes." It imports all games from a user's Chess.com or Lichess account, batch-analyzes them with a UCI engine (Komodo or Stockfish), extracts positions where the player made inaccuracies/mistakes/blunders, and presents them as interactive puzzles.

### Two Puzzle Modes
1. **Find the Correct Move** — Board shows position before your mistake. You must find the engine's best move.
2. **Punish the Mistake** — Board shows position after your mistake. You play as the opponent and find the punishing move.

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
4. If existing puzzles found → resume directly to puzzle view
5. Otherwise → start `analyze_games_for_mistakes` command
6. Backend queries all player games from DB, runs engine analysis
7. Mistakes stored in `mistake_puzzles.db3` (app data dir)
8. Frontend fetches puzzles, shows interactive puzzle board
9. User solves puzzles, completion tracked per puzzle

## Files Modified
- `src-tauri/src/chess.rs` (made BestMoves fields + parse_uci_attrs public)
- `src-tauri/src/error.rs` (added Rusqlite error variant + From impl)
- `src-tauri/src/main.rs` (registered module + commands)
- `src/components/Sidebar.tsx` (added sidebar entry)
- `src/bindings/generated.ts` (added commands + types)
- `src/routeTree.gen.ts` (registered route)

## Build Status
- Frontend (Vite): ✅ Builds successfully (8949 modules, 15s)
- Backend (Cargo): ⚠️ Cannot verify (Rust not installed on dev machine)
  - All API signatures verified against existing codebase
  - rusqlite error handling added to error.rs
  - UciMove::from_move used (matching existing patterns)
  - Borrow-safe HashMap caching for player/site names

## Latest Fixes (2026-04-02)
- Fixed Tauri async Send error in mistake analysis by removing rusqlite connection usage from async awaits.
  - `analyze_single_game` now returns in-memory pending puzzle rows.
  - Database writes happen after async engine analysis completes via `insert_pending_mistakes`.
- Confirmed full release build succeeds:
  - Output: `src-tauri/target/release/en-croissant.exe`
  - Build time: ~2m 21s
  - Remaining warnings are non-blocking dead-code/unused-field warnings in `mistake_puzzle.rs`.
- `src/translation/en-US.json` (added keys)
- `src/translation/*.json` (all 15 other files: added SideBar.Learn)

## Files Created
- `src-tauri/src/mistake_puzzle.rs`
- `src/routes/learn-from-mistakes.tsx`
- `src/components/learn-from-mistakes/LearnFromMistakes.tsx`
- `src/components/learn-from-mistakes/SetupPanel.tsx`
- `src/components/learn-from-mistakes/AnalysisProgress.tsx`
- `src/components/learn-from-mistakes/MistakePuzzleBoard.tsx`
- `src/components/learn-from-mistakes/StatsPanel.tsx`
