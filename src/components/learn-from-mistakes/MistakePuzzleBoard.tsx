"use no memo";
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Card,
  Group,
  SegmentedControl,
  Stack,
  Text,
  Tooltip,
} from "@mantine/core";
import { useElementSize } from "@mantine/hooks";
import {
  IconArrowLeft,
  IconArrowRight,
  IconBulb,
  IconEye,
  IconRotate,
} from "@tabler/icons-react";
import {
  type Move,
  type NormalMove,
  makeUci,
  parseSquare,
  parseUci,
} from "chessops";
import { chessgroundDests, chessgroundMove } from "chessops/compat";
import { makeFen, parseFen } from "chessops/fen";
import { useTranslation } from "react-i18next";
import { Chessground } from "@/chessground/Chessground";
import { commands } from "@/bindings";
import type { MistakePuzzle, MistakeStats } from "@/bindings";
import { positionFromFen } from "@/utils/chessops";
import classes from "@/styles/Chessboard.module.css";
import type { AnalysisConfig } from "@/state/atoms";
import PromotionModal from "../boards/PromotionModal";

type PuzzleMode = "find_correct" | "punish_mistake";
type PuzzleState = "solving" | "correct" | "incorrect" | "revealed";

interface MistakePuzzleBoardProps {
  puzzles: MistakePuzzle[];
  config: AnalysisConfig;
  onStatsUpdate: (stats: MistakeStats | null) => void;
  onBack: () => void;
}

export default function MistakePuzzleBoard({
  puzzles,
  config,
  onStatsUpdate,
  onBack,
}: MistakePuzzleBoardProps) {
  const { t } = useTranslation();
  const [currentIndex, setCurrentIndex] = useState(0);
  const [mode, setMode] = useState<PuzzleMode>("find_correct");
  const [puzzleState, setPuzzleState] = useState<PuzzleState>("solving");
  const [hintSquare, setHintSquare] = useState<string | null>(null);
  const [pendingMove, setPendingMove] = useState<NormalMove | null>(null);
  const [userMove, setUserMove] = useState<string | null>(null);

  const filteredPuzzles = useMemo(() => {
    return puzzles.filter((p) => config.annotationFilter.includes(p.annotation));
  }, [puzzles, config.annotationFilter]);

  const puzzle = filteredPuzzles[currentIndex] ?? null;

  // For "find_correct" mode: the puzzle FEN is the position before the mistake.
  // The user needs to find best_move.
  // For "punish_mistake" mode: we need the position AFTER the player's bad move.
  // The user plays as the opponent and finds opponent_punishment.
  const puzzleFen = useMemo(() => {
    if (!puzzle) return null;
    if (mode === "find_correct") {
      return puzzle.fen;
    }
    // Apply the played_move to get the position after the mistake
    const [pos] = positionFromFen(puzzle.fen);
    if (!pos) return puzzle.fen;
    const move = parseUci(puzzle.playedMove);
    if (!move) return puzzle.fen;
    pos.play(move);
    return makeFen(pos.toSetup());
  }, [puzzle, mode]);

  const expectedMove = useMemo(() => {
    if (!puzzle) return null;
    return mode === "find_correct" ? puzzle.bestMove : puzzle.opponentPunishment;
  }, [puzzle, mode]);

  const [pos] = puzzleFen ? positionFromFen(puzzleFen) : [null];

  // Determine orientation
  const orientation = useMemo(() => {
    if (!puzzle) return "white" as const;
    if (mode === "find_correct") {
      return puzzle.playerColor === "white" ? ("white" as const) : ("black" as const);
    }
    // Punish mode: opponent's perspective
    return puzzle.playerColor === "white" ? ("black" as const) : ("white" as const);
  }, [puzzle, mode]);

  const dests = pos ? chessgroundDests(pos) : new Map();
  const turn = pos?.turn || "white";

  function resetPuzzleState() {
    setPuzzleState("solving");
    setHintSquare(null);
    setUserMove(null);
    setPendingMove(null);
  }

  function goToNextPuzzle() {
    if (currentIndex < filteredPuzzles.length - 1) {
      setCurrentIndex((i: number) => i + 1);
      resetPuzzleState();
    }
  }

  function goToPreviousPuzzle() {
    if (currentIndex > 0) {
      setCurrentIndex((i: number) => i - 1);
      resetPuzzleState();
    }
  }

  async function updateCompletion(correct: boolean) {
    if (!puzzle) return;
    const completed = correct ? 1 : 2;
    await commands.updateMistakePuzzleCompletion(
      config.mistakeDbPath,
      puzzle.id,
      completed,
    );
    // Refresh stats
    const statsResult = await commands.getMistakeStats(
      config.mistakeDbPath,
      config.username,
      config.source,
    );
    if (statsResult.status === "ok") {
      onStatsUpdate(statsResult.data);
    }
  }

  async function checkMove(move: Move) {
    if (!pos || !expectedMove || puzzleState !== "solving") return;

    const uci = makeUci(move);
    setUserMove(uci);

    if (uci === expectedMove) {
      setPuzzleState("correct");
      if (puzzle?.completed === 0) {
        await updateCompletion(true);
      }
    } else {
      setPuzzleState("incorrect");
      if (puzzle?.completed === 0) {
        await updateCompletion(false);
      }
    }
  }

  function showHint() {
    if (!expectedMove) return;
    const from = expectedMove.substring(0, 2);
    setHintSquare(from);
  }

  function revealSolution() {
    setPuzzleState("revealed");
  }

  // Annotation badge color
  function getAnnotationColor(annotation: string) {
    switch (annotation) {
      case "??":
        return "red";
      case "?":
        return "orange";
      case "?!":
        return "yellow";
      case "miss":
        return "cyan";
      default:
        return "gray";
    }
  }

  function getAnnotationLabel(annotation: string) {
    switch (annotation) {
      case "??":
        return t("Annotate.Blunder");
      case "?":
        return t("Annotate.Mistake");
      case "?!":
        return t("Annotate.Dubious");
      case "miss":
        return t("LearnFromMistakes.Miss", { defaultValue: "Missed Opportunity" });
      default:
        return annotation;
    }
  }

  function getMissTypeBadge(puzzle: MistakePuzzle | undefined) {
    if (!puzzle || !puzzle.isMiss) return null;
    const missTypeLabel = puzzle.missType === "MATE_MISSED"
      ? t("LearnFromMistakes.MateMissed", { defaultValue: "Missed Forced Mate" })
      : puzzle.missType === "WINNING_OPPORTUNITY_MISSED"
        ? t("LearnFromMistakes.WinningMissed", { defaultValue: "Missed Winning Move" })
        : "";
    if (!missTypeLabel) return null;
    return (
      <Badge color="cyan" variant="light" size="sm">
        {missTypeLabel}
        {puzzle.mateIn > 0 ? ` (M${puzzle.mateIn})` : ""}
      </Badge>
    );
  }

  const { ref: parentRef, height: parentHeight } = useElementSize();

  if (filteredPuzzles.length === 0) {
    return (
      <Card withBorder shadow="sm" radius="md" p="lg">
        <Stack align="center" gap="md">
          <Text size="lg" fw={600}>
            {t("LearnFromMistakes.NoPuzzles")}
          </Text>
          <Text c="dimmed">{t("LearnFromMistakes.NoPuzzlesDesc")}</Text>
          <Button onClick={onBack}>{t("LearnFromMistakes.BackToSetup")}</Button>
        </Stack>
      </Card>
    );
  }

  // Build auto shapes for hints and solution display
  const autoShapes: any[] = [];
  if (hintSquare && puzzleState === "solving") {
    autoShapes.push({
      orig: hintSquare,
      brush: "green",
    });
  }
  if ((puzzleState === "revealed" || puzzleState === "incorrect") && expectedMove) {
    const from = expectedMove.substring(0, 2);
    const to = expectedMove.substring(2, 4);
    autoShapes.push({
      orig: from,
      dest: to,
      brush: "green",
    });
  }
  if (puzzleState === "incorrect" && userMove) {
    const from = userMove.substring(0, 2);
    const to = userMove.substring(2, 4);
    autoShapes.push({
      orig: from,
      dest: to,
      brush: "red",
    });
  }

  return (
    <Stack gap="md" style={{ flex: 1, minHeight: 0 }}>
      {/* Mode toggle */}
      <Group justify="space-between">
        <SegmentedControl
          value={mode}
          onChange={(v: string) => {
            setMode(v as PuzzleMode);
            resetPuzzleState();
          }}
          data={[
            {
              label: t("LearnFromMistakes.FindCorrect"),
              value: "find_correct",
            },
            {
              label: t("LearnFromMistakes.PunishMistake"),
              value: "punish_mistake",
            },
          ]}
        />
        <Button variant="subtle" onClick={onBack}>
          {t("LearnFromMistakes.BackToSetup")}
        </Button>
      </Group>

      <Group align="flex-start" gap="md" style={{ flex: 1, minHeight: 0 }}>
        {/* Board */}
        <Box
          w="100%"
          style={{ flex: 1, minHeight: 0, maxWidth: 600 }}
          ref={parentRef}
        >
          <Box
            className={classes.chessboard}
            style={{ maxWidth: parentHeight }}
          >
            <PromotionModal
              pendingMove={pendingMove}
              cancelMove={() => setPendingMove(null)}
              confirmMove={async (p: any) => {
                if (pendingMove) {
                  await checkMove({ ...pendingMove, promotion: p });
                  setPendingMove(null);
                }
              }}
              turn={turn}
              orientation={orientation}
            />
            <Chessground
              animation={{ enabled: true }}
              coordinates={true}
              orientation={orientation}
              drawable={{
                enabled: true,
                visible: true,
                autoShapes,
              }}
              movable={{
                free: false,
                color: puzzleState === "solving" ? turn : undefined,
                dests: puzzleState === "solving" ? dests : new Map(),
                events: {
                  after: (orig: string, dest: string) => {
                    const from = parseSquare(orig)!;
                    const to = parseSquare(dest)!;
                    const move: NormalMove = { from, to };
                    if (
                      pos?.board.get(from)?.role === "pawn" &&
                      ((dest[1] === "8" && turn === "white") ||
                        (dest[1] === "1" && turn === "black"))
                    ) {
                      setPendingMove(move);
                    } else {
                      checkMove(move);
                    }
                  },
                },
              }}
              turnColor={turn}
              fen={puzzleFen || ""}
              check={pos?.isCheck()}
            />
          </Box>
        </Box>

        {/* Info panel */}
        <Card withBorder shadow="sm" radius="md" p="md" w={320}>
          <Stack gap="sm">
            {/* Puzzle info */}
            <Group justify="space-between">
              <Text size="sm" fw={600}>
                {t("LearnFromMistakes.PuzzleN", {
                  n: currentIndex + 1,
                  total: filteredPuzzles.length,
                })}
              </Text>
              <Badge color={getAnnotationColor(puzzle?.annotation || "")}>
                {getAnnotationLabel(puzzle?.annotation || "")} ({puzzle?.annotation})
              </Badge>
            </Group>

            {/* Miss type badge */}
            {getMissTypeBadge(puzzle)}

            {/* Classification badge */}
            {puzzle?.moveClassification && (
              <Badge
                color={
                  puzzle.moveClassification === "MISS" ? "cyan" :
                  puzzle.moveClassification === "BLUNDER" ? "red" :
                  puzzle.moveClassification === "MISTAKE" ? "orange" :
                  puzzle.moveClassification === "INACCURACY" ? "yellow" :
                  "gray"
                }
                variant="outline"
                size="sm"
              >
                {puzzle.moveClassification} (Δ{puzzle.evalDelta}cp)
              </Badge>
            )}

            {/* Move info */}
            {puzzle && (
              <>
                <Text size="xs" c="dimmed">
                  {t("LearnFromMistakes.MoveNumber")}: {puzzle.moveNumber}
                </Text>
                <Text size="xs" c="dimmed">
                  {t("LearnFromMistakes.EvalBefore")}: {puzzle.evalBefore} →{" "}
                  {t("LearnFromMistakes.EvalAfter")}: {puzzle.evalAfter}
                </Text>
                <Text size="xs" c="dimmed">
                  {t("LearnFromMistakes.CPLoss")}: {puzzle.cpLoss} cp (
                  {puzzle.winChanceDrop.toFixed(1)}%)
                </Text>
              </>
            )}

            {/* Status */}
            {puzzleState === "correct" && (
              <Badge color="green" size="lg" fullWidth>
                {t("LearnFromMistakes.Correct")}
              </Badge>
            )}
            {puzzleState === "incorrect" && (
              <Badge color="red" size="lg" fullWidth>
                {t("LearnFromMistakes.Incorrect")}
              </Badge>
            )}
            {puzzleState === "revealed" && (
              <Badge color="blue" size="lg" fullWidth>
                {t("LearnFromMistakes.SolutionRevealed")}
              </Badge>
            )}

            {/* What you played vs what was best */}
            {(puzzleState === "incorrect" || puzzleState === "revealed") && puzzle && (
              <Card withBorder p="xs" bg="dark.6">
                <Stack gap={4}>
                  {mode === "find_correct" && (
                    <Text size="xs" c="red">
                      {t("LearnFromMistakes.YouPlayed")}: {puzzle.playedMove}
                    </Text>
                  )}
                  <Text size="xs" c="green">
                    {t("LearnFromMistakes.BestMove")}: {expectedMove}
                  </Text>
                </Stack>
              </Card>
            )}

            {/* Actions */}
            <Group gap="xs" grow>
              <Tooltip label={t("LearnFromMistakes.ShowHint")}>
                <ActionIcon
                  variant="light"
                  size="lg"
                  onClick={showHint}
                  disabled={puzzleState !== "solving"}
                >
                  <IconBulb size={18} />
                </ActionIcon>
              </Tooltip>
              <Tooltip label={t("LearnFromMistakes.ShowSolution")}>
                <ActionIcon
                  variant="light"
                  size="lg"
                  onClick={revealSolution}
                  disabled={puzzleState === "correct" || puzzleState === "revealed"}
                >
                  <IconEye size={18} />
                </ActionIcon>
              </Tooltip>
              <Tooltip label={t("LearnFromMistakes.RetryPuzzle")}>
                <ActionIcon variant="light" size="lg" onClick={resetPuzzleState}>
                  <IconRotate size={18} />
                </ActionIcon>
              </Tooltip>
            </Group>

            {/* Navigation */}
            <Group gap="xs" grow>
              <Button
                variant="light"
                leftSection={<IconArrowLeft size={16} />}
                onClick={goToPreviousPuzzle}
                disabled={currentIndex === 0}
              >
                {t("LearnFromMistakes.PreviousPuzzle")}
              </Button>
              <Button
                variant="light"
                rightSection={<IconArrowRight size={16} />}
                onClick={goToNextPuzzle}
                disabled={currentIndex >= filteredPuzzles.length - 1}
              >
                {t("LearnFromMistakes.NextPuzzle")}
              </Button>
            </Group>
          </Stack>
        </Card>
      </Group>
    </Stack>
  );
}
