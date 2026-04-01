"use no memo";
import { useEffect, useState, useRef } from "react";
import { Button, Card, Group, Progress, Stack, Text, Badge } from "@mantine/core";
import { IconPlayerStop } from "@tabler/icons-react";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import { useNavigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { resolve } from "@tauri-apps/api/path";
import { commands, events } from "@/bindings";
import type { MistakeStats, GoMode } from "@/bindings";
import {
  mistakeAnalysisConfigAtom,
  mistakeAnalysisIdAtom,
  mistakeAnalysisStartedAtom,
  mistakeAnalysisStartTimeAtom,
  activeTabAtom,
  tabsAtom,
  selectedPuzzleDbAtom,
} from "@/state/atoms";
import { getPuzzlesDir } from "@/utils/directories";

interface AnalysisProgressProps {
  onComplete: (stats: MistakeStats) => void;
  onCancel: () => void;
}

export default function AnalysisProgress({
  onComplete,
  onCancel,
}: AnalysisProgressProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const config = useAtomValue(mistakeAnalysisConfigAtom)!;
  const analysisId = useAtomValue(mistakeAnalysisIdAtom);
  const [started, setStarted] = useAtom(mistakeAnalysisStartedAtom);
  const startTime = useAtomValue(mistakeAnalysisStartTimeAtom);
  const [, setTabs] = useAtom(tabsAtom);
  const activeTab = useAtomValue(activeTabAtom);
  const setSelectedPuzzleDb = useSetAtom(selectedPuzzleDbAtom);

  const [progress, setProgress] = useState(0);
  const [finished, setFinished] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const completedRef = useRef(false);

  // Timer: tick every second
  useEffect(() => {
    if (finished) return;
    const interval = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTime) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [startTime, finished]);

  // On mount: check if analysis is already running (reconnect)
  useEffect(() => {
    if (started) {
      commands.getProgress(analysisId).then((item) => {
        if (item) {
          setProgress(item.progress);
          if (item.finished) {
            setFinished(true);
          }
        }
      });
    }
  }, []);

  // Listen for progress events (always active while mounted)
  useEffect(() => {
    const unlisten = events.progressEvent.listen((event: any) => {
      if (event.payload.id === analysisId) {
        setProgress(event.payload.progress);
        if (event.payload.finished) {
          setFinished(true);
        }
      }
    });
    return () => {
      unlisten.then((fn: () => void) => fn());
    };
  }, [analysisId]);

  // Navigate to Puzzles tab with the new puzzle DB selected
  async function navigateToPuzzles(puzzleDbPath: string) {
    setSelectedPuzzleDb(puzzleDbPath);
    setTabs((prev) => {
      if (activeTab) {
        const tab = prev.find((tab) => tab.value === activeTab);
        if (tab) {
          tab.name = t("Home.PuzzleTraining", { defaultValue: "Puzzle Training" });
          tab.type = "puzzles";
          return [...prev];
        }
      }
      return prev;
    });
    navigate({ to: "/" });
  }

  // Start analysis (only if not already started)
  useEffect(() => {
    if (started) return;
    setStarted(true);

    const goMode: GoMode = { t: "Depth", c: config.depth };

    commands
      .analyzeGamesForMistakes({
        id: analysisId,
        engine: config.enginePath,
        goMode,
        uciOptions: config.uciOptions,
        dbPath: config.dbPath,
        mistakeDbPath: config.mistakeDbPath,
        username: config.username,
        source: config.source,
        minWinChanceDrop: config.minWinChanceDrop,
      })
      .then(async () => {
        if (completedRef.current) return;
        completedRef.current = true;

        // Get stats
        const statsResult = await commands.getMistakeStats(
          config.mistakeDbPath,
          config.username,
          config.source,
        );

        // Export mistakes to a standard puzzle DB in the puzzles directory
        const puzzlesDir = await getPuzzlesDir();
        const puzzleDbPath = await resolve(
          puzzlesDir,
          `my_mistakes_${config.username}_${config.source.replace(".", "")}.db3`,
        );

        const exportResult = await commands.exportMistakesToPuzzleDb(
          config.mistakeDbPath,
          puzzleDbPath,
          config.username,
          config.source,
        );

        if (statsResult.status === "ok") {
          onComplete(statsResult.data);
        }

        if (exportResult.status === "ok" && exportResult.data > 0) {
          await navigateToPuzzles(puzzleDbPath);
        } else {
          setFinished(true);
        }
      })
      .catch(() => {
        setFinished(true);
      });
  }, []);

  async function handleCancel() {
    setCancelling(true);
    await commands.cancelAnalysis(analysisId);
    setTimeout(() => onCancel(), 1000);
  }

  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;

  return (
    <Card withBorder shadow="sm" radius="md" p="lg">
      <Stack gap="md">
        <Group justify="space-between">
          <Text fw={600} size="lg">
            {t("LearnFromMistakes.Analyzing")}
          </Text>
          <Badge color="blue" variant="light">
            {config.engineName}
          </Badge>
        </Group>

        <Progress
          value={progress}
          size="xl"
          radius="md"
          animated={!finished}
          color={finished ? "green" : "blue"}
        />

        <Group justify="space-between">
          <Text size="sm" c="dimmed">
            {t("LearnFromMistakes.Progress", { progress: progress.toFixed(1) })}
          </Text>
          <Text size="sm" c="dimmed">
            {t("LearnFromMistakes.AnalyzingUser", { username: config.username, source: config.source })}
          </Text>
        </Group>

        <Group justify="space-between">
          <Text size="sm" c="dimmed">
            {t("LearnFromMistakes.Depth")}: {config.depth}
          </Text>
          <Text size="sm" c="dimmed">
            {minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`}
          </Text>
        </Group>

        {!finished && (
          <Button
            color="red"
            variant="light"
            leftSection={<IconPlayerStop size={16} />}
            onClick={handleCancel}
            loading={cancelling}
          >
            {t("LearnFromMistakes.Cancel")}
          </Button>
        )}
      </Stack>
    </Card>
  );
}
