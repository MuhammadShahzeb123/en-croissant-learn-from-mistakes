"use no memo";
import { useEffect, useState, useRef } from "react";
import { Button, Card, Group, Progress, Stack, Text, Badge } from "@mantine/core";
import { IconPlayerStop } from "@tabler/icons-react";
import { useTranslation } from "react-i18next";
import { commands, events } from "@/bindings";
import type { MistakePuzzle, MistakeStats, GoMode, EngineOption } from "@/bindings";
import type { AnalysisConfig } from "./LearnFromMistakes";
import { unwrap } from "@/utils/unwrap";

interface AnalysisProgressProps {
  config: AnalysisConfig;
  analysisId: string;
  onComplete: (puzzles: MistakePuzzle[], stats: MistakeStats) => void;
  onCancel: () => void;
}

export default function AnalysisProgress({
  config,
  analysisId,
  onComplete,
  onCancel,
}: AnalysisProgressProps) {
  const { t } = useTranslation();
  const [progress, setProgress] = useState(0);
  const [finished, setFinished] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const startedRef = useRef(false);
  const startTimeRef = useRef(Date.now());

  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    startTimeRef.current = Date.now();

    // Listen for progress events
    const unlisten = events.progressEvent.listen((event: any) => {
      if (event.payload.id === analysisId) {
        setProgress(event.payload.progress);
        if (event.payload.finished) {
          setFinished(true);
        }
      }
    });

    // Start analysis
    const goMode: GoMode = { t: "Depth", c: config.depth };
    const uciOptions: EngineOption[] = [];

    commands
      .analyzeGamesForMistakes({
        id: analysisId,
        engine: config.enginePath,
        goMode,
        uciOptions,
        dbPath: config.dbPath,
        mistakeDbPath: config.mistakeDbPath,
        username: config.username,
        source: config.source,
        minWinChanceDrop: config.minWinChanceDrop,
      })
      .then(async (result) => {
        // Analysis done — fetch puzzles and stats
        const puzzlesResult = await commands.getMistakePuzzles(config.mistakeDbPath, {
          username: config.username,
          source: config.source,
          annotation: null,
          completed: null,
          limit: null,
          offset: null,
        });

        const statsResult = await commands.getMistakeStats(
          config.mistakeDbPath,
          config.username,
          config.source,
        );

        if (puzzlesResult.status === "ok" && statsResult.status === "ok") {
          onComplete(puzzlesResult.data, statsResult.data);
        } else {
          setFinished(true);
        }
      })
      .catch(() => {
        setFinished(true);
      });

    return () => {
      unlisten.then((fn: () => void) => fn());
    };
  }, []);

  async function handleCancel() {
    setCancelling(true);
    await commands.cancelAnalysis(analysisId);
    // Give time for cancellation
    setTimeout(() => onCancel(), 1000);
  }

  const elapsed = Math.floor((Date.now() - startTimeRef.current) / 1000);
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
