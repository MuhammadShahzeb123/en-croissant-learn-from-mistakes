"use no memo";
import { Container, Title, Text, Stack, Button } from "@mantine/core";
import MistakePuzzleBoard from "./MistakePuzzleBoard";
import { commands } from "@/bindings";
import { useAtom, useSetAtom } from "jotai";
import { useTranslation } from "react-i18next";
import type { MistakeStats } from "@/bindings";
import {
  mistakeAnalysisViewAtom,
  mistakeAnalysisConfigAtom,
  mistakeAnalysisIdAtom,
  mistakeAnalysisStartedAtom,
  mistakeAnalysisStartTimeAtom,
  mistakeStatsAtom,
  mistakePuzzlesAtom, // Add this
  type AnalysisConfig,
} from "@/state/atoms";
import SetupPanel from "./SetupPanel";
import AnalysisProgress from "./AnalysisProgress";
import StatsPanel from "./StatsPanel";


export default function LearnFromMistakes() {
  const { t } = useTranslation();
  const [view, setView] = useAtom(mistakeAnalysisViewAtom);
  const [config, setConfig] = useAtom(mistakeAnalysisConfigAtom);
  const [stats, setStats] = useAtom(mistakeStatsAtom);
  const setAnalysisId = useSetAtom(mistakeAnalysisIdAtom);
  const setStarted = useSetAtom(mistakeAnalysisStartedAtom);
  const setStartTime = useSetAtom(mistakeAnalysisStartTimeAtom);
  const [puzzles, setPuzzles] = useAtom(mistakePuzzlesAtom); // Use atom instead of local state

  // Load puzzles and switch view
  const startPuzzles = async () => {
    if (!config) return;

    const result = await commands.getMistakePuzzles(config.mistakeDbPath, {
      username: config.username,
      source: config.source,
      annotation: null,
      completed: null,
      limit: null,
      offset: null
    });
    if (result.status === "ok") {
      setPuzzles(result.data);
      setView("puzzles");
    }
  };

  return (
    <Container size="xl" py="lg" h="100%">
      <Stack gap="md" h="100%">
        <Title order={2}>{t("LearnFromMistakes.Title")}</Title>
        <Text c="dimmed" size="sm">
          {t("LearnFromMistakes.Description")}
        </Text>

        {view === "setup" && (
          <SetupPanel
            onStart={(cfg: AnalysisConfig, id: string) => {
              setConfig(cfg);
              setAnalysisId(id);
              setStarted(false);
              setStartTime(Date.now());
              setView("analyzing");
            }}
          />
        )}

        {view === "analyzing" && config && (
          <AnalysisProgress
            onComplete={(statsData: MistakeStats) => {
              setStats(statsData);
              setStarted(false);
              setView("setup");
            }}
            onCancel={() => {
              setStarted(false);
              setView("setup");
            }}
          />
        )}

        {stats && view === "setup" && (
          <Stack gap="md">
            <StatsPanel stats={stats} />
            {Number(stats.total) > 0 && (
              <Button
                variant="filled"
                size="md"
                onClick={startPuzzles}
                fullWidth
              >
                {t("LearnFromMistakes.StartPuzzles")}
              </Button>
            )}
          </Stack>
        )}

        {view === "puzzles" && puzzles && config && (
          <MistakePuzzleBoard
            puzzles={puzzles}
            config={config}
            onStatsUpdate={setStats}
            onBack={() => setView("setup")}
          />
        )}
      </Stack>
    </Container>
  );
}
