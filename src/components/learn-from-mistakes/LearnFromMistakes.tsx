"use no memo";
import { Container, Title, Text, Stack } from "@mantine/core";
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

        {stats && stats.total > 0 && view === "setup" && (
          <StatsPanel stats={stats} />
        )}
      </Stack>
    </Container>
  );
}
