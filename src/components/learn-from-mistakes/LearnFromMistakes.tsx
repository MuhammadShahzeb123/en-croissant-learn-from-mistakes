"use no memo";
import { useState } from "react";
import { Container, Title, Text, Stack } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { MistakePuzzle, MistakeStats } from "@/bindings";
import SetupPanel from "./SetupPanel";
import AnalysisProgress from "./AnalysisProgress";
import MistakePuzzleBoard from "./MistakePuzzleBoard";
import StatsPanel from "./StatsPanel";

export type LearnView = "setup" | "analyzing" | "puzzles";

export interface AnalysisConfig {
  username: string;
  source: "lichess" | "chess.com";
  enginePath: string;
  engineName: string;
  depth: number;
  dbPath: string;
  mistakeDbPath: string;
  minWinChanceDrop: number;
  annotationFilter: string[];
}

export default function LearnFromMistakes() {
  const { t } = useTranslation();
  const [view, setView] = useState<LearnView>("setup");
  const [config, setConfig] = useState<AnalysisConfig | null>(null);
  const [puzzles, setPuzzles] = useState<MistakePuzzle[]>([]);
  const [stats, setStats] = useState<MistakeStats | null>(null);
  const [analysisId, setAnalysisId] = useState<string>("");

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
              setView("analyzing");
            }}
            onResume={(cfg: AnalysisConfig, puzzleList: MistakePuzzle[], statsData: MistakeStats) => {
              setConfig(cfg);
              setPuzzles(puzzleList);
              setStats(statsData);
              setView("puzzles");
            }}
          />
        )}

        {view === "analyzing" && config && (
          <AnalysisProgress
            config={config}
            analysisId={analysisId}
            onComplete={(puzzleList: MistakePuzzle[], statsData: MistakeStats) => {
              setPuzzles(puzzleList);
              setStats(statsData);
              setView("puzzles");
            }}
            onCancel={() => setView("setup")}
          />
        )}

        {view === "puzzles" && config && (
          <>
            <StatsPanel stats={stats} />
            <MistakePuzzleBoard
              puzzles={puzzles}
              config={config}
              onStatsUpdate={setStats}
              onBack={() => setView("setup")}
            />
          </>
        )}
      </Stack>
    </Container>
  );
}
