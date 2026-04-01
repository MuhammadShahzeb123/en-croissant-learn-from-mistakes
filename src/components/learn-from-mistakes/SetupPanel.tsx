"use no memo";
import { useState } from "react";
import {
  Button,
  Card,
  Checkbox,
  Group,
  NumberInput,
  Select,
  Slider,
  Stack,
  Text,
  Alert,
  Loader,
} from "@mantine/core";
import { IconAlertCircle } from "@tabler/icons-react";
import { useAtomValue } from "jotai";
import { useTranslation } from "react-i18next";
import { appDataDir, resolve } from "@tauri-apps/api/path";
import { exists, mkdir } from "@tauri-apps/plugin-fs";
import { commands } from "@/bindings";
import type { MistakePuzzle, MistakeStats, MistakePuzzleFilter } from "@/bindings";
import { sessionsAtom, enginesAtom } from "@/state/atoms";
import type { AnalysisConfig } from "./LearnFromMistakes";
import { unwrap } from "@/utils/unwrap";
import { getDatabasesDir } from "@/utils/directories";
import type { Engine, LocalEngine } from "@/utils/engines";

interface SetupPanelProps {
  onStart: (config: AnalysisConfig, analysisId: string) => void;
  onResume: (config: AnalysisConfig, puzzles: MistakePuzzle[], stats: MistakeStats) => void;
}

export default function SetupPanel({ onStart, onResume }: SetupPanelProps) {
  const { t } = useTranslation();
  const sessions = useAtomValue(sessionsAtom);
  const engines = useAtomValue(enginesAtom);

  const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
  const [selectedEngine, setSelectedEngine] = useState<string | null>(null);
  const [depth, setDepth] = useState<number>(18);
  const [annotations, setAnnotations] = useState<string[]>(["??", "?", "?!"]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Build account options from sessions
  const accountOptions: { value: string; label: string }[] = [];
  for (const session of sessions || []) {
    if (session.lichess) {
      accountOptions.push({
        value: `lichess:${session.lichess.username}`,
        label: `${session.lichess.username} (Lichess)`,
      });
    }
    if (session.chessCom) {
      accountOptions.push({
        value: `chess.com:${session.chessCom.username}`,
        label: `${session.chessCom.username} (Chess.com)`,
      });
    }
    if (session.player) {
      accountOptions.push({
        value: `local:${session.player}`,
        label: `${session.player} (Local)`,
      });
    }
  }

  // Build engine options (local engines only)
  const localEngines = ((engines || []) as Engine[]).filter(
    (e: Engine): e is LocalEngine => e.type === "local",
  );
  const engineOptions = localEngines.map((e: LocalEngine) => ({
    value: e.path,
    label: `${e.name} ${e.version || ""}`.trim(),
  }));

  // Min win chance drop mapping
  const minWinChanceDrop = annotations.includes("?!") ? 5 : annotations.includes("?") ? 10 : 20;

  async function getMistakeDbPath(): Promise<string> {
    const dataDir = await appDataDir();
    const mistakeDir = await resolve(dataDir, "mistakes");
    if (!(await exists(mistakeDir))) {
      await mkdir(mistakeDir, { recursive: true });
    }
    return resolve(mistakeDir, "mistake_puzzles.db3");
  }

  async function handleStart() {
    if (!selectedAccount || !selectedEngine) return;

    setLoading(true);
    setError(null);

    try {
      const [source, username] = selectedAccount.split(/:(.+)/);
      const engine = localEngines.find((e: LocalEngine) => e.path === selectedEngine);
      if (!engine) throw new Error("Engine not found");

      // Find the database path for this user's games
      const dbDir = await getDatabasesDir();
      const dbPath = await resolve(
        dbDir,
        `${username}_${source === "chess.com" ? "chesscom" : source}.db3`,
      );
      const mistakeDbPath = await getMistakeDbPath();

      // Initialize mistake DB
      const initResult = await commands.initMistakeDb(mistakeDbPath);
      if (initResult.status === "error") throw new Error(initResult.error);

      // Check if we already have puzzles for this user
      const existingResult = await commands.getMistakePuzzles(mistakeDbPath, {
        username,
        source: source as "lichess" | "chess.com",
        annotation: null,
        completed: null,
        limit: 1,
        offset: null,
      });

      if (existingResult.status === "ok" && existingResult.data.length > 0) {
        // We have existing puzzles — ask to resume or re-analyze
        const allPuzzlesResult = await commands.getMistakePuzzles(mistakeDbPath, {
          username,
          source: source as "lichess" | "chess.com",
          annotation: null,
          completed: null,
          limit: null,
          offset: null,
        });
        const statsResult = await commands.getMistakeStats(
          mistakeDbPath,
          username,
          source as "lichess" | "chess.com",
        );

        if (allPuzzlesResult.status === "ok" && statsResult.status === "ok") {
          const config: AnalysisConfig = {
            username,
            source: source as "lichess" | "chess.com",
            enginePath: engine.path,
            engineName: engine.name,
            depth,
            dbPath,
            mistakeDbPath,
            minWinChanceDrop,
            annotationFilter: annotations,
          };
          onResume(config, allPuzzlesResult.data, statsResult.data);
          return;
        }
      }

      // Check if database exists
      if (!(await exists(dbPath))) {
        throw new Error(
          t("LearnFromMistakes.NoDatabaseFound", {
            defaultValue: `No games database found for ${username}. Please import your games first from the Databases page.`,
          }),
        );
      }

      const config: AnalysisConfig = {
        username,
        source: source as "lichess" | "chess.com",
        enginePath: engine.path,
        engineName: engine.name,
        depth,
        dbPath,
        mistakeDbPath,
        minWinChanceDrop,
        annotationFilter: annotations,
      };

      const analysisId = `mistake-analysis-${username}-${Date.now()}`;
      onStart(config, analysisId);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  const noAccounts = accountOptions.length === 0;
  const noEngines = engineOptions.length === 0;

  return (
    <Card withBorder shadow="sm" radius="md" p="lg">
      <Stack gap="md">
        {noAccounts && (
          <Alert icon={<IconAlertCircle size={16} />} color="yellow" title={t("LearnFromMistakes.NoAccounts")}>
            {t("LearnFromMistakes.NoAccountsDesc")}
          </Alert>
        )}

        {noEngines && (
          <Alert icon={<IconAlertCircle size={16} />} color="yellow" title={t("LearnFromMistakes.NoEngines")}>
            {t("LearnFromMistakes.NoEnginesDesc")}
          </Alert>
        )}

        <Select
          label={t("LearnFromMistakes.SelectAccount")}
          placeholder={t("LearnFromMistakes.SelectAccountPlaceholder")}
          data={accountOptions}
          value={selectedAccount}
          onChange={setSelectedAccount}
          disabled={noAccounts}
        />

        <Select
          label={t("LearnFromMistakes.SelectEngine")}
          placeholder={t("LearnFromMistakes.SelectEnginePlaceholder")}
          data={engineOptions}
          value={selectedEngine}
          onChange={setSelectedEngine}
          disabled={noEngines}
        />

        <div>
          <Text size="sm" fw={500} mb={4}>
            {t("LearnFromMistakes.AnalysisDepth")} — {depth}
          </Text>
          <Slider
            min={8}
            max={30}
            step={1}
            value={depth}
            onChange={setDepth}
            marks={[
              { value: 10, label: "10" },
              { value: 15, label: "15" },
              { value: 20, label: "20" },
              { value: 25, label: "25" },
              { value: 30, label: "30" },
            ]}
          />
        </div>

        <Checkbox.Group
          label={t("LearnFromMistakes.MistakeTypes")}
          value={annotations}
          onChange={setAnnotations}
        >
          <Group mt="xs">
            <Checkbox value="??" label={`${t("Annotate.Blunder")} (??)`} />
            <Checkbox value="?" label={`${t("Annotate.Mistake")} (?)`} />
            <Checkbox value="?!" label={`${t("Annotate.Dubious")} (?!)`} />
          </Group>
        </Checkbox.Group>

        {error && (
          <Alert icon={<IconAlertCircle size={16} />} color="red" title="Error">
            {error}
          </Alert>
        )}

        <Button
          size="md"
          onClick={handleStart}
          loading={loading}
          disabled={!selectedAccount || !selectedEngine || annotations.length === 0}
        >
          {t("LearnFromMistakes.StartAnalysis")}
        </Button>
      </Stack>
    </Card>
  );
}
