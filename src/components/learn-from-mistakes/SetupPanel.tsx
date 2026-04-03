"use no memo";
import { useState, useEffect } from "react";
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
  Tooltip,
} from "@mantine/core";
import { IconAlertCircle, IconInfoCircle, IconCloud } from "@tabler/icons-react";
import { useAtomValue } from "jotai";
import { useTranslation } from "react-i18next";
import { appDataDir, resolve } from "@tauri-apps/api/path";
import { exists, mkdir } from "@tauri-apps/plugin-fs";
import { commands } from "@/bindings";
import type { EngineOption } from "@/bindings";
import { sessionsAtom, enginesAtom, type AnalysisConfig } from "@/state/atoms";

import { getDatabasesDir } from "@/utils/directories";
import type { Engine, LocalEngine } from "@/utils/engines";

const LICHESS_CLOUD_VALUE = "__lichess_cloud__";
const HYBRID_VALUE = "__hybrid__";

interface SetupPanelProps {
  onStart: (config: AnalysisConfig, analysisId: string) => void;
}

export default function SetupPanel({ onStart }: SetupPanelProps) {
  const { t } = useTranslation();
  const sessions = useAtomValue(sessionsAtom);
  const engines = useAtomValue(enginesAtom);

  const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
  const [selectedEngine, setSelectedEngine] = useState<string | null>(HYBRID_VALUE);
  const [selectedHybridEngine, setSelectedHybridEngine] = useState<string | null>(null);
  const [depth, setDepth] = useState<number>(10);
  const [threads, setThreads] = useState<number>(6);
  const [hash, setHash] = useState<number>(256);
  const [annotations, setAnnotations] = useState<string[]>(["??", "?", "?!", "miss"]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isCloudEngine = selectedEngine === LICHESS_CLOUD_VALUE;
  const isHybridEngine = selectedEngine === HYBRID_VALUE;

  // Build engine options: local engines + Lichess cloud
  const localEngines = ((engines || []) as Engine[]).filter(
    (e: Engine): e is LocalEngine => e.type === "local",
  );
  const engineOptions = [
    {
      group: "Cloud",
      items: [{ value: LICHESS_CLOUD_VALUE, label: "Lichess Cloud Eval" }],
    },
    {
      group: "Hybrid",
      items: [{ value: HYBRID_VALUE, label: "Hybrid (Cloud + Local Engine)" }],
    },
    {
      group: "Local",
      items: localEngines.map((e: LocalEngine) => ({
        value: e.path,
        label: `${e.name} ${e.version || ""}`.trim(),
      })),
    },
  ];

  // When engine selection changes, load its stored settings as defaults
  const activeEnginePath = isHybridEngine ? selectedHybridEngine : selectedEngine;
  const selectedLocalEngine = localEngines.find((e: LocalEngine) => e.path === activeEnginePath);

  // Set the default hybrid fallback engine to the first local engine if not set
  useEffect(() => {
    if (isHybridEngine && !selectedHybridEngine && localEngines.length > 0) {
      setSelectedHybridEngine(localEngines[0].path);
    }
  }, [isHybridEngine, localEngines, selectedHybridEngine]);

  useEffect(() => {
    if (!selectedLocalEngine?.settings) return;
    for (const s of selectedLocalEngine.settings) {
      if (s.name === "Threads" && s.value != null) setThreads(Number(s.value));
      if (s.name === "Hash" && s.value != null) setHash(Number(s.value));
    }
  }, [activeEnginePath]);

  // Build account options from sessions (deduplicated by value)
  const accountOptionsMap = new Map<string, { value: string; label: string }>();
  for (const session of sessions || []) {
    if (session.lichess) {
      const v = `lichess:${session.lichess.username}`;
      if (!accountOptionsMap.has(v))
        accountOptionsMap.set(v, { value: v, label: `${session.lichess.username} (Lichess)` });
    }
    if (session.chessCom) {
      const v = `chess.com:${session.chessCom.username}`;
      if (!accountOptionsMap.has(v))
        accountOptionsMap.set(v, { value: v, label: `${session.chessCom.username} (Chess.com)` });
    }
    if (session.player) {
      const v = `local:${session.player}`;
      if (!accountOptionsMap.has(v))
        accountOptionsMap.set(v, { value: v, label: `${session.player} (Local)` });
    }
  }
  const accountOptions = Array.from(accountOptionsMap.values());

  // Build UCI options from current Threads/Hash values + any other engine settings
  function buildUciOptions(): EngineOption[] {
    const opts: EngineOption[] = [
      { name: "Threads", value: String(threads) },
      { name: "Hash", value: String(hash) },
    ];
    // Forward other stored engine settings (excluding ones we already handle)
    const skipNames = new Set(["Threads", "Hash", "MultiPV", "UCI_Chess960"]);
    if (selectedLocalEngine?.settings) {
      for (const s of selectedLocalEngine.settings) {
        if (!skipNames.has(s.name) && s.value != null) {
          opts.push({ name: s.name, value: String(s.value) });
        }
      }
    }
    return opts;
  }

  // Min win chance drop mapping
  const minWinChanceDrop = annotations.includes("?!") ? 5 : annotations.includes("?") ? 10 : 20;

  async function getMistakeDbPath(): Promise<string> {
    const dataDir = await appDataDir();
    const mistakeDir = await resolve(dataDir, "mistakes");
    if (!(await exists(mistakeDir))) {
      await mkdir(mistakeDir, { recursive: true });
    }
    return resolve(mistakeDir, "mistake_puzzles.pgn");
  }

  async function handleStart() {
    if (!selectedAccount || !selectedEngine) return;

    setLoading(true);
    setError(null);

    try {
      const [source, username] = selectedAccount.split(/:(.+)/);

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

      // Check if database exists
      if (!(await exists(dbPath))) {
        throw new Error(
          t("LearnFromMistakes.NoDatabaseFound", {
            defaultValue: `No games database found for ${username}. Please import your games first from the Databases page.`,
          }),
        );
      }

      if (isCloudEngine) {
        const config: AnalysisConfig = {
          username,
          source: source as "lichess" | "chess.com",
          enginePath: "",
          engineName: "Lichess Cloud",
          engineType: "lichess",
          depth: 0, // cloud determines depth
          dbPath,
          mistakeDbPath,
          minWinChanceDrop,
          annotationFilter: annotations,
          uciOptions: [],
        };
        const analysisId = `mistake-analysis-cloud-${username}-${Date.now()}`;
        onStart(config, analysisId);
      } else if (isHybridEngine) {
        const engine = localEngines.find((e: LocalEngine) => e.path === selectedHybridEngine);
        if (!engine) throw new Error("Please select a local engine for hybrid mode");

        const config: AnalysisConfig = {
          username,
          source: source as "lichess" | "chess.com",
          enginePath: engine.path,
          engineName: `Hybrid (${engine.name})`,
          engineType: "hybrid",
          depth,
          dbPath,
          mistakeDbPath,
          minWinChanceDrop,
          annotationFilter: annotations,
          uciOptions: buildUciOptions(),
        };
        const analysisId = `mistake-analysis-hybrid-${username}-${Date.now()}`;
        onStart(config, analysisId);
      } else {
        const engine = localEngines.find((e: LocalEngine) => e.path === selectedEngine);
        if (!engine) throw new Error("Engine not found");

        const config: AnalysisConfig = {
          username,
          source: source as "lichess" | "chess.com",
          enginePath: engine.path,
          engineName: engine.name,
          engineType: "local",
          depth,
          dbPath,
          mistakeDbPath,
          minWinChanceDrop,
          annotationFilter: annotations,
          uciOptions: buildUciOptions(),
        };
        const analysisId = `mistake-analysis-${username}-${Date.now()}`;
        onStart(config, analysisId);
      }
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  const noAccounts = accountOptions.length === 0;

  return (
    <Card withBorder shadow="sm" radius="md" p="lg">
      <Stack gap="md">
        {noAccounts && (
          <Alert icon={<IconAlertCircle size={16} />} color="yellow" title={t("LearnFromMistakes.NoAccounts")}>
            {t("LearnFromMistakes.NoAccountsDesc")}
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
        />

        {isCloudEngine && (
          <Alert icon={<IconCloud size={16} />} color="blue" variant="light">
            {t("LearnFromMistakes.CloudInfo", {
              defaultValue: "Lichess Cloud uses pre-computed evaluations from Lichess servers. Positions not in the cloud database will be skipped. No local engine needed — much faster but may miss some positions.",
            })}
          </Alert>
        )}

        {isHybridEngine && (
          <>
            <Alert icon={<IconCloud size={16} />} color="teal" variant="light">
              {t("LearnFromMistakes.HybridInfo", {
                defaultValue: "Hybrid mode tries Lichess Cloud Eval first (free & fast), then falls back to your local engine for positions not in the cloud. Best of both worlds — fast and thorough.",
              })}
            </Alert>

            <Select
              label={t("LearnFromMistakes.SelectFallbackEngine", { defaultValue: "Fallback Engine" })}
              description={t("LearnFromMistakes.FallbackEngineDesc", { defaultValue: "Local engine used when cloud eval is unavailable" })}
              placeholder={t("LearnFromMistakes.SelectEnginePlaceholder")}
              data={localEngines.map((e: LocalEngine) => ({
                value: e.path,
                label: `${e.name} ${e.version || ""}`.trim(),
              }))}
              value={selectedHybridEngine}
              onChange={setSelectedHybridEngine}
            />
          </>
        )}

        {!isCloudEngine && (
          <>
            <div>
              <Text size="sm" fw={500} mb={4}>
                {t("LearnFromMistakes.AnalysisDepth")} — {depth}
              </Text>
              <Slider
                min={8}
                max={20}
                step={1}
                value={depth}
                onChange={setDepth}
                marks={[
                  { value: 8, label: "8" },
                  { value: 10, label: "10" },
                  { value: 14, label: "14" },
                  { value: 18, label: "18" },
                  { value: 20, label: "20" },
                ]}
              />
            </div>

            <Group grow>
              <NumberInput
                label={t("LearnFromMistakes.Threads", { defaultValue: "Threads" })}
                description={t("LearnFromMistakes.ThreadsDesc", { defaultValue: "CPU threads for engine analysis" })}
                min={1}
                max={navigator.hardwareConcurrency || 128}
                value={threads}
                onChange={(v) => setThreads(Number(v) || 1)}
              />
              <Tooltip
                label={t("LearnFromMistakes.HashTooltip", { defaultValue: "Memory allocated for engine hash table. Higher values improve analysis quality." })}
                multiline
                w={250}
              >
                <NumberInput
                  label={t("LearnFromMistakes.Hash", { defaultValue: "Hash (MB)" })}
                  description={t("LearnFromMistakes.HashDesc", { defaultValue: "Hash table size in megabytes" })}
                  min={1}
                  max={65536}
                  step={64}
                  value={hash}
                  onChange={(v) => setHash(Number(v) || 128)}
                  rightSection={<IconInfoCircle size={16} />}
                />
              </Tooltip>
            </Group>
          </>
        )}

        <Checkbox.Group
          label={t("LearnFromMistakes.MistakeTypes")}
          value={annotations}
          onChange={setAnnotations}
        >
          <Group mt="xs">
            <Checkbox value="??" label={`${t("Annotate.Blunder")} (??)`} />
            <Checkbox value="?" label={`${t("Annotate.Mistake")} (?)`} />
            <Checkbox value="?!" label={`${t("Annotate.Dubious")} (?!)`} />
            <Checkbox value="miss" label={t("LearnFromMistakes.Miss", { defaultValue: "Missed Opportunity" })} />
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
          disabled={!selectedAccount || !selectedEngine || annotations.length === 0 || (isHybridEngine && !selectedHybridEngine)}
        >
          {isCloudEngine
            ? t("LearnFromMistakes.StartCloudAnalysis", { defaultValue: "Start Cloud Analysis" })
            : isHybridEngine
              ? t("LearnFromMistakes.StartHybridAnalysis", { defaultValue: "Start Hybrid Analysis" })
              : t("LearnFromMistakes.StartAnalysis")}
        </Button>
      </Stack>
    </Card>
  );
}
