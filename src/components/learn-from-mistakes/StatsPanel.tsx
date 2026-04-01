"use no memo";
import { Card, Group, RingProgress, SimpleGrid, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { MistakeStats } from "@/bindings";

interface StatsPanelProps {
  stats: MistakeStats | null;
}

export default function StatsPanel({ stats }: StatsPanelProps) {
  const { t } = useTranslation();

  if (!stats || stats.total === 0) return null;

  const solved = stats.solvedCorrect + stats.solvedWrong;
  const solvedPct = stats.total > 0 ? (solved / stats.total) * 100 : 0;

  return (
    <Card withBorder shadow="sm" radius="md" p="md">
      <Group justify="space-between" align="flex-start">
        <SimpleGrid cols={4} spacing="md" style={{ flex: 1 }}>
          <StatItem
            label={t("LearnFromMistakes.TotalPuzzles")}
            value={stats.total.toString()}
            color="blue"
          />
          <StatItem
            label={t("Annotate.Blunder")}
            value={stats.blunders.toString()}
            color="red"
            sublabel="??"
          />
          <StatItem
            label={t("Annotate.Mistake")}
            value={stats.mistakes.toString()}
            color="orange"
            sublabel="?"
          />
          <StatItem
            label={t("Annotate.Dubious")}
            value={stats.inaccuracies.toString()}
            color="yellow"
            sublabel="?!"
          />
        </SimpleGrid>

        <Group gap="md">
          <RingProgress
            size={80}
            thickness={8}
            roundCaps
            sections={[
              { value: stats.accuracy, color: "green" },
              { value: 100 - stats.accuracy, color: "red" },
            ]}
            label={
              <Text ta="center" size="xs" fw={700}>
                {stats.accuracy.toFixed(0)}%
              </Text>
            }
          />
          <Stack gap={2}>
            <Text size="xs" c="dimmed">
              {t("LearnFromMistakes.Accuracy")}
            </Text>
            <Text size="xs" c="green">
              {t("LearnFromMistakes.Correct")}: {stats.solvedCorrect}
            </Text>
            <Text size="xs" c="red">
              {t("LearnFromMistakes.Incorrect")}: {stats.solvedWrong}
            </Text>
            <Text size="xs" c="dimmed">
              {t("LearnFromMistakes.Unsolved")}: {stats.unsolved}
            </Text>
          </Stack>
        </Group>
      </Group>
    </Card>
  );
}

function StatItem({
  label,
  value,
  color,
  sublabel,
}: {
  label: string;
  value: string;
  color: string;
  sublabel?: string;
}) {
  return (
    <Stack gap={2}>
      <Text size="xs" c="dimmed">
        {label} {sublabel ? `(${sublabel})` : ""}
      </Text>
      <Text size="xl" fw={700} c={color}>
        {value}
      </Text>
    </Stack>
  );
}
