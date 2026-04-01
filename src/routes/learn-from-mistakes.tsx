import { createFileRoute } from "@tanstack/react-router";
import LearnFromMistakes from "@/components/learn-from-mistakes/LearnFromMistakes";

export const Route = createFileRoute("/learn-from-mistakes")({
  component: LearnFromMistakes,
});
