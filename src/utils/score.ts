import { minMax } from "@tiptap/react";
import type { Color } from "chessops";
import { match } from "ts-pattern";
import type { BestMoves, Score, ScoreValue } from "@/bindings";
import type { Annotation } from "./annotation";

export const INITIAL_SCORE: Score = {
    value: {
        type: "cp",
        value: 15,
    },
    wdl: null,
};

const CP_CEILING = 1000;

export function formatScore(score: ScoreValue, precision = 2): string {
    let scoreText = match(score.type)
        .with("cp", () => Math.abs(score.value / 100).toFixed(precision))
        .with("mate", () => `M${Math.abs(score.value)}`)
        .with("dtz", () => `DTZ${Math.abs(score.value)}`)
        .exhaustive();
    if (score.type !== "dtz") {
        if (score.value > 0) {
            scoreText = `+${scoreText}`;
        }
        if (score.value < 0) {
            scoreText = `-${scoreText}`;
        }
    }
    return scoreText;
}

export function getWinChance(centipawns: number) {
    return 50 + 50 * (2 / (1 + Math.exp(-0.00368208 * centipawns)) - 1);
}

export function normalizeScore(score: ScoreValue, color: Color): number {
    let cp = score.value;
    if (color === "black") {
        cp *= -1;
    }
    if (score.type === "mate") {
        cp = CP_CEILING * Math.sign(cp);
    }
    return minMax(cp, -CP_CEILING, CP_CEILING);
}

function normalizeScores(
    prev: ScoreValue,
    next: ScoreValue,
    color: Color,
): { prevCP: number; nextCP: number } {
    return {
        prevCP: normalizeScore(prev, color),
        nextCP: normalizeScore(next, color),
    };
}

export function getAccuracy(prev: ScoreValue, next: ScoreValue, color: Color): number {
    const { prevCP, nextCP } = normalizeScores(prev, next, color);
    return minMax(
        103.1668 * Math.exp(-0.04354 * (getWinChance(prevCP) - getWinChance(nextCP))) - 3.1669 + 1,
        0,
        100,
    );
}

export function getCPLoss(prev: ScoreValue, next: ScoreValue, color: Color): number {
    const { prevCP, nextCP } = normalizeScores(prev, next, color);

    return Math.max(0, prevCP - nextCP);
}

export function getAnnotation(
    prevprev: ScoreValue | null,
    prev: ScoreValue | null,
    next: ScoreValue,
    color: Color,
    prevMoves: BestMoves[],
    is_sacrifice?: boolean,
    move?: string,
): Annotation {
    const { prevCP, nextCP } = normalizeScores(prev || { type: "cp", value: 0 }, next, color);
    const winChanceDiff = getWinChance(prevCP) - getWinChance(nextCP);

    if (winChanceDiff > 20) {
        return "??";
    }
    if (winChanceDiff > 10) {
        return "?";
    }
    if (winChanceDiff > 5) {
        return "?!";
    }

    if (prevMoves.length > 1) {
        const scores = normalizeScores(prevMoves[0].score.value, prevMoves[1].score.value, color);
        if (
            getWinChance(scores.prevCP) - getWinChance(scores.nextCP) > 10 &&
            move === prevMoves[0].sanMoves[0]
        ) {
            const scores = normalizeScores(
                prevprev || { type: "cp", value: 0 },
                prevMoves[0].score.value,
                color,
            );
            if (is_sacrifice) {
                return "!!";
            }
            if (getWinChance(scores.nextCP) - getWinChance(scores.prevCP) > 5) {
                return "!";
            }
        } else if (is_sacrifice && nextCP > -200) {
            return "!?";
        }
    }
    return "";
}

// ── Enhanced classification & accuracy (from miss detection briefing) ────────

/**
 * Compute per-move accuracy from eval delta (centipawns).
 * Uses Chess.com's publicly reverse-engineered formula.
 */
export function moveAccuracyFromDelta(evalDeltaCp: number): number {
    const cpl = Math.max(0, evalDeltaCp) / 100; // convert to pawns
    const accuracy = 103.1668 * Math.exp(-0.04354 * cpl) - 3.1669;
    return Math.min(100, Math.max(0, accuracy));
}

/**
 * Compute overall game accuracy from an array of eval deltas (centipawns).
 * Averages the per-move accuracy scores clamped to [0, 100].
 */
export function gameAccuracyFromDeltas(evalDeltas: number[]): number {
    if (evalDeltas.length === 0) return 0;
    const sum = evalDeltas.reduce((acc, d) => acc + moveAccuracyFromDelta(d), 0);
    return Math.min(100, Math.max(0, sum / evalDeltas.length));
}

/**
 * Classify a move by centipawn eval delta.
 * Returns one of: "BEST", "EXCELLENT", "GOOD", "INACCURACY", "MISTAKE", "BLUNDER", "MISS"
 */
export function classifyMoveByCp(
    evalDelta: number,
    wasMateAvailable: boolean,
    isMateAllowedAfter: boolean,
): string {
    if (wasMateAvailable) return "MISS";
    if (isMateAllowedAfter) return "BLUNDER";
    if (evalDelta <= 10) return "BEST";
    if (evalDelta <= 25) return "EXCELLENT";
    if (evalDelta <= 50) return "GOOD";
    if (evalDelta <= 100) return "INACCURACY";
    if (evalDelta <= 300) return "MISTAKE";
    return "BLUNDER";
}

/**
 * Get a human-readable label for a miss type.
 */
export function getMissTypeLabel(missType: string): string {
    switch (missType) {
        case "MATE_MISSED":
            return "Missed Forced Mate";
        case "WINNING_OPPORTUNITY_MISSED":
            return "Missed Winning Move";
        default:
            return "";
    }
}

/**
 * Get a color for each move classification.
 */
export function getClassificationColor(classification: string): string {
    switch (classification) {
        case "BEST":
            return "teal";
        case "EXCELLENT":
            return "green";
        case "GOOD":
            return "lime";
        case "INACCURACY":
            return "yellow";
        case "MISTAKE":
            return "orange";
        case "BLUNDER":
            return "red";
        case "MISS":
            return "cyan";
        default:
            return "gray";
    }
}
