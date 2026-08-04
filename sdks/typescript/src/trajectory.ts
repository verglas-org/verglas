// The canonical session-input normalizer for the memory workflow (WHITEPAPER
// §7.6). Every supported harness transcript (Claude Code JSONL, Codex rollout,
// Letta Code, OpenHands, Hermes, OpenClaw) is turned into one validated
// trajectory-v1 record stream by @letta-ai/trajectory. This is the single
// capture format: the Rust capture source shells out to the CLI wrapper, scrubs
// the records, and enqueues them as the session's canonical record stream.
//
// The normalizer is verbatim — it never removes secrets. Secret scrubbing is a
// downstream property enforced before anything reaches the queue (Rust side).

import {
  normalizeTranscript as libNormalize,
  validateTranscript,
  NormalizationError,
  type Diagnostic,
  type NormalizeResult,
  type NormalizedRecord,
  type TranscriptTrajectorySource,
} from "@letta-ai/trajectory";

export {
  validateTranscript,
  NormalizationError,
  type Diagnostic,
  type NormalizeResult,
  type NormalizedRecord,
  type TranscriptTrajectorySource,
};

/**
 * The transcript-based harness sources this build normalizes. Deep Agents is
 * checkpoint-based (a LangGraph SQLite store, not a transcript string) and is
 * not wired into the file-based capture path.
 */
export const HARNESS_SOURCES = [
  "claude-code",
  "codex",
  "hermes",
  "letta-code",
  "openclaw",
  "openhands",
] as const satisfies readonly TranscriptTrajectorySource[];

/** Narrows an arbitrary string to a supported transcript source. */
export function isHarnessSource(source: string): source is TranscriptTrajectorySource {
  return (HARNESS_SOURCES as readonly string[]).includes(source);
}

/**
 * Normalizes one harness-native transcript into trajectory-v1 records. Throws
 * a {@link NormalizationError} when the transcript is structurally invalid (for
 * example no user/assistant turn); line-level junk is reported as diagnostics,
 * not thrown. Callers on the capture path use {@link safeNormalize} instead so a
 * bad transcript never fails the host session.
 */
export function normalizeTranscript(
  source: TranscriptTrajectorySource,
  transcript: string,
): NormalizeResult {
  return libNormalize({ source, transcript });
}

/** The result of a never-throwing normalization. */
export interface SafeNormalizeResult {
  records: NormalizedRecord[];
  diagnostics: Diagnostic[];
  /** Set when normalization could not run at all; records is then empty. */
  error?: string;
}

/**
 * Normalizes without ever throwing: a structurally invalid transcript yields an
 * empty record set and a reported `error` rather than an exception. This is the
 * capture-path entry — fail open, exactly like the pre-trajectory capture hook,
 * so a malformed transcript is a no-op, never a crash.
 */
export function safeNormalize(source: string, transcript: string): SafeNormalizeResult {
  if (!isHarnessSource(source)) {
    return { records: [], diagnostics: [], error: `unknown source: ${source}` };
  }
  try {
    return normalizeTranscript(source, transcript);
  } catch (e) {
    const message = e instanceof NormalizationError ? `${e.code}: ${e.message}` : String(e);
    return { records: [], diagnostics: [], error: message };
  }
}
