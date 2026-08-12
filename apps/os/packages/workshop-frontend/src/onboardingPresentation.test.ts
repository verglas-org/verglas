import { describe, expect, it } from "vitest";
import type { AiChatAuthorInfo } from "@verglas/workshop-shared/api";
import {
  canContinueWithOnboardingModel,
  onboardingModelStatus,
} from "./onboardingPresentation";

const MODELS = [
  { id: "runtime:codex:gpt-5.6-sol", name: "GPT-5.6-Sol" },
  { id: "runtime:codex:gpt-5.6-terra", name: "GPT-5.6-Terra" },
] as AiChatAuthorInfo[];

describe("onboarding model selection", () => {
  it("requires an explicit configured model before continuing", () => {
    expect(canContinueWithOnboardingModel(MODELS, null, false, false)).toBe(
      false,
    );
    expect(
      canContinueWithOnboardingModel(MODELS, MODELS[0].id, false, false),
    ).toBe(true);
  });

  it("does not continue while models are loading or failed to load", () => {
    expect(
      canContinueWithOnboardingModel(MODELS, MODELS[0].id, true, false),
    ).toBe(false);
    expect(
      canContinueWithOnboardingModel(MODELS, MODELS[0].id, false, true),
    ).toBe(false);
  });

  it("rejects a stale model identifier", () => {
    expect(
      canContinueWithOnboardingModel(
        MODELS,
        "runtime:codex:removed",
        false,
        false,
      ),
    ).toBe(false);
  });

  it("describes blocking states honestly", () => {
    expect(onboardingModelStatus([], null, true, false)).toBe("loading");
    expect(onboardingModelStatus([], null, false, true)).toBe("error");
    expect(onboardingModelStatus([], null, false, false)).toBe("empty");
    expect(onboardingModelStatus(MODELS, null, false, false)).toBe(
      "selection-required",
    );
    expect(onboardingModelStatus(MODELS, MODELS[1].id, false, false)).toBe(
      "ready",
    );
  });
});
