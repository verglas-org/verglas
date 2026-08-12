import type { AiChatAuthorInfo } from "@verglas/workshop-shared/api";

/** The model-selection states surfaced by onboarding instead of treating every state as skippable. */
export type OnboardingModelStatus =
  "loading" | "error" | "empty" | "selection-required" | "ready";

/** Returns the honest state of the mandatory onboarding model choice. */
export function onboardingModelStatus(
  models: AiChatAuthorInfo[],
  selectedModelId: string | null,
  loading: boolean,
  loadError: boolean,
): OnboardingModelStatus {
  if (loading) return "loading";
  if (loadError) return "error";
  if (models.length === 0) return "empty";
  if (
    !selectedModelId ||
    !models.some((model) => model.id === selectedModelId)
  ) {
    return "selection-required";
  }
  return "ready";
}

/** Allows onboarding to advance only after a configured model has been explicitly selected. */
export function canContinueWithOnboardingModel(
  models: AiChatAuthorInfo[],
  selectedModelId: string | null,
  loading: boolean,
  loadError: boolean,
): boolean {
  return (
    onboardingModelStatus(models, selectedModelId, loading, loadError) ===
    "ready"
  );
}
