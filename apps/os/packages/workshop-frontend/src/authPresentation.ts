/** Minimal deployment auth state used to decide whether account creation is offered. */
export type SignupPresentationConfig = {
  /** Whether email/password authentication is available. */
  passwordAuthEnabled: boolean;
  /** Whether the deployment accepts new accounts. */
  signupsEnabled: boolean;
};

/** Returns whether the login surface should offer account creation. */
export function shouldShowSignupLink(config: SignupPresentationConfig): boolean {
  return config.passwordAuthEnabled && config.signupsEnabled;
}
