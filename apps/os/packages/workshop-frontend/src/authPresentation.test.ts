import { describe, expect, it } from "vitest";
import { shouldShowSignupLink } from "./authPresentation";

describe("shouldShowSignupLink", () => {
  it("shows account creation only when password auth and signups are both enabled", () => {
    expect(
      shouldShowSignupLink({ passwordAuthEnabled: true, signupsEnabled: true }),
    ).toBe(true);
    expect(
      shouldShowSignupLink({
        passwordAuthEnabled: true,
        signupsEnabled: false,
      }),
    ).toBe(false);
    expect(
      shouldShowSignupLink({
        passwordAuthEnabled: false,
        signupsEnabled: true,
      }),
    ).toBe(false);
  });
});
