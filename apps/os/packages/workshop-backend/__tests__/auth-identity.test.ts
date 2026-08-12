import { describe, expect, it } from "vitest";
import { normalizeEmail } from "../src/user.js";

describe("normalizeEmail", () => {
  it("canonicalizes email identity for Durable Object and password-salt lookup", () => {
    expect(normalizeEmail("  J.Brown9513@GMAIL.COM ")).toBe(
      "j.brown9513@gmail.com",
    );
  });

  it("rejects usernames and malformed email addresses", () => {
    for (const value of [
      "jfbrown",
      "missing@domain",
      "@example.com",
      "two@@example.com",
    ]) {
      expect(() => normalizeEmail(value), value).toThrow(
        "Invalid email address",
      );
    }
  });
});
