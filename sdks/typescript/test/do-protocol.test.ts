import { describe, expect, it } from "vitest";
import { encodeCanonicalTransaction, encodeHex, encodeUtf8Hex } from "../src/do-protocol";

describe("Durable Object worker socket protocol", () => {
  it("encodes the engine's empty canonical envelope byte-for-byte", () => {
    const bytes = encodeCanonicalTransaction({
      doId: "agent-1",
      transactionId: "00000000-0000-0000-0000-000000000021",
      baseCommitSequence: 0,
      isolation: "snapshot",
    });
    expect(encodeHex(bytes)).toBe(
      "07000000000000006167656e742d310000000000000000000000000000002100000000000000000100000000000000000000000000000000",
    );
  });

  it("hex-encodes QUERY SQL as one whitespace-safe endpoint token", () => {
    expect(encodeUtf8Hex("SELECT value FROM kv")).toBe(
      "53454c4543542076616c75652046524f4d206b76",
    );
  });
});
