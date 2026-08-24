import { describe, expect, it } from "vitest";
import { decodeArrowStream, encodeArrowSchema, encodeArrowStream } from "../src/arrow-ipc";

describe("Arrow IPC worker protocol codec", () => {
  it("round-trips primitive query batches", () => {
    const stream = encodeArrowStream(
      [
        { name: "id", type: "int64", nullable: false },
        { name: "name", type: "utf8", nullable: true },
        { name: "active", type: "bool", nullable: false },
      ],
      [
        { id: 7, name: "first", active: true },
        { id: 8, name: null, active: false },
      ],
    );
    expect(decodeArrowStream(stream)).toEqual({
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "name", type: "utf8", nullable: true },
        { name: "active", type: "bool", nullable: false },
      ],
      rows: [
        { id: 7, name: "first", active: true },
        { id: 8, name: null, active: false },
      ],
      rowsRead: 2,
      rowsWritten: 0,
    });
  });

  it("writes an empty schema stream accepted by the decoder", () => {
    const stream = encodeArrowSchema([{ name: "value", type: "utf8" }]);
    expect(decodeArrowStream(stream)).toMatchObject({
      columns: [{ name: "value", type: "utf8", nullable: true }],
      rows: [],
      rowsRead: 0,
    });
  });
});
