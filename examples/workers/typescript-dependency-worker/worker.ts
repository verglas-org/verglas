import { z } from "zod";

const resultPath = z.string().parse(process.env.RESULT_PATH);
await Bun.write(resultPath, JSON.stringify({ rows: 7, error: null }));
