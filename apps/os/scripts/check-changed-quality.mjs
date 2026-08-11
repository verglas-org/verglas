import { execFileSync, spawnSync } from "node:child_process";
import { access, readFile } from "node:fs/promises";
import { extname, relative, resolve, sep } from "node:path";
import { check, resolveConfig } from "prettier";

const mode = process.argv[2];
if (mode !== "format" && mode !== "lint") {
  throw new Error("Usage: check-changed-quality.mjs <format|lint>");
}

const repository = execFileSync("git", ["rev-parse", "--show-toplevel"], {
  encoding: "utf8",
}).trim();
const client = process.cwd();
const clientPath = relative(repository, client).split(sep).join("/");

function gitFiles(args) {
  const [command, ...commandArgs] = args;
  const output = execFileSync(
    "git",
    ["-C", repository, command, "-z", ...commandArgs],
    {
      encoding: "buffer",
    },
  );
  return output.toString("utf8").split("\0").filter(Boolean);
}

function changedFiles() {
  const base = process.env.CLIENT_QUALITY_BASE_SHA;
  const head = process.env.CLIENT_QUALITY_HEAD_SHA || "HEAD";
  if (base) {
    return gitFiles([
      "diff",
      "--name-only",
      "--diff-filter=ACMR",
      base,
      head,
      "--",
      clientPath,
    ]);
  }

  return [
    ...gitFiles([
      "diff",
      "--name-only",
      "--diff-filter=ACMR",
      "HEAD",
      "--",
      clientPath,
    ]),
    ...gitFiles([
      "ls-files",
      "--others",
      "--exclude-standard",
      "--",
      clientPath,
    ]),
  ];
}

const ignoredSegments = [
  "/dist/",
  "/generated/",
  "/node_modules/",
  "/.wrangler/",
];
const formatExtensions = new Set([
  ".cjs",
  ".css",
  ".js",
  ".json",
  ".jsonc",
  ".md",
  ".mjs",
  ".ts",
  ".tsx",
  ".yaml",
  ".yml",
]);
const lintExtensions = new Set([".cjs", ".js", ".mjs", ".ts", ".tsx"]);

const candidates = [...new Set(changedFiles())]
  .filter(
    (file) => !ignoredSegments.some((segment) => `/${file}`.includes(segment)),
  )
  .filter((file) => file !== `${clientPath}/pnpm-lock.yaml`)
  .map((file) => resolve(repository, file));

const existing = [];
for (const candidate of candidates) {
  try {
    await access(candidate);
    existing.push(candidate);
  } catch {
    // A file may disappear between diff selection and this check; deleted files are irrelevant.
  }
}

if (mode === "format") {
  const failures = [];
  for (const file of existing.filter((candidate) =>
    formatExtensions.has(extname(candidate)),
  )) {
    const source = await readFile(file, "utf8");
    const options = { ...(await resolveConfig(file)), filepath: file };
    if (!(await check(source, options))) failures.push(relative(client, file));
  }
  if (failures.length > 0) {
    console.error(
      `Prettier would change:\n${failures.map((failure) => `  ${failure}`).join("\n")}`,
    );
    process.exitCode = 1;
  } else {
    console.log("Changed client files are formatted.");
  }
} else {
  const files = existing.filter((file) => lintExtensions.has(extname(file)));
  if (files.length === 0) {
    console.log("No changed JavaScript or TypeScript files to lint strictly.");
  } else {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "oxlint",
        "--deny-warnings",
        "--report-unused-disable-directives",
        ...files,
      ],
      { cwd: client, stdio: "inherit" },
    );
    if (result.error) throw result.error;
    process.exitCode = result.status ?? 1;
  }
}
