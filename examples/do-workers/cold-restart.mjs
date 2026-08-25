#!/usr/bin/env node

/**
 * Build and run the real six-product JavaScript/Python cold-restart proof.
 *
 * This harness deliberately requires production Turso credentials and the
 * production celld/runtime binaries. Set VERGLAS_TURSO_URL_TEMPLATE,
 * VERGLAS_TURSO_TOKEN_FILE, and VERGLAS_RUNTIME_HOST_CONFIG for a real run. It never swaps in the AC1 local
 * store or an in-process product adapter. TRANSCRIPT.md is not written here.
 */

import { createWriteStream } from 'node:fs';
import {
  access,
  mkdtemp,
  mkdir,
  readFile,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { createConnection } from 'node:net';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const HERE = resolve(fileURLToPath(new URL('.', import.meta.url)));
const REPO = resolve(HERE, '../..');
const JS_BUILDER = join(REPO, 'sdks/worker-js/bin/build.mjs');
const PY_BUILDER = join(REPO, 'sdks/worker-py/build.py');
const PYTHON = join(REPO, 'sdks/worker-py/.venv/bin/python');
const PRODUCTS = ['stream', 'pipeline', 'sink', 'catalog'];
const PRODUCT_DIRS = Object.fromEntries(PRODUCTS.map((product) => [
  product,
  join(REPO, 'system', product),
]));
const DEFAULT_GATEWAY = join(REPO, 'target/debug/verglas-gateway');
const DEFAULT_CELLD = join(REPO, 'target/debug/verglas-celld');
const DEFAULT_RUNTIME = join(REPO, 'target/debug/verglas-runtime');
const BUILD_ROOT_PREFIX = 'verglas-do-cold-chain-';
const READY_TIMEOUT_MS = 20_000;
const REQUEST_TIMEOUT_MS = 300_000;
const STOP_TIMEOUT_MS = 15_000;

/**
 * Parse the deliberately small harness command line.
 * @param {string[]} argv
 * @returns {{languages: string[], buildOnly: boolean, keep: boolean}}
 */
function parseArgs(argv) {
  let languages = ['js', 'py'];
  let buildOnly = false;
  let keep = false;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--build-only') {
      buildOnly = true;
    } else if (argument === '--keep') {
      keep = true;
    } else if (argument === '--language') {
      const value = argv[++index];
      if (value === 'js' || value === 'py') languages = [value];
      else if (value === 'both') languages = ['js', 'py'];
      else throw new Error(`--language must be js, py, or both; got ${value ?? '<missing>'}`);
    } else if (argument === '--help') {
      process.stdout.write(
        'usage: node examples/do-workers/cold-restart.mjs [--language js|py|both] [--build-only] [--keep]\n',
      );
      process.exit(0);
    } else {
      throw new Error(`unknown argument ${argument}`);
    }
  }
  return { languages, buildOnly, keep };
}

/**
 * Run one child command and retain its exact output for diagnostics.
 * @param {string} command
 * @param {string[]} args
 * @param {string} cwd
 * @param {string} label
 * @param {string} logPath
 * @returns {Promise<{stdout: string, stderr: string}>}
 */
async function runCommand(command, args, cwd, label, logPath) {
  process.stdout.write(`$ ${command} ${args.join(' ')}\n`);
  const child = spawn(command, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  const output = createWriteStream(logPath, { encoding: 'utf8' });
  child.stdout.on('data', (chunk) => {
    const text = String(chunk);
    stdout += text;
    output.write(text);
  });
  child.stderr.on('data', (chunk) => {
    const text = String(chunk);
    stderr += text;
    output.write(text);
  });
  const result = await new Promise((resolvePromise, reject) => {
    child.on('error', reject);
    child.on('close', (code, signal) => resolvePromise({ code, signal }));
  });
  output.end();
  if (result.code !== 0) {
    throw new Error(
      `${label} failed (code=${result.code ?? 'null'}, signal=${result.signal ?? 'none'}); log=${logPath}\n${stderr || stdout}`,
    );
  }
  return { stdout, stderr };
}

/**
 * Build all four prebuilt products and one language Worker component.
 * @param {string} language
 * @param {string} root
 * @returns {Promise<{language: string, root: string, workerManifest: object, products: object}>}
 */
async function buildArtifacts(language, root) {
  const buildRoot = join(root, language);
  await mkdir(buildRoot, { recursive: true });
  const logs = join(buildRoot, 'logs');
  await mkdir(logs, { recursive: true });
  const products = {};
  for (const product of PRODUCTS) {
    const output = join(buildRoot, product);
    await mkdir(output, { recursive: true });
    await runCommand(
      process.execPath,
      [JS_BUILDER, PRODUCT_DIRS[product], '--out', output],
      REPO,
      `build ${product}`,
      join(logs, `${product}.log`),
    );
    products[product] = JSON.parse(await readFile(join(output, 'manifest.out.json'), 'utf8'));
  }

  const workerProject = join(REPO, 'examples/do-workers', `${language}-cold-chain`);
  const workerOutput = join(buildRoot, 'worker');
  await mkdir(workerOutput, { recursive: true });
  const builder = language === 'js' ? process.execPath : PYTHON;
  const builderArgs = language === 'js'
    ? [JS_BUILDER, workerProject, '--out', workerOutput]
    : [PY_BUILDER, workerProject, '--out', workerOutput];
  await runCommand(
    builder,
    builderArgs,
    REPO,
    `build ${language} Worker`,
    join(logs, `${language}-worker.log`),
  );
  const workerManifest = JSON.parse(await readFile(join(workerOutput, 'manifest.out.json'), 'utf8'));
  return { language, root: buildRoot, workerManifest, products };
}

/**
 * Select one nested artifact descriptor from a builder manifest.
 * @param {object} manifest
 * @param {string} product
 * @returns {{digest: string, component_dir: string}}
 */
function descriptor(manifest, product) {
  const value = manifest.artifacts?.[product];
  if (!value || typeof value.digest !== 'string' || typeof value.component_dir !== 'string') {
    throw new Error(`manifest for ${product} is missing nested artifact descriptor`);
  }
  return value;
}

/**
 * Create the aggregate gateway manifest used by the production gateway.
 * @param {{language: string, root: string, workerManifest: object, products: object}} built
 * @param {string} urlTemplate
 * @param {string} tokenFile
 * @returns {object}
 */
function aggregateManifest(built, urlTemplate, tokenFile) {
  const worker = descriptor(built.workerManifest, 'worker');
  const durableObject = descriptor(built.workerManifest, 'durable_object');
  const productArtifacts = Object.fromEntries(PRODUCTS.map((product) => [
    product,
    descriptor(built.products[product], 'durable_object'),
  ]));
  const pipelineObject = 'orders';
  return {
    name: `${built.language}-cold-chain`,
    main: built.language === 'js' ? 'worker.js' : 'counter.py',
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    // The Pipeline product bakes source object `events`; this proof addresses
    // the Stream DO directly and does not route the optional Stream edge Worker,
    // whose stock manifest names its public object `main`.
    pipelines: [{ binding: 'STREAM', stream: 'events' }],
    // These object names are the baked system vars: orders, primary_sink,
    // and warehouse. The host service has no object identity.
    services: [
      { binding: 'PIPELINE', service: 'pipeline', object: pipelineObject },
      { binding: 'SINK_A', service: 'sink', object: 'primary_sink' },
      { binding: 'CATALOG', service: 'catalog', object: 'warehouse' },
      { binding: 'ICEBERG_COMMIT', service: 'verglas-runtime' },
    ],
    artifacts: {
      worker,
      durable_object: durableObject,
      stream: productArtifacts.stream,
      pipeline: productArtifacts.pipeline,
      sink: productArtifacts.sink,
      catalog: productArtifacts.catalog,
    },
    data_root: join(built.root, 'data'),
    turso: { url_template: urlTemplate, token_file: tokenFile },
  };
}

/**
 * Verify that the aggregate manifest points at digest-named bytes.
 * @param {object} manifest
 */
async function verifyManifestArtifacts(manifest) {
  for (const [product, value] of Object.entries(manifest.artifacts)) {
    const path = join(value.component_dir, `${value.digest}.wasm`);
    await access(path);
    const bytes = await stat(path);
    if (bytes.size === 0) throw new Error(`${product} artifact is empty: ${path}`);
  }
}

/**
 * Spawn a long-lived process and stream its output into a named log.
 * @param {string} command
 * @param {string[]} args
 * @param {string} cwd
 * @param {string} logPath
 * @returns {{child: import('node:child_process').ChildProcess, logPath: string}}
 */
function spawnLogged(command, args, cwd, logPath) {
  process.stdout.write(`$ ${command} ${args.join(' ')}\n`);
  const child = spawn(command, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
  const log = createWriteStream(logPath, { encoding: 'utf8' });
  child.stdout.on('data', (chunk) => log.write(chunk));
  child.stderr.on('data', (chunk) => log.write(chunk));
  child.on('close', () => log.end());
  return { child, logPath };
}

/**
 * Wait for a process to exit, returning its exit status.
 * @param {import('node:child_process').ChildProcess} child
 * @param {number} timeoutMs
 * @returns {Promise<{code: number|null, signal: string|null}>}
 */
function waitForExit(child, timeoutMs) {
  return new Promise((resolvePromise, reject) => {
    const timer = setTimeout(() => reject(new Error(`process ${child.pid} did not exit within ${timeoutMs}ms`)), timeoutMs);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      resolvePromise({ code, signal });
    });
  });
}

/**
 * Wait for a TCP listener without dispatching a Worker request.
 * @param {number} port
 * @param {number} timeoutMs
 * @returns {Promise<void>}
 */
async function waitForPort(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const connected = await new Promise((resolvePromise) => {
      const socket = createConnection({ host: '127.0.0.1', port });
      socket.once('connect', () => {
        socket.destroy();
        resolvePromise(true);
      });
      socket.once('error', () => {
        socket.destroy();
        resolvePromise(false);
      });
    });
    if (connected) return;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 100));
  }
  throw new Error(`gateway did not listen on 127.0.0.1:${port} within ${timeoutMs}ms`);
}

/**
 * Stop a process at the requested lifecycle boundary.
 * @param {{child: import('node:child_process').ChildProcess, logPath: string}|undefined} processHandle
 */
async function stopProcess(processHandle) {
  if (!processHandle || processHandle.child.exitCode !== null) return;
  processHandle.child.kill('SIGINT');
  try {
    await waitForExit(processHandle.child, STOP_TIMEOUT_MS);
  } catch {
    processHandle.child.kill('SIGKILL');
    await waitForExit(processHandle.child, STOP_TIMEOUT_MS);
  }
}

/**
 * Send one bounded HTTP request and decode the JSON response.
 * @param {string} base
 * @param {string} path
 * @param {string} method
 * @returns {Promise<{status: number, body: any, text: string}>}
 */
async function requestJson(base, path, method) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(`${base}${path}`, { method, signal: controller.signal });
    const text = await response.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = undefined;
    }
    return { status: response.status, body, text };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Require one successful JSON response and include its raw body on failure.
 * @param {string} base
 * @param {string} path
 * @param {string} method
 * @returns {Promise<any>}
 */
async function requireJson(base, path, method) {
  const result = await requestJson(base, path, method);
  if (result.status < 200 || result.status >= 300) {
    throw new Error(`${method} ${path} returned HTTP ${result.status}: ${result.text}`);
  }
  if (result.body === undefined) throw new Error(`${method} ${path} returned non-JSON: ${result.text}`);
  return result.body;
}

/**
 * Assert one language's durable count and product progress after each phase.
 * @param {string} base
 * @param {number} count
 */
async function assertProgress(base, count) {
  const counter = await requireJson(base, '/', 'GET');
  if (counter.count !== count) throw new Error(`counter count ${counter.count} did not equal ${count}`);
  const pipeline = await requireJson(base, '/pipeline-status', 'GET');
  if (pipeline.cursor !== count || pipeline.pending !== false) {
    throw new Error(`pipeline did not confirm cursor ${count}: ${JSON.stringify(pipeline)}`);
  }
  const sink = await requireJson(base, '/sink-status', 'GET');
  if (sink.confirmed_batches !== (count === 0 ? 0 : 1)) {
    throw new Error(`sink status did not confirm one batch: ${JSON.stringify(sink)}`);
  }
  const catalog = await requireJson(base, '/catalog-status', 'GET');
  if (catalog.confirmed_batches !== (count === 0 ? 0 : 1)) {
    throw new Error(`catalog status did not confirm one batch: ${JSON.stringify(catalog)}`);
  }
}

/**
 * Run one language through increment, process, shutdown, restart, and replay.
 * @param {{language: string, root: string, workerManifest: object, products: object}} built
 * @param {string} urlTemplate
 * @param {string} tokenFile
 * @param {string} gatewayBin
 * @param {string} celldBin
 * @param {string} runtimeBin
 * @param {string} runtimeHostConfig
 * @returns {Promise<object>}
 */
async function runLanguage(built, urlTemplate, tokenFile, gatewayBin, celldBin, runtimeBin, runtimeHostConfig) {
  const manifest = aggregateManifest(built, urlTemplate, tokenFile);
  await verifyManifestArtifacts(manifest);
  const manifestPath = join(built.root, 'gateway.json');
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  const dataRoot = manifest.data_root;
  await mkdir(dataRoot, { recursive: true });
  const logs = join(built.root, 'logs');
  const control = join(built.root, 'celld.sock');
  const port = built.language === 'js' ? 18180 : 18181;
  const base = `http://127.0.0.1:${port}`;
  let celld;
  let gateway;
  const start = () => {
    celld = spawnLogged(
      celldBin,
      [
        '--host-id', `${built.language}-cold-cell`,
        '--root', dataRoot,
        '--child', runtimeBin,
        '--control', control,
        '--catalog-host-config', runtimeHostConfig,
      ],
      REPO,
      join(logs, 'celld.log'),
    );
    gateway = spawnLogged(
      gatewayBin,
      ['--manifest', manifestPath, '--listen', `127.0.0.1:${port}`, '--celld-control', control, '--data-root', dataRoot],
      REPO,
      join(logs, 'gateway.log'),
    );
    return waitForPort(port, READY_TIMEOUT_MS);
  };
  try {
    await start();
    const first = await requireJson(base, '/incr', 'POST');
    if (first.count !== 1) throw new Error(`first increment returned ${JSON.stringify(first)}`);
    const second = await requireJson(base, '/incr', 'POST');
    if (second.count !== 2) throw new Error(`second increment returned ${JSON.stringify(second)}`);
    await requireJson(base, '/process', 'POST');
    await assertProgress(base, 2);
    await stopProcess(gateway);
    gateway = undefined;
    await stopProcess(celld);
    celld = undefined;

    await start();
    await assertProgress(base, 2);
    const replay = await requireJson(base, '/process', 'POST');
    if (replay.cursor !== 2 || replay.pending !== false) {
      throw new Error(`post-restart replay changed progress: ${JSON.stringify(replay)}`);
    }
    return { language: built.language, manifestPath, dataRoot, logs };
  } finally {
    await stopProcess(gateway);
    await stopProcess(celld);
  }
}

/**
 * Build the requested examples, then run only when production prerequisites exist.
 */
async function main() {
  const options = parseArgs(process.argv.slice(2));
  const buildRoot = await mkdtemp(join(tmpdir(), BUILD_ROOT_PREFIX));
  const urlTemplate = process.env.VERGLAS_TURSO_URL_TEMPLATE;
  const tokenFile = process.env.VERGLAS_TURSO_TOKEN_FILE;
  const runtimeHostConfig = process.env.VERGLAS_RUNTIME_HOST_CONFIG;
  const gatewayBin = process.env.VERGLAS_GATEWAY_BIN ?? DEFAULT_GATEWAY;
  const celldBin = process.env.VERGLAS_CELLD_BIN ?? DEFAULT_CELLD;
  const runtimeBin = process.env.VERGLAS_RUNTIME_BIN ?? DEFAULT_RUNTIME;
  const built = [];
  try {
    for (const language of options.languages) built.push(await buildArtifacts(language, buildRoot));
    process.stdout.write(`built artifacts under ${buildRoot}\n`);
    if (options.buildOnly) return;
    if (!urlTemplate || !tokenFile || !runtimeHostConfig) {
      throw new Error('blocked: set VERGLAS_TURSO_URL_TEMPLATE, VERGLAS_TURSO_TOKEN_FILE, and VERGLAS_RUNTIME_HOST_CONFIG for the production cold-restart run');
    }
    await access(tokenFile);
    await access(runtimeHostConfig);
    for (const binary of [gatewayBin, celldBin, runtimeBin]) await access(binary);
    for (const item of built) {
      const result = await runLanguage(
        item,
        urlTemplate,
        tokenFile,
        gatewayBin,
        celldBin,
        runtimeBin,
        runtimeHostConfig,
      );
      process.stdout.write(`PASS ${item.language}: ${JSON.stringify(result)}\n`);
    }
    process.stdout.write('PASS JS and Python six-product cold-restart runs\n');
  } finally {
    if (options.keep) process.stdout.write(`kept build root ${buildRoot}\n`);
    else await rm(buildRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error}\n`);
  process.exitCode = 1;
});
