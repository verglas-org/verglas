#!/usr/bin/env node

/**
 * Builds one wrangler-style JavaScript Durable Object project into a
 * content-addressed verglas:do-worker component.
 */

import { createHash } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

import { build as bundle } from 'esbuild';

import { readWranglerManifest } from '../src/manifest.js';

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repositoryDir = resolve(packageDir, '../..');
const witDir = resolve(repositoryDir, 'crates/verglas-do-wasm/wit');
const shimPath = resolve(packageDir, 'src/shim.js');
const jcoPath = resolve(packageDir, 'node_modules/.bin/jco');

/**
 * Runs one child process and preserves its stderr in the thrown error.
 * @param {string} command
 * @param {string[]} args
 * @param {import('node:child_process').SpawnOptions} options
 * @returns {Promise<void>}
 */
function run(command, args, options) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', reject);
    child.on('close', (code, signal) => {
      if (code === 0) {
        resolvePromise();
        return;
      }
      const detail = stderr.trim() || stdout.trim() || `exited with ${signal ?? `code ${code}`}`;
      reject(new Error(`${command} ${args.join(' ')} failed: ${detail}`));
    });
  });
}

/**
 * Parses the build CLI arguments.
 * @param {string[]} args
 * @returns {{projectDir: string, outputDir: string, gatewayPath: string}}
 */
function parseArguments(args) {
  if (args.length < 3) {
    throw new Error('usage: node sdks/worker-js/bin/build.mjs <project-dir> --out <dir> [--gateway <path>]');
  }
  const projectDir = resolve(args[0]);
  if (args[1] !== '--out' || !args[2] || (args.length !== 3 && args.length !== 5) || (args.length === 5 && args[3] !== '--gateway')) {
    throw new Error('usage: node sdks/worker-js/bin/build.mjs <project-dir> --out <dir> [--gateway <path>]');
  }
  return {
    projectDir,
    outputDir: resolve(args[2]),
    gatewayPath: resolve(args.length === 5 ? args[4] : join(projectDir, 'gateway.json')),
  };
}

/**
 * Updates a checked-in gateway manifest when the project owns one.
 * @param {string} gatewayPath
 * @param {string} outputDir
 * @param {string} componentDigest
 * @returns {Promise<void>}
 */
async function updateGatewayManifest(gatewayPath, outputDir, componentDigest) {
  let source;
  try {
    source = await readFile(gatewayPath, 'utf8');
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return;
    }
    throw error;
  }
  const manifest = JSON.parse(source);
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) {
    throw new Error(`gateway manifest ${gatewayPath} must contain a JSON object`);
  }
  manifest.component_digest = componentDigest;
  if (Object.hasOwn(manifest, 'component_dir')) {
    manifest.component_dir = resolve(outputDir);
  }
  await writeFile(gatewayPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
}

/**
 * Escapes one path for use in a generated ESM import statement.
 * @param {string} path
 * @returns {string}
 */
function importPath(path) {
  return JSON.stringify(path);
}

/**
 * Bundles and componentizes one wrangler-style project.
 * @param {string} projectDir
 * @param {string} outputDir
 * @param {string} [gatewayPath]
 * @returns {Promise<{name: string, componentDigest: string, componentPath: string, manifestPath: string, componentBytes: Uint8Array, bindings: Array<{name: string, class_name: string}>}>}
 */
export async function buildProject(projectDir, outputDir, gatewayPath = join(projectDir, 'gateway.json')) {
  const manifest = await readWranglerManifest(projectDir);
  const mainPath = resolve(projectDir, manifest.main);
  const workDir = await mkdtemp(join(tmpdir(), 'verglas-worker-js-'));
  const bundlePath = join(workDir, 'worker.bundle.js');
  const componentPath = join(workDir, 'worker.component.wasm');

  try {
    const entryPath = join(workDir, 'entry.js');
    const entrySource = [
      `import worker from ${importPath(mainPath)};`,
      `import { createHandler } from ${importPath(shimPath)};`,
      'export const handler = createHandler(worker);',
      '',
    ].join('\n');
    await writeFile(entryPath, entrySource, 'utf8');

    const bundled = await bundle({
      entryPoints: [entryPath],
      bundle: true,
      format: 'esm',
      platform: 'neutral',
      target: 'es2022',
      write: false,
      legalComments: 'none',
      minify: true,
      external: ['verglas:do-worker/*@0.1.0'],
    });
    if (bundled.outputFiles.length !== 1) {
      throw new Error(`esbuild produced ${bundled.outputFiles.length} files; exactly one bundle is required`);
    }
    await writeFile(bundlePath, bundled.outputFiles[0].contents);

    await run(
      jcoPath,
      [
        'componentize',
        bundlePath,
        '--wit',
        witDir,
        '--world-name',
        'durable-object',
        '--disable=all',
        '--out',
        componentPath,
      ],
      { cwd: repositoryDir },
    );

    const componentBytes = new Uint8Array(await readFile(componentPath));
    // ComponentizeJS snapshots StarlingMonkey and may emit different bytes for
    // identical input. The deployment digest always names the bytes emitted by
    // this invocation; it is never replaced with a source or manifest hash.
    const componentDigest = createHash('sha256').update(componentBytes).digest('hex');
    await mkdir(outputDir, { recursive: true });
    const outputComponentPath = join(outputDir, `${componentDigest}.wasm`);
    await writeFile(outputComponentPath, componentBytes);

    const outputManifest = {
      name: manifest.name,
      component_digest: componentDigest,
      bindings: manifest.bindings,
    };
    const manifestPath = join(outputDir, 'manifest.out.json');
    await writeFile(manifestPath, `${JSON.stringify(outputManifest, null, 2)}\n`, 'utf8');
    await updateGatewayManifest(gatewayPath, outputDir, componentDigest);

    return {
      name: manifest.name,
      componentDigest,
      componentPath: outputComponentPath,
      manifestPath,
      componentBytes,
      bindings: manifest.bindings,
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

/**
 * Runs the command-line entry point.
 */
async function main() {
  const { projectDir, outputDir } = parseArguments(process.argv.slice(2));
  const result = await buildProject(projectDir, outputDir);
  process.stdout.write(`${result.componentDigest}\n`);
}

const invokedPath = process.argv[1] ? realpathSync(process.argv[1]) : '';
if (invokedPath === fileURLToPath(import.meta.url)) {
  await main();
}
