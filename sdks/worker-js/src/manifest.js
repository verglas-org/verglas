/**
 * Parses and validates the deliberately small wrangler.jsonc contract used by
 * the JavaScript Durable Object build pipeline.
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const topLevelKeys = new Set(['name', 'main', 'durable_objects']);
const durableObjectsKeys = new Set(['bindings']);
const bindingKeys = new Set(['name', 'class_name']);

/**
 * Error raised when a project manifest is outside the supported subset.
 */
export class ManifestError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = 'ManifestError';
  }
}

/**
 * Removes JSONC comments without changing text inside quoted strings.
 * @param {string} source
 * @returns {string}
 */
function stripComments(source) {
  let output = '';
  let inString = false;
  let escaped = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    const next = source[index + 1];

    if (inLineComment) {
      if (character === '\n') {
        inLineComment = false;
        output += character;
      } else {
        output += ' ';
      }
      continue;
    }

    if (inBlockComment) {
      if (character === '*' && next === '/') {
        inBlockComment = false;
        output += '  ';
        index += 1;
      } else {
        output += character === '\n' ? '\n' : ' ';
      }
      continue;
    }

    if (inString) {
      output += character;
      if (escaped) {
        escaped = false;
      } else if (character === '\\') {
        escaped = true;
      } else if (character === '"') {
        inString = false;
      }
      continue;
    }

    if (character === '"') {
      inString = true;
      output += character;
    } else if (character === '/' && next === '/') {
      inLineComment = true;
      output += '  ';
      index += 1;
    } else if (character === '/' && next === '*') {
      inBlockComment = true;
      output += '  ';
      index += 1;
    } else {
      output += character;
    }
  }

  if (inBlockComment) {
    throw new ManifestError('unterminated block comment in wrangler.jsonc');
  }
  return output;
}

/**
 * Removes trailing commas outside quoted strings.
 * @param {string} source
 * @returns {string}
 */
function stripTrailingCommas(source) {
  let output = '';
  let inString = false;
  let escaped = false;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (inString) {
      output += character;
      if (escaped) {
        escaped = false;
      } else if (character === '\\') {
        escaped = true;
      } else if (character === '"') {
        inString = false;
      }
      continue;
    }
    if (character === '"') {
      inString = true;
      output += character;
      continue;
    }
    if (character === ',') {
      let lookahead = index + 1;
      while (lookahead < source.length && /\s/u.test(source[lookahead])) {
        lookahead += 1;
      }
      if (source[lookahead] === '}' || source[lookahead] === ']') {
        continue;
      }
    }
    output += character;
  }
  return output;
}

/**
 * Parses one JSONC document.
 * @param {string} source
 * @returns {unknown}
 */
export function parseJsonc(source) {
  try {
    return JSON.parse(stripTrailingCommas(stripComments(source)));
  } catch (error) {
    if (error instanceof ManifestError) {
      throw error;
    }
    throw new ManifestError(`invalid wrangler.jsonc: ${error.message}`);
  }
}

/**
 * Requires a non-empty string field.
 * @param {Record<string, unknown>} object
 * @param {string} field
 * @param {string} path
 * @returns {string}
 */
function requiredString(object, field, path) {
  if (typeof object[field] !== 'string' || object[field].trim() === '') {
    throw new ManifestError(`${path}.${field} is required and must be a non-empty string`);
  }
  return object[field];
}

/**
 * Rejects keys not in a known object shape.
 * @param {Record<string, unknown>} object
 * @param {Set<string>} allowed
 * @param {string} path
 */
function rejectUnknownKeys(object, allowed, path) {
  for (const key of Object.keys(object)) {
    if (!allowed.has(key)) {
      throw new ManifestError(`unknown ${path} key: ${key}`);
    }
  }
}

/**
 * Validates the supported wrangler manifest subset.
 * @param {unknown} raw
 * @returns {{name: string, main: string, bindings: Array<{name: string, class_name: string}>}}
 */
export function parseWranglerManifest(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new ManifestError('wrangler.jsonc must contain a JSON object');
  }
  const object = /** @type {Record<string, unknown>} */ (raw);
  rejectUnknownKeys(object, topLevelKeys, 'top-level');

  const name = requiredString(object, 'name', 'manifest');
  const main = requiredString(object, 'main', 'manifest');
  const durableObjects = object.durable_objects;
  let bindings = [];

  if (durableObjects !== undefined) {
    if (!durableObjects || typeof durableObjects !== 'object' || Array.isArray(durableObjects)) {
      throw new ManifestError('manifest.durable_objects must be an object');
    }
    const durableObjectObject = /** @type {Record<string, unknown>} */ (durableObjects);
    rejectUnknownKeys(durableObjectObject, durableObjectsKeys, 'durable_objects');
    if (!Array.isArray(durableObjectObject.bindings)) {
      throw new ManifestError('manifest.durable_objects.bindings is required and must be an array');
    }
    bindings = durableObjectObject.bindings.map((rawBinding, index) => {
      if (!rawBinding || typeof rawBinding !== 'object' || Array.isArray(rawBinding)) {
        throw new ManifestError(`manifest.durable_objects.bindings[${index}] must be an object`);
      }
      const binding = /** @type {Record<string, unknown>} */ (rawBinding);
      rejectUnknownKeys(binding, bindingKeys, `durable_objects.bindings[${index}]`);
      return {
        name: requiredString(binding, 'name', `manifest.durable_objects.bindings[${index}]`),
        class_name: requiredString(binding, 'class_name', `manifest.durable_objects.bindings[${index}]`),
      };
    });
  }

  const names = new Set();
  for (const binding of bindings) {
    if (names.has(binding.name)) {
      throw new ManifestError(`duplicate durable object binding name: ${binding.name}`);
    }
    names.add(binding.name);
  }

  return { name, main, bindings };
}

/**
 * Reads and validates the project's wrangler.jsonc file.
 * @param {string} projectDir
 * @returns {Promise<ReturnType<typeof parseWranglerManifest>>}
 */
export async function readWranglerManifest(projectDir) {
  const path = join(projectDir, 'wrangler.jsonc');
  let source;
  try {
    source = await readFile(path, 'utf8');
  } catch (error) {
    throw new ManifestError(`cannot read ${path}: ${error.message}`);
  }
  return parseWranglerManifest(parseJsonc(source));
}
