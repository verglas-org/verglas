/**
 * Parses and validates the deliberately small wrangler.jsonc contract used by
 * the JavaScript Durable Object build pipeline.
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const topLevelKeys = new Set([
  'name',
  'main',
  'compatibility_date',
  'compatibility_flags',
  'durable_objects',
  'migrations',
  'vars',
  'pipelines',
]);
const durableObjectsKeys = new Set(['bindings']);
const bindingKeys = new Set(['name', 'class_name']);
const migrationKeys = new Set(['tag', 'new_classes', 'new_sqlite_classes']);
const pipelineKeys = new Set(['binding', 'stream']);

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
 * Validates a manifest string array.
 * @param {unknown} value
 * @param {string} path
 * @returns {string[]}
 */
function stringArray(value, path) {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string' || entry.trim() === '')) {
    throw new ManifestError(`${path} must be an array of non-empty strings`);
  }
  return [...value];
}

/**
 * Parses the accepted Durable Object migration forms.
 * @param {unknown} value
 * @returns {Array<{tag:string, new_classes:string[], new_sqlite_classes:string[]}>}
 */
function parseMigrations(value) {
  if (!Array.isArray(value)) throw new ManifestError('manifest.migrations must be an array');
  return value.map((rawMigration, index) => {
    if (!rawMigration || typeof rawMigration !== 'object' || Array.isArray(rawMigration)) {
      throw new ManifestError(`manifest.migrations[${index}] must be an object`);
    }
    const migration = /** @type {Record<string, unknown>} */ (rawMigration);
    rejectUnknownKeys(migration, migrationKeys, `migrations[${index}]`);
    const tag = requiredString(migration, 'tag', `manifest.migrations[${index}]`);
    const newClasses = migration.new_classes === undefined
      ? []
      : stringArray(migration.new_classes, `manifest.migrations[${index}].new_classes`);
    const newSqliteClasses = migration.new_sqlite_classes === undefined
      ? []
      : stringArray(migration.new_sqlite_classes, `manifest.migrations[${index}].new_sqlite_classes`);
    return { tag, new_classes: newClasses, new_sqlite_classes: newSqliteClasses };
  });
}

/**
 * Parses the exact Cloudflare Pipeline binding shape.
 * @param {unknown} value
 * @returns {Array<{binding: string, stream: string}>}
 */
function parsePipelines(value) {
  if (!Array.isArray(value)) throw new ManifestError('manifest.pipelines must be an array');
  return value.map((rawPipeline, index) => {
    if (!rawPipeline || typeof rawPipeline !== 'object' || Array.isArray(rawPipeline)) {
      throw new ManifestError(`manifest.pipelines[${index}] must be an object`);
    }
    const pipeline = /** @type {Record<string, unknown>} */ (rawPipeline);
    rejectUnknownKeys(pipeline, pipelineKeys, `pipelines[${index}]`);
    return {
      binding: requiredString(pipeline, 'binding', `manifest.pipelines[${index}]`),
      stream: requiredString(pipeline, 'stream', `manifest.pipelines[${index}]`),
    };
  });
}

/**
 * Validates the supported wrangler manifest subset.
 * @param {unknown} raw
 * @returns {{name: string, main: string, compatibility_date?: string, compatibility_flags: string[], bindings: Array<{name: string, class_name: string}>, migrations: Array<{tag: string, new_classes: string[], new_sqlite_classes: string[]}>, vars: Record<string, unknown>, pipelines?: Array<{binding: string, stream: string}>}}
 */
export function parseWranglerManifest(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new ManifestError('wrangler.jsonc must contain a JSON object');
  }
  const object = /** @type {Record<string, unknown>} */ (raw);
  rejectUnknownKeys(object, topLevelKeys, 'top-level');

  const name = requiredString(object, 'name', 'manifest');
  const main = requiredString(object, 'main', 'manifest');
  let compatibilityDate;
  if (object.compatibility_date !== undefined) compatibilityDate = requiredString(object, 'compatibility_date', 'manifest');
  const compatibilityFlags = object.compatibility_flags === undefined
    ? []
    : stringArray(object.compatibility_flags, 'manifest.compatibility_flags');
  const migrations = object.migrations === undefined ? [] : parseMigrations(object.migrations);
  let vars = {};
  if (object.vars !== undefined) {
    if (!object.vars || typeof object.vars !== 'object' || Array.isArray(object.vars)) {
      throw new ManifestError('manifest.vars must be an object');
    }
    vars = { .../** @type {Record<string, unknown>} */ (object.vars) };
  }
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

  const pipelines = object.pipelines === undefined ? undefined : parsePipelines(object.pipelines);
  const names = new Set();
  for (const binding of bindings) {
    if (names.has(binding.name)) {
      throw new ManifestError(`duplicate durable object binding name: ${binding.name}`);
    }
    names.add(binding.name);
  }
  for (const pipeline of pipelines ?? []) {
    if (names.has(pipeline.binding)) {
      throw new ManifestError(`duplicate binding name: ${pipeline.binding}`);
    }
    names.add(pipeline.binding);
  }

  return {
    name,
    main,
    ...(compatibilityDate === undefined ? {} : { compatibility_date: compatibilityDate }),
    compatibility_flags: compatibilityFlags,
    bindings,
    migrations,
    vars,
    ...(pipelines === undefined ? {} : { pipelines }),
  };
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
