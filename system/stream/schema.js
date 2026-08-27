/**
 * Frozen structured Stream schema parsing and record validation.
 * This module is the only schema authority; callers receive either one deeply
 * frozen schema or an explicit unstructured mode, never a compatibility fallback.
 */

export const MAX_SCHEMA_FIELDS = 64;
export const MAX_SCHEMA_DEPTH = 8;
export const MAX_SCHEMA_BYTES = 64 * 1024;
export const MAX_FIELD_NAME_BYTES = 128;
export const MAX_RECORD_FIELDS = 64;
export const MAX_RECORD_BYTES = 1024 * 1024;
export const MAX_LIST_ITEMS = 1000;
export const MAX_RECORDS_PER_REQUEST = 10_000;

export const USER_ERROR_FAMILIES = Object.freeze([
  'invalid_json',
  'not_array',
  'request_limit',
  'record_limit',
  'field_limit',
  'list_limit',
  'missing_required_field',
  'null_value',
  'unknown_field',
  'schema_type_mismatch',
]);

const TYPES = new Set(['string', 'int32', 'int64', 'float32', 'float64', 'bool', 'timestamp', 'json', 'binary', 'list', 'struct']);
const SCHEMA_KEYS = new Set(['fields']);
const FIELD_KEYS = new Set(['name', 'type', 'required', 'items', 'fields']);
const TYPE_KEYS = new Set(['type', 'items', 'fields']);
const encoder = new TextEncoder();

/**
 * Validates and deeply freezes the deployment schema. An omitted value means
 * unstructured mode; any supplied value must be the complete schema shape.
 * @param {unknown} value
 * @returns {object|undefined}
 */
export function validateSchema(value) {
  if (value === undefined) return undefined;
  if (!plainObject(value)) throw new Error('STREAM_SCHEMA must be an object with a fields array');
  rejectUnknownKeys(value, SCHEMA_KEYS, 'STREAM_SCHEMA');
  if (!Array.isArray(value.fields) || value.fields.length === 0) {
    throw new Error('STREAM_SCHEMA.fields must be a non-empty array');
  }
  const count = { value: 0 };
  const fields = parseFields(value.fields, 1, count, 'STREAM_SCHEMA.fields');
  const schema = { fields };
  if (encoder.encode(JSON.stringify(schema)).byteLength > MAX_SCHEMA_BYTES) {
    throw new Error(`STREAM_SCHEMA exceeds the ${MAX_SCHEMA_BYTES}-byte ceiling`);
  }
  return deepFreeze(schema);
}

/**
 * Validates one decoded record against the immutable schema and global hard
 * ceilings. The returned family is stable and is suitable for user metrics.
 * @param {unknown} record
 * @param {object|undefined} schema
 * @returns {string|undefined}
 */
export function validateRecord(record, schema) {
  let encoded;
  try {
    encoded = encoder.encode(JSON.stringify(record));
  } catch (_error) {
    return 'record_limit';
  }
  if (encoded.byteLength > MAX_RECORD_BYTES) return 'record_limit';
  if (countFields(record) > MAX_RECORD_FIELDS) return 'field_limit';
  if (schema === undefined) return undefined;
  if (!plainObject(record)) return 'schema_type_mismatch';
  return validateStruct(record, schema.fields);
}

/** @returns {Record<string, number>} */
export function emptyUserErrors() {
  return Object.fromEntries(USER_ERROR_FAMILIES.map((family) => [family, 0]));
}

/** @param {Record<string, number>} target @param {string} family */
export function incrementUserError(target, family) {
  if (!Object.hasOwn(target, family)) throw new Error(`unknown user-error family: ${family}`);
  target[family] += 1;
}

/**
 * Converts Verglas's stable validation outcomes to Cloudflare's documented
 * deserialization family and error-type dimensions for operator metrics.
 * @param {Record<string, number>} counts
 * @returns {{deserialization: Record<string, number>}}
 */
export function documentedUserErrors(counts) {
  const result = {
    deserialization: {
      missing_field: 0,
      type_mismatch: 0,
      parse_failure: 0,
      null_value: 0,
    },
  };
  const typeByFamily = {
    missing_required_field: 'missing_field',
    null_value: 'null_value',
    schema_type_mismatch: 'type_mismatch',
    invalid_json: 'parse_failure',
    not_array: 'parse_failure',
    request_limit: 'parse_failure',
    record_limit: 'parse_failure',
    field_limit: 'parse_failure',
    list_limit: 'parse_failure',
    unknown_field: 'parse_failure',
  };
  for (const [family, count] of Object.entries(counts)) {
    const type = typeByFamily[family];
    if (type === undefined) throw new Error(`unknown user-error family: ${family}`);
    result.deserialization[type] += count;
  }
  return result;
}

/** @param {unknown} value @returns {boolean} */
function plainObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/** @param {Record<string, unknown>} object @param {Set<string>} allowed @param {string} path */
function rejectUnknownKeys(object, allowed, path) {
  for (const key of Object.keys(object)) {
    if (!allowed.has(key)) throw new Error(`unknown schema field key at ${path}: ${key}`);
  }
}

/** @param {unknown} value @param {number} depth @param {{value:number}} count @param {string} path @returns {Array<object>} */
function parseFields(value, depth, count, path) {
  if (depth > MAX_SCHEMA_DEPTH) throw new Error(`STREAM_SCHEMA exceeds the ${MAX_SCHEMA_DEPTH}-level depth ceiling`);
  if (!Array.isArray(value) || value.length === 0) throw new Error(`${path} must be a non-empty array`);
  const names = new Set();
  return value.map((rawField, index) => {
    const fieldPath = `${path}[${index}]`;
    if (!plainObject(rawField)) throw new Error(`${fieldPath} must be an object`);
    rejectUnknownKeys(rawField, FIELD_KEYS, fieldPath);
    const name = rawField.name;
    if (typeof name !== 'string' || name.length === 0) throw new Error(`${fieldPath}.name must be a non-empty string`);
    if (encoder.encode(name).byteLength > MAX_FIELD_NAME_BYTES) throw new Error(`${fieldPath}.name exceeds ${MAX_FIELD_NAME_BYTES} bytes`);
    if (names.has(name)) throw new Error(`duplicate schema field name at ${fieldPath}: ${name}`);
    names.add(name);
    const type = rawField.type;
    if (typeof type !== 'string' || !TYPES.has(type)) throw new Error(`${fieldPath}.type is unsupported`);
    if (typeof rawField.required !== 'boolean') throw new Error(`${fieldPath}.required must be a boolean`);
    count.value += 1;
    if (count.value > MAX_SCHEMA_FIELDS) throw new Error(`STREAM_SCHEMA exceeds the ${MAX_SCHEMA_FIELDS}-field ceiling`);

    const field = { name, type, required: rawField.required };
    if (type === 'list') {
      if (!Object.hasOwn(rawField, 'items') || !plainObject(rawField.items)) throw new Error(`${fieldPath}.items is required for list fields`);
      field.items = parseType(rawField.items, depth, count, `${fieldPath}.items`);
      if (Object.hasOwn(rawField, 'fields')) throw new Error(`${fieldPath}.fields is not valid for list fields`);
    } else if (type === 'struct') {
      if (!Object.hasOwn(rawField, 'fields')) throw new Error(`${fieldPath}.fields is required for struct fields`);
      field.fields = parseFields(rawField.fields, depth + 1, count, `${fieldPath}.fields`);
      if (Object.hasOwn(rawField, 'items')) throw new Error(`${fieldPath}.items is not valid for struct fields`);
    } else if (Object.hasOwn(rawField, 'items') || Object.hasOwn(rawField, 'fields')) {
      throw new Error(`${fieldPath} cannot contain nested schema keys for ${type}`);
    }
    return field;
  });
}

/** @param {unknown} value @param {number} depth @param {{value:number}} count @param {string} path @returns {object} */
function parseType(value, depth, count, path) {
  rejectUnknownKeys(value, TYPE_KEYS, path);
  const type = value.type;
  if (typeof type !== 'string' || !TYPES.has(type)) throw new Error(`${path}.type is unsupported`);
  const result = { type };
  if (type === 'list') {
    if (!Object.hasOwn(value, 'items') || !plainObject(value.items)) throw new Error(`${path}.items is required for list fields`);
    result.items = parseType(value.items, depth, count, `${path}.items`);
    if (Object.hasOwn(value, 'fields')) throw new Error(`${path}.fields is not valid for list fields`);
  } else if (type === 'struct') {
    if (!Object.hasOwn(value, 'fields')) throw new Error(`${path}.fields is required for struct fields`);
    result.fields = parseFields(value.fields, depth + 1, count, `${path}.fields`);
    if (Object.hasOwn(value, 'items')) throw new Error(`${path}.items is not valid for struct fields`);
  } else if (Object.hasOwn(value, 'items') || Object.hasOwn(value, 'fields')) {
    throw new Error(`${path} cannot contain nested schema keys for ${type}`);
  }
  return result;
}

/** @param {unknown} record @returns {number} */
function countFields(record) {
  if (Array.isArray(record)) {
    let total = 0;
    for (const value of record) {
      total += countFields(value);
      if (total > MAX_RECORD_FIELDS) return total;
    }
    return total;
  }
  if (!plainObject(record)) return 0;
  let total = Object.keys(record).length;
  if (total > MAX_RECORD_FIELDS) return total;
  for (const value of Object.values(record)) {
    total += countFields(value);
    if (total > MAX_RECORD_FIELDS) return total;
  }
  return total;
}

/** @param {Record<string, unknown>} record @param {Array<object>} fields @returns {string|undefined} */
function validateStruct(record, fields) {
  const known = new Map(fields.map((field) => [field.name, field]));
  for (const field of fields) {
    if (!Object.hasOwn(record, field.name)) {
      if (field.required) return 'missing_required_field';
      continue;
    }
    if (field.required && record[field.name] === null) return 'null_value';
    const family = validateType(record[field.name], field);
    if (family) return family;
  }
  for (const key of Object.keys(record)) {
    if (!known.has(key)) return 'unknown_field';
  }
  return undefined;
}

/** @param {unknown} value @param {object} field @returns {string|undefined} */
function validateType(value, field) {
  switch (field.type) {
    case 'string': return typeof value === 'string' ? undefined : 'schema_type_mismatch';
    case 'int32': return Number.isInteger(value) && value >= -2147483648 && value <= 2147483647 ? undefined : 'schema_type_mismatch';
    case 'int64': return Number.isSafeInteger(value) ? undefined : 'schema_type_mismatch';
    case 'float32':
    case 'float64': return typeof value === 'number' && Number.isFinite(value) ? undefined : 'schema_type_mismatch';
    case 'bool': return typeof value === 'boolean' ? undefined : 'schema_type_mismatch';
    case 'timestamp': return validTimestamp(value) ? undefined : 'schema_type_mismatch';
    case 'json': return undefined;
    case 'binary': return validBinary(value) ? undefined : 'schema_type_mismatch';
    case 'list':
      if (!Array.isArray(value)) return 'schema_type_mismatch';
      if (value.length > MAX_LIST_ITEMS) return 'list_limit';
      for (const item of value) {
        const family = validateType(item, field.items);
        if (family) return family;
      }
      return undefined;
    case 'struct':
      return plainObject(value) ? validateStruct(value, field.fields) : 'schema_type_mismatch';
    default: return 'schema_type_mismatch';
  }
}

/** @param {unknown} value @returns {boolean} */
function validTimestamp(value) {
  if (typeof value === 'number') return Number.isFinite(value);
  return typeof value === 'string' && value.includes('T') && !Number.isNaN(Date.parse(value));
}

/** @param {unknown} value @returns {boolean} */
function validBinary(value) {
  if (typeof value !== 'string' || value.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/u.test(value)) return false;
  const padding = value.endsWith('==') ? 2 : value.endsWith('=') ? 1 : 0;
  return (value.length / 4) * 3 - padding <= MAX_RECORD_BYTES;
}

/** @param {object} value @returns {object} */
function deepFreeze(value) {
  for (const nested of Object.values(value)) {
    if (nested && typeof nested === 'object' && !Object.isFrozen(nested)) deepFreeze(nested);
  }
  return Object.freeze(value);
}
