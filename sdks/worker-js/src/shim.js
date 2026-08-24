/**
 * Cloudflare-flavoured Durable Object Worker shim for the
 * verglas:do-worker@0.1.0 WIT world. WIT imports are intentionally versioned
 * interface specifiers; ComponentizeJS resolves these to the host capabilities.
 */

import {
  delete as witDelete,
  deleteAlarm as witDeleteAlarm,
  get as witGet,
  getAlarm as witGetAlarm,
  list as witList,
  put as witPut,
  setAlarm as witSetAlarm,
  sqlRows as witSqlRows,
} from 'verglas:do-worker/storage@0.1.0';
import {
  attached as witAttached,
  close as witClose,
  getAttachment as witGetAttachment,
  send as witSend,
  setAttachment as witSetAttachment,
} from 'verglas:do-worker/sockets@0.1.0';

import {
  bytesFromValue,
  errorMessage,
  makeRequest,
  makeResponse,
  u64,
  valueFromBytes,
} from './http.js';

/**
 * Converts ComponentizeJS's WIT result representation into an authoring value.
 * A host may return a plain value for `ok`; an explicit `err` is always raised.
 * @param {unknown} result
 * @param {string} operation
 * @returns {unknown}
 */
function unwrapResult(result, operation) {
  if (result && typeof result === 'object' && 'tag' in result) {
    if (result.tag === 'ok') {
      return result.val;
    }
    if (result.tag === 'err') {
      const value = result.val;
      throw new Error(`${operation}: ${errorMessage(value)}`);
    }
  }
  return result;
}

/**
 * Calls a WIT import and maps its handler-error result to an exception.
 * @param {string} operation
 * @param {Function} functionToCall
 * @param {...unknown} args
 * @returns {unknown}
 */
function callImport(operation, functionToCall, ...args) {
  return unwrapResult(functionToCall(...args), operation);
}

/**
 * Reads one optional byte value and applies a storage representation helper.
 * @param {string} key
 * @param {unknown} representation
 * @returns {Uint8Array|string|unknown|null}
 */
function readValue(key, representation) {
  const raw = callImport('storage.get', witGet, String(key));
  if (raw === undefined || raw === null) {
    return null;
  }
  return valueFromBytes(raw, storageRepresentation(representation));
}

/**
 * Normalizes a storage read helper argument.
 * @param {unknown} representation
 * @returns {'bytes'|'string'|'json'}
 */
function storageRepresentation(representation) {
  if (representation === undefined) {
    return 'bytes';
  }
  if (typeof representation === 'string') {
    if (representation === 'bytes' || representation === 'string' || representation === 'json') {
      return representation;
    }
  }
  if (representation && typeof representation === 'object' && 'type' in representation) {
    return storageRepresentation(representation.type);
  }
  throw new TypeError('Storage representation must be bytes, string, or json');
}

/**
 * Creates the transactional KV authoring surface.
 * @returns {object}
 */
function createStorage() {
  const storage = {
    get(key, representation) {
      return readValue(key, representation);
    },
    getBytes(key) {
      return readValue(key, 'bytes');
    },
    getString(key) {
      return readValue(key, 'string');
    },
    getJson(key) {
      return readValue(key, 'json');
    },
    put(key, value, options) {
      const representation = options === undefined ? 'bytes' : storageRepresentation(options);
      let bytes;
      if (representation === 'json') {
        bytes = bytesFromValue(JSON.stringify(value));
      } else {
        if (representation === 'string' && typeof value !== 'string') {
          throw new TypeError('String storage writes require a string value');
        }
        bytes = bytesFromValue(value);
      }
      callImport('storage.put', witPut, String(key), bytes);
    },
    putBytes(key, value) {
      callImport('storage.put', witPut, String(key), bytesFromValue(value));
    },
    putString(key, value) {
      if (typeof value !== 'string') {
        throw new TypeError('String storage writes require a string value');
      }
      callImport('storage.put', witPut, String(key), bytesFromValue(value));
    },
    putJson(key, value) {
      callImport('storage.put', witPut, String(key), bytesFromValue(JSON.stringify(value)));
    },
    delete(key) {
      return Boolean(callImport('storage.delete', witDelete, String(key)));
    },
    list(prefix = '', limit = 1000) {
      if (!Number.isSafeInteger(limit) || limit < 0 || limit > 0xffffffff) {
        throw new RangeError('Storage list limit must be an integer between 0 and 4294967295');
      }
      return callImport('storage.list', witList, String(prefix), limit);
    },
  };
  return Object.freeze(storage);
}

/**
 * Executes SQL through the WIT JSON-row encoding.
 * @param {string} statement
 * @returns {Array<object>}
 */
function executeSql(statement) {
  if (typeof statement !== 'string') {
    throw new TypeError('SQL statements must be strings');
  }
  const encodedRows = callImport('storage.sql-rows', witSqlRows, statement);
  const rows = JSON.parse(String(encodedRows));
  if (!Array.isArray(rows)) {
    throw new TypeError('env.sql returned a JSON value that is not an array');
  }
  if (!rows.every((row) => row && typeof row === 'object' && !Array.isArray(row))) {
    throw new TypeError('env.sql returned a row that is not an object');
  }
  return rows;
}

/**
 * Creates the alarm and socket capabilities exposed on env.
 * @returns {object}
 */
function createCapabilities() {
  const sockets = {
    send(socket, data) {
      callImport('sockets.send', witSend, u64(socket), bytesFromValue(data));
    },
    close(socket, code = 1000, reason = '') {
      if (!Number.isInteger(code) || code < 0 || code > 65535) {
        throw new RangeError('WebSocket close code must be an integer between 0 and 65535');
      }
      callImport('sockets.close', witClose, u64(socket), code, String(reason));
    },
    setAttachment(socket, value) {
      callImport('sockets.set-attachment', witSetAttachment, u64(socket), bytesFromValue(value));
    },
    getAttachment(socket, representation) {
      const raw = callImport('sockets.get-attachment', witGetAttachment, u64(socket));
      if (raw === undefined || raw === null) {
        return null;
      }
      return valueFromBytes(raw, storageRepresentation(representation));
    },
    getAttachmentString(socket) {
      return sockets.getAttachment(socket, 'string');
    },
    attached() {
      return callImport('sockets.attached', witAttached).map((socket) => u64(socket));
    },
  };

  const capabilities = {
    setAlarm(milliseconds) {
      callImport('storage.set-alarm', witSetAlarm, u64(milliseconds));
    },
    getAlarm() {
      const value = callImport('storage.get-alarm', witGetAlarm);
      return value === undefined || value === null ? null : Number(value);
    },
    deleteAlarm() {
      callImport('storage.delete-alarm', witDeleteAlarm);
    },
    sockets: Object.freeze(sockets),
  };
  return Object.freeze(capabilities);
}

/**
 * Creates the WIT handler export around one Cloudflare-flavoured Worker object.
 * @param {{fetch?: Function, init?: Function, alarm?: Function, webSocketMessage?: Function, webSocketClose?: Function}} worker
 * @returns {{init: Function, fetch: Function, alarm: Function, websocketMessage: Function, websocketClose: Function}}
 */
export function createHandler(worker) {
  if (!worker || typeof worker !== 'object') {
    throw new TypeError('Worker default export must be an object');
  }
  if (typeof worker.fetch !== 'function') {
    throw new TypeError('Worker default export must define fetch(request, env)');
  }

  const env = {
    storage: createStorage(),
    sql: executeSql,
    ...createCapabilities(),
  };
  Object.freeze(env);

  return {
    async init() {
      if (typeof worker.init === 'function') {
        await worker.init(env);
      }
    },
    async fetch(record) {
      const request = makeRequest(record);
      const response = await worker.fetch(request, env);
      return makeResponse(response);
    },
    async alarm(scheduledEpochMillis) {
      if (typeof worker.alarm === 'function') {
        await worker.alarm(Number(scheduledEpochMillis), env);
      }
    },
    async websocketMessage(socket, message) {
      if (typeof worker.webSocketMessage === 'function') {
        await worker.webSocketMessage(u64(socket), new Uint8Array(message), env);
      }
    },
    async websocketClose(socket, code, reason) {
      if (typeof worker.webSocketClose === 'function') {
        await worker.webSocketClose(u64(socket), code, reason, env);
      }
    },
  };
}

export { makeRequest, makeResponse, bytesFromValue, valueFromBytes };
