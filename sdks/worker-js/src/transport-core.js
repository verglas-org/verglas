/**
 * Dependency-free WIT transport adapter logic. The component entry supplies the
 * generated WIT functions; tests supply a mock host with the same verbs.
 */

import { bytesFromValue, errorMessage } from './http.js';

/** @param {unknown} result @param {string} operation @returns {unknown} */
export function unwrapResult(result, operation) {
  if (result && typeof result === 'object' && 'tag' in result) {
    if (result.tag === 'ok') return result.val;
    if (result.tag === 'err') throw new Error(`${operation}: ${errorMessage(result.val)}`);
  }
  return result;
}

/** @param {unknown} value @returns {unknown} */
export function unwrapOption(value) {
  if (value && typeof value === 'object' && 'tag' in value) {
    if (value.tag === 'none') return undefined;
    if (value.tag === 'some') return value.val;
  }
  return value === null ? undefined : value;
}

/**
 * Creates one direct adapter from WIT verb functions. It does not serialize a
 * command line or construct a canonical transaction envelope.
 * @param {{get:Function,put:Function,delete:Function,list:Function,sqlRows:Function,streamSend:Function,setAlarm:Function,getAlarm:Function,deleteAlarm:Function,send:Function,close:Function,setAttachment:Function,getAttachment:Function,attached:Function,doFetch:Function}} imports
 * @returns {object}
 */
export function createTransport(imports) {
  const call = (operation, functionToCall, ...args) => unwrapResult(functionToCall(...args), operation);
  return {
    get(key) {
      return unwrapOption(call('storage.get', imports.get, String(key)));
    },
    put(key, value) {
      return call('storage.put', imports.put, String(key), bytesFromValue(value));
    },
    delete(key) {
      return Boolean(call('storage.delete', imports.delete, String(key)));
    },
    list(prefix, limit) {
      return call('storage.list', imports.list, String(prefix), limit);
    },
    sqlRows(statement) {
      return call('storage.sql-rows', imports.sqlRows, String(statement));
    },
    streamSend(binding, stream, records) {
      return call('storage.stream-send', imports.streamSend, String(binding), String(stream), String(records));
    },
    setAlarm(milliseconds) {
      return call('storage.set-alarm', imports.setAlarm, BigInt(milliseconds));
    },
    getAlarm() {
      return unwrapOption(call('storage.get-alarm', imports.getAlarm));
    },
    deleteAlarm() {
      return call('storage.delete-alarm', imports.deleteAlarm);
    },
    send(socket, message) {
      return call('sockets.send', imports.send, BigInt(socket), bytesFromValue(message));
    },
    close(socket, code = 1000, reason = '') {
      if (!Number.isInteger(code) || code < 0 || code > 65535) throw new RangeError('WebSocket close code must be an integer between 0 and 65535');
      return call('sockets.close', imports.close, BigInt(socket), code, String(reason));
    },
    setAttachment(socket, value) {
      return call('sockets.set-attachment', imports.setAttachment, BigInt(socket), bytesFromValue(value));
    },
    getAttachment(socket) {
      return unwrapOption(call('sockets.get-attachment', imports.getAttachment, BigInt(socket)));
    },
    attached() {
      return call('sockets.attached', imports.attached).map((socket) => BigInt(socket));
    },
    doFetch(binding, object, request) {
      return call('bindings.do-fetch', imports.doFetch, String(binding), String(object), request);
    },
  };
}
