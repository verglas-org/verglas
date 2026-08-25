/**
 * Generated WIT capability bindings for the Cloudflare Durable Object shim.
 * The core adapter maps structured calls directly to WIT capability imports.
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
  streamSend as witStreamSend,
} from 'verglas:do-worker/storage@0.1.0';
import {
  attached as witAttached,
  close as witClose,
  getAttachment as witGetAttachment,
  send as witSend,
  setAttachment as witSetAttachment,
} from 'verglas:do-worker/sockets@0.1.0';
import { doFetch as witDoFetch } from 'verglas:do-worker/bindings@0.1.0';
import { createTransport } from './transport-core.js';

/** Creates the transport backed by the component's WIT imports. */
export function createWitTransport() {
  return createTransport({
    get: witGet,
    put: witPut,
    delete: witDelete,
    list: witList,
    sqlRows: witSqlRows,
    streamSend: witStreamSend,
    setAlarm: witSetAlarm,
    getAlarm: witGetAlarm,
    deleteAlarm: witDeleteAlarm,
    send: witSend,
    close: witClose,
    setAttachment: witSetAttachment,
    getAttachment: witGetAttachment,
    attached: witAttached,
    doFetch: witDoFetch,
  });
}

export { unwrapOption, unwrapResult } from './transport-core.js';
