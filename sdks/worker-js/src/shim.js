/**
 * Component entry helpers for the Cloudflare Workers compatibility surface.
 * The public authoring module is `cloudflare:workers`; this entry alone wires
 * the core classes to generated WIT imports for component exports.
 */

import {
  createDurableObjectBinding,
  createHandler as createHandlerCore,
  createWorker as createWorkerCore,
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  DurableObjectStorage,
  DurableObjectStub,
  SqlStorageCursor,
  PipelineBinding,
  createStreamBinding,
} from './cloudflare-workers.js';
import { createWitTransport } from './transport.js';

/** @param {object} project @param {object} manifest @param {object} [options] @returns {object} */
export function createHandler(project, manifest, options = {}) {
  return createHandlerCore(project, manifest, {
    ...options,
    transport: options.transport ?? createWitTransport(),
  });
}

/** @param {object} project @param {object} manifest @param {object} [options] @returns {object} */
export function createWorker(project, manifest, options = {}) {
  return createWorkerCore(project, manifest, {
    ...options,
    transport: options.transport ?? createWitTransport(),
  });
}

export {
  createDurableObjectBinding,
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  DurableObjectStorage,
  DurableObjectStub,
  SqlStorageCursor,
  PipelineBinding,
  createStreamBinding,
};
export {
  bytesFromValue,
  errorMessage,
  headersToTuples,
  makeRequest,
  makeResponse,
  requestToRecord,
  responseFromRecord,
  u64,
  valueFromBytes,
} from './http.js';
