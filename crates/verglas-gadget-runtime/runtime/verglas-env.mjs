//! Builds the credential-hiding Verglas SDK environment injected into one Gadget host.

import { connect } from "@verglas/sdk";

/** Creates the frozen application environment backed by the captured runtime transport. */
export function makeVerglasEnvironment({ endpoint, token, fetchImpl }) {
  if (!endpoint) throw new Error("missing Gadget data capability endpoint");
  if (!token) throw new Error("missing Gadget data capability token");
  if (typeof fetchImpl !== "function") throw new TypeError("fetchImpl must be a function");

  return Object.freeze({
    VERGLAS: connect({ endpoint, token, fetch: fetchImpl }),
  });
}
