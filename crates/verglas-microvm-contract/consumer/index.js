// Generated-schema consumer for non-Rust runtimes. Keep semantic checks aligned
// with MicroVmStack::validate and cover both implementations with shared fixtures.

import Ajv2020 from "ajv/dist/2020.js";
import { parse } from "yaml";

import schema from "../artifacts/microvm-stack.schema.json" with { type: "json" };

const ajv = new Ajv2020({ allErrors: true });
ajv.addFormat("uint16", true);
ajv.addFormat("uint32", true);
const validateSchema = ajv.compile(schema);
const namePattern = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const sha256Pattern = /^[a-f0-9]{64}$/;

/**
 * Parse and validate a MicroVMStack YAML document.
 *
 * @param {string} yaml contract YAML
 * @returns {import("../artifacts/index.d.ts").MicroVmStack} validated desired state
 */
export function parseManifest(yaml) {
  let stack;
  try {
    stack = parse(yaml);
  } catch (error) {
    throw new Error(`invalid MicroVMStack YAML: ${errorMessage(error)}`);
  }
  if (!validateSchema(stack)) {
    throw new Error(
      `invalid MicroVMStack: ${validateSchema.errors
        .map((error) => `${error.instancePath || "/"} ${error.message}`)
        .join("; ")}`,
    );
  }
  validateSemantics(stack);
  return stack;
}

/** Validate invariants that JSON Schema cannot express. */
function validateSemantics(stack) {
  if (stack.apiVersion !== "verglas.io/v1alpha1") {
    invalid("apiVersion must be verglas.io/v1alpha1");
  }
  if (stack.kind !== "MicroVMStack") {
    invalid("kind must be MicroVMStack");
  }
  validateName("tenant.name", stack.tenant.name);
  if (stack.components.length === 0) {
    invalid("components must contain at least one component");
  }

  const components = new Map();
  for (const component of stack.components) {
    validateComponent(component);
    if (components.has(component.name)) {
      invalid(`duplicate component name \`${component.name}\``);
    }
    components.set(component.name, component);
  }

  for (const component of stack.components) {
    const dependencies = new Set();
    for (const dependency of component.dependsOn ?? []) {
      if (dependency === component.name) {
        invalid(`component \`${component.name}\` depends on itself`);
      }
      if (!components.has(dependency)) {
        invalid(`component \`${component.name}\` depends on missing component \`${dependency}\``);
      }
      if (dependencies.has(dependency)) {
        invalid(`component \`${component.name}\` repeats dependency \`${dependency}\``);
      }
      dependencies.add(dependency);
    }
  }

  validateAcyclic(components);
  const ingress = components.get(stack.ingress.component);
  if (!ingress) {
    invalid(`ingress references missing component \`${stack.ingress.component}\``);
  }
  if (!hasPort(ingress, stack.ingress.port)) {
    invalid(
      `ingress port \`${stack.ingress.port}\` is not declared by component \`${stack.ingress.component}\``,
    );
  }
}

/** Validate fields scoped to one component. */
function validateComponent(component) {
  validateName("component.name", component.name);
  const object = component.runtime.object;
  const segments = object.split("/");
  if (
    object.startsWith("/") ||
    !object.endsWith("/rootfs.ext4") ||
    /[\s\\?#]/.test(object) ||
    segments.some((segment) => segment === "" || segment === "." || segment === "..")
  ) {
    invalid(
      `runtime.object \`${object}\` must be a relative R2 key ending in /rootfs.ext4`,
    );
  }
  if (!sha256Pattern.test(component.runtime.sha256)) {
    invalid(`runtime.sha256 must be 64 lowercase hexadecimal characters`);
  }
  if (component.exec.length === 0 || component.exec.some((argument) => argument.length === 0)) {
    invalid(`component \`${component.name}\` exec must not be empty`);
  }
  if (component.resources.vcpus === 0 || component.resources.memoryMiB === 0) {
    invalid(`component \`${component.name}\` resources must be greater than zero`);
  }
  if (component.cluster?.members === 0) {
    invalid(`component \`${component.name}\` cluster.members must be greater than zero`);
  }

  const names = new Set();
  const numbers = new Set();
  if (component.network?.ports.length === 0) {
    invalid(`component \`${component.name}\` network.ports must not be empty`);
  }
  for (const port of component.network?.ports ?? []) {
    validateName("network port name", port.name);
    if (port.port === 0) {
      invalid(`component \`${component.name}\` network port must be greater than zero`);
    }
    if (names.has(port.name)) {
      invalid(`component \`${component.name}\` repeats network port \`${port.name}\``);
    }
    if (numbers.has(port.port)) {
      invalid(`component \`${component.name}\` repeats network port number \`${port.port}\``);
    }
    names.add(port.name);
    numbers.add(port.port);
  }
  if (component.health && !hasPort(component, component.health.port)) {
    invalid(`component \`${component.name}\` health.port \`${component.health.port}\` is not declared`);
  }
  if (
    component.health?.path &&
    (!component.health.path.startsWith("/") || /\s/.test(component.health.path))
  ) {
    invalid(`component \`${component.name}\` health.path must be an absolute HTTP path`);
  }
}

/** Validate one portable DNS-label identifier. */
function validateName(field, name) {
  if (!namePattern.test(name)) {
    invalid(`${field} \`${name}\` must be a lowercase DNS label`);
  }
}

/** Return whether a component provides a named port. */
function hasPort(component, name) {
  return component.network?.ports.some((port) => port.name === name) ?? false;
}

/** Detect dependency cycles with a depth-first traversal. */
function validateAcyclic(components) {
  const visiting = new Set();
  const visited = new Set();
  const visit = (name) => {
    if (visited.has(name)) return;
    if (visiting.has(name)) invalid(`dependency cycle includes component \`${name}\``);
    visiting.add(name);
    for (const dependency of components.get(name).dependsOn ?? []) visit(dependency);
    visiting.delete(name);
    visited.add(name);
  };
  for (const name of components.keys()) visit(name);
}

/** Throw a consistently prefixed semantic validation error. */
function invalid(message) {
  throw new Error(`invalid MicroVMStack: ${message}`);
}

/** Render an unknown caught value without assuming it is an Error. */
function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}
