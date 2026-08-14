#!/usr/bin/env node

// Generates native SDK DTOs directly from the checked-in REST-JSON models.
// These artifacts intentionally avoid a runtime code-generation dependency.

import { readFile, writeFile } from "node:fs/promises";

const files = ["s3vectors-service-2.json", "verglas-graphs-service-1.json"];
const models = await Promise.all(files.map(async (file) => JSON.parse(await readFile("contracts/" + file, "utf8"))));
const shapes = new Map();
for (const model of models) for (const [name, shape] of Object.entries(model.shapes)) if (!shapes.has(name)) shapes.set(name, shape);

function rustType(name) {
  const shape = shapes.get(name);
  if (shape.document || shape.type === "document") return "serde_json::Value";
  if (shape.type === "boolean") return "bool";
  if (shape.type === "integer") return "i32";
  if (shape.type === "long") return "i64";
  if (shape.type === "float") return "f32";
  if (shape.type === "double") return "f64";
  if (shape.type === "timestamp") return "String";
  if (shape.type === "string") return shape.enum ? name : "std::string::String";
  if (shape.type === "list") return "Vec<" + rustType(shape.member.shape) + ">";
  if (shape.type === "map") return "std::collections::BTreeMap<String, " + rustType(shape.value.shape) + ">";
  return name;
}

function tsType(name) {
  const shape = shapes.get(name);
  if (shape.document || shape.type === "document") return "unknown";
  if (shape.type === "boolean") return "boolean";
  if (["integer", "long", "float", "double"].includes(shape.type)) return "number";
  if (shape.type === "timestamp" || shape.type === "string") return shape.enum ? name : "string";
  if (shape.type === "list") return tsType(shape.member.shape) + "[]";
  if (shape.type === "map") return "Record<string, " + tsType(shape.value.shape) + ">";
  return name;
}

const rust = [
  "//! Field-level DTOs generated from the checked-in semantic service models.",
  "//! Do not edit by hand; run scripts/generate-semantic-dtos.mjs.",
  "",
  "use serde::{Deserialize, Serialize};",
  "",
];
const ts = [
  "// Field-level DTOs generated from the checked-in semantic service models.",
  "// Do not edit by hand; run scripts/generate-semantic-dtos.mjs.",
  "",
];
for (const [name, shape] of [...shapes.entries()].sort(([a], [b]) => a.localeCompare(b))) {
  if (shape.document || shape.type === "document") continue;
  if (shape.type === "string" && shape.enum) {
    rust.push("#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]", "#[serde(rename_all = \"camelCase\")]", "pub enum " + name + " {");
    for (const value of shape.enum) rust.push("    #[serde(rename = " + JSON.stringify(value) + ")]", "    " + value.replaceAll(/[^A-Za-z0-9]+(.)/g, (_, character) => character.toUpperCase()).replace(/^[0-9]/, "_$&").replace(/^./, (character) => character.toUpperCase()) + ",");
    rust.push("}", "");
    ts.push("export type " + name + " = " + shape.enum.map((value) => JSON.stringify(value)).join(" | ") + ";", "");
  } else if (shape.type === "structure" && shape.union) {
    rust.push("#[derive(Debug, Clone, Serialize, Deserialize)]", "#[serde(untagged)]", "pub enum " + name + " {");
    for (const [member, memberSpec] of Object.entries(shape.members)) rust.push("    " + member.replaceAll(/(^|[A-Z])([a-z])/g, (_, prefix, character) => prefix + character.toUpperCase()) + " { " + member.replaceAll(/([A-Z])/g, "_$1").toLowerCase() + ": " + rustType(memberSpec.shape) + " },");
    rust.push("}", "");
    ts.push("export type " + name + " = " + Object.entries(shape.members).map(([member, memberSpec]) => "{ " + member + ": " + tsType(memberSpec.shape) + " }").join(" | ") + ";", "");
  } else if (shape.type === "structure") {
    rust.push("#[derive(Debug, Clone, Serialize, Deserialize)]", "#[serde(rename_all = \"camelCase\")]", "pub struct " + name + " {");
    ts.push("export interface " + name + " {");
    const required = new Set(shape.required ?? []);
    for (const [member, memberSpec] of Object.entries(shape.members)) {
      const type = rustType(memberSpec.shape);
      rust.push("    pub " + member.replaceAll(/([A-Z])/g, "_$1").toLowerCase() + ": " + (required.has(member) ? type : "Option<" + type + ">") + ",");
      ts.push("  " + member + (required.has(member) ? "" : "?") + ": " + tsType(memberSpec.shape) + ";");
    }
    rust.push("}", "");
    ts.push("}", "");
  } else {
    rust.push("pub type " + name + " = " + rustType(name) + ";", "");
    ts.push("export type " + name + " = " + tsType(name) + ";", "");
  }
}
await writeFile("sdks/rust/src/semantic_types.rs", rust.join("\n") + "\n");
await writeFile("sdks/typescript/src/semantic-types.ts", ts.join("\n") + "\n");
