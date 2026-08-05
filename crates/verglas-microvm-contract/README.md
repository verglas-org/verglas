# verglas-microvm-contract

`verglas-microvm-contract` is the canonical portable `MicroVMStack` desired-state
contract. It owns YAML decoding, semantic validation, dependency-DAG validation,
JSON Schema generation, and generated TypeScript declarations.

Rust consumers use `parse_manifest`. Non-Rust consumers install the repository's
`@verglas/microvm-contract` package and call `parseManifest`.

Regenerate checked-in consumer artifacts after changing a contract type:

```sh
cargo run -p verglas-microvm-contract --example generate_artifacts
```

The artifact-conformance tests fail when generated output is stale.
