# Verglas Vessel contract

This crate is the portable parser and schema source for a composable Verglas Vessel. A Vessel is
one versioned release containing:

* zero or more independently versioned Integrations;
* zero or more independently versioned Workers, which become Jobs when invoked; and
* exactly one independently versioned graphical Interface.

The source manifest pins component versions by value. A build or publishing layer resolves each
project to an immutable content digest and records those digests in the published Vessel release.
Runtime instances retain both identities: the Vessel release and their component revision. A new
component version does not change an existing Vessel release; upgrading the Vessel reconciles its
Integration instances, derived Jobs, and Interface together.

## Integration configuration

Every Integration owns a `config` declaration containing typed fields and ordered setup guidance.
This is the contract used to render a configuration page. It never contains runtime credential
values. Secret fields cannot declare defaults, and the runtime resolves configured values through
its credential store when it starts the Integration component.

See `tests/fixtures/valid.yaml` for a complete composition.

## Consumer artifacts

Regenerate the checked-in JSON Schema and TypeScript declarations after changing the Rust types:

```sh
cargo run -p verglas-vessel-contract --example generate_artifacts
```
