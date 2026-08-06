# Worklog

- #63: Added an authenticated, read-only Vessel manifest validation endpoint backed by the shared
  compositional contract; invalid compositions cannot partially mutate local desired state.

- #61: Added bounded, content-addressed TypeScript Vessel project builds with declared npm
  dependencies, a platform-owned Bun Dockerfile, authenticated project deployment API, and
  reconciliation into standalone Application or Integration images.

- #43: Added manifest discovery and streaming namespace invocation for private Integration Vessels, with stable Vessel-name identity validation and no public workload ports.
- #49: Added the open-source Docker Engine placement crate with label-owned, idempotent container
  lifecycle reconciliation and fake-engine contract tests.
- #50: Added the authenticated persistent desired-state manager, shared-network and host-port
  placement, and the two-service Compose bootstrap.
- #50: Kept the self-host guide's tested Compose example synchronized with the two-service
  bootstrap after rebasing onto the merged runtime crate.
- #50: Built the runtime-manager image from the shared runtime base with scheduler only.
  It no longer layers on the Gadget runtime image or documents Gadgets as managed products.
- #55: Added persisted Vessel declarations, one-container mapping, and authenticated private-network
  HTTP proxying without publishing workload ports to the host.
- #55: Exposed GET-only localhost preview routes for Application Vessels while keeping Integration
  HTTP surfaces behind runtime authentication.
- #55: Removed the in-tree Linear integration and dashboard Application crates. Vessel persistence,
  proxying, and Application previews stay; product examples are no longer bundled.
