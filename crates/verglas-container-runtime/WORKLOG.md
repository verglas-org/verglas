# Worklog

- #49: Added the open-source Docker Engine placement crate with label-owned, idempotent container
  lifecycle reconciliation and fake-engine contract tests.
- #50: Added the authenticated persistent desired-state manager, shared-network and host-port
  placement, and the two-service Compose bootstrap.
- #50: Kept the self-host guide's tested Compose example synchronized with the two-service
  bootstrap after rebasing onto the merged runtime crate.
- #50: Built the runtime-manager image from the shared runtime base with scheduler only.
  It no longer layers on the Gadget runtime image or documents Gadgets as managed products.
