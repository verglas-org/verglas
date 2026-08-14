# Contributor instructions

RIME is an evaluated search algorithm, not a prompt collection. Preserve the
AIDE draft, debug, and improve policy; parallelism may fan out one decision but
must not let completion order affect selection.

Every behavior change starts with a failing test. Evaluators are frozen before
candidate execution and remain independent of mutation agents. Hard gates must
reject incorrect behavior, dead code, unrequested fallbacks, architectural point
fixes, and guessed parsing or typing.

Every speculative workspace is owned by RIME. Promote the winner before removal
and exhaust cleanup on success, failure, and cancellation. Never delete the
caller's baseline workspace.

Use plain JavaScript modules and the Node test runner already present in the
repository. Run `npm test` and `npm pack --dry-run` before publishing changes.
