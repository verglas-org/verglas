# Vectorize worklog

- #179: Added the prebuilt Cloudflare-shaped Vectorize Worker and Durable Object. Each named index stores immutable configuration, F32 vectors, namespaces, metadata declarations, and replay-stable mutation receipts in Turso, with hard-bounded native cosine, L2, and dot-product queries.
- #0: Consume the published JavaScript Worker SDK for component and runtime-surface tests. Vectorize no longer reaches into an SDK source tree owned by another repository.
