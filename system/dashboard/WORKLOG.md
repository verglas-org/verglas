# Worklog

- #181: Added a stateless Dashboard Worker using semantic JSX components backed directly by Query bindings. Rendering escapes data, applies a restrictive CSP, and rejects row, column, chart-point, and response-byte overages instead of truncating results.
- #0: Added behavior coverage for the concrete Dashboard Worker, not only its component manifest. The test executes its declared Query binding and verifies escaped table/chart output plus the restrictive content-security policy.
- #0: Aligned the builtin Dashboard binding name with the remote Query Machine's `QUERY` binding. Split-Machine dashboard rendering now reaches the declared `analytics` Query object instead of receiving an unknown-binding response.
- #0: Consume the published JavaScript Worker SDK for component and runtime-surface tests. Dashboard no longer carries or imports a copied runtime shim.
