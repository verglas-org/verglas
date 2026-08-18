# Authorization with Verglas

Use the Verglas authorizer when one Lakekeeper instance serves one or more lakehouse databases inside a Verglas tenant. Verglas remains the only policy source. The trusted Verglas catalog proxy verifies the caller token and forwards the opaque bearer unchanged. Lakekeeper maps each catalog operation to a stable resource and action, then asks the tenant-local Verglas access service to authorize that same bearer.

## Run the Verglas-maintained image

The Verglas fork publishes `ghcr.io/verglas-org/lakekeeper:latest` from `main` and an immutable `ghcr.io/verglas-org/lakekeeper:<full-git-sha>` tag for every published commit. Pin the full SHA in production. Use `latest` only for local development or when the deployment controller records and verifies the resolved digest.

## Configure the adapter

Set the backend and these Verglas fields:

```shell
LAKEKEEPER__AUTHZ_BACKEND=verglas
LAKEKEEPER__VERGLAS__ENDPOINT=http://verglas-access:8345/
LAKEKEEPER__VERGLAS__WORKLOAD_CREDENTIAL_FILE=/run/secrets/verglas-policy-engine-token
LAKEKEEPER__VERGLAS__TENANT_ID=tenant-123
```

The workload credential must be a short-lived Verglas token whose audience is `policy-engine` and whose principal can synchronize Lakekeeper-owned resource records. Mount it as a read-only file. Lakekeeper rereads the file for every lifecycle operation, so an atomic file replacement rotates the credential without restarting the catalog.

Do not give Lakekeeper a tenant owner token. The workload credential does not authorize catalog requests, supply an end-user identity, or grant Lakekeeper access to data.

## Decision contract

For each catalog authorization decision, Lakekeeper calls `POST /v1/access/authorize` with the caller's opaque data-plane bearer:

```http
Authorization: Bearer <opaque-caller-token>
Content-Type: application/json

{
  "audience": "data-plane",
  "resource_id": "warehouse/7f9.../table/91a...",
  "action": "query"
}
```

The request body does not contain a tenant or principal. The access service derives both values from the bearer and returns them with the decision. Lakekeeper verifies that the returned tenant matches its configured tenant and fails closed if the bearer is missing, the service is unavailable, a non-success status is returned, or the response is invalid.

The catalog proxy removes any caller-provided `X-Verglas-Database-ID` header and injects the stable database ID from its trusted runtime registry. Lakekeeper accepts opaque bearers only when the Verglas authorizer is selected. It does not accept role-assumption headers or synchronize the local placeholder actor as a user.

## Lifecycle synchronization

Lakekeeper synchronizes authorization resources before committing catalog object creation or deletion. It uses `POST /v1/access/policy/resources` with `{id,kind,parent_id}` for idempotent creation and `DELETE /v1/access/policy/resources/<percent-encoded-id>` for idempotent deletion. Verglas owns user and process principals; Lakekeeper does not create principal records.

The access service derives the tenant from the `policy-engine` bearer. It permits creation only when the workload principal has `create_child` on the declared parent and permits deletion only when it has `modify` on the exact target. Lakekeeper fails the catalog lifecycle operation if synchronization fails; it never falls back to an allow-all policy or a second authorization store.

## Resource mapping

Verglas provisions each `database/<stable-database-id>` resource. When Lakekeeper creates a warehouse, it reads the trusted database ID header and registers `warehouse/<warehouse-id>` as a child of that database. Lakekeeper uses immutable IDs rather than display names, so renaming a database, project, warehouse, namespace, table, or view does not change its authorization identity.

| Lakekeeper object | Verglas resource |
| --- | --- |
| Database | `database/<database-id>` |
| Lakekeeper control root | `lakekeeper` |
| Project | `lakekeeper/project/<project-id>` |
| Warehouse | `warehouse/<warehouse-id>` |
| Namespace | `namespace/<namespace-id>` |
| Table | `warehouse/<warehouse-id>/table/<table-id>` |
| View | `warehouse/<warehouse-id>/view/<view-id>` |
| Generic table | `warehouse/<warehouse-id>/generic-table/<table-id>` |
| Role | `lakekeeper/role/<role-id>` |
| Tag | `lakekeeper/tag/<tag-id>` |

Register these resources and their parent relationships in Verglas when the corresponding catalog objects are reconciled. Database grants can then be inherited by child catalog resources without granting access to another database in the same tenant.

## Action mapping

Lakekeeper maps reads to `discover`, `describe`, or `query`; child creation to `create_child`; task control and role assumption to `execute`; metadata and data mutations to `modify`; and role-assignment inspection or mutation to `manage_grants`.

Lakekeeper `WriteData` maps to `modify`, not `append`, because an Iceberg write credential can also delete data objects. Grant `append` only to APIs that can prove additive-only behavior.

## Caller identity

Verglas derives the caller identity from the opaque bearer on every authorization request. Lakekeeper never derives authority from its internal placeholder actor and never lets a request choose another principal for permission inspection or delegated execution. This prevents a caller from obtaining another user's decision by changing a request field or header.
