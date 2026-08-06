# Linear integration Vessel

This standalone HTTP service connects Verglas to one Linear workspace using either a personal API
key or an OAuth access token. It does not use a
Cloudflare Worker, Durable Object, Gatekeeper capability, or Cap'n Web.

## Configure

The service publishes its setup contract at `GET /v1/config/schema`. The response includes the
field definitions, setup instructions, and a link to Linear's API-key settings. Verglas OS can
render that response as the integration's configuration screen.

Submit the API key through the authenticated runtime proxy. Reading the secret from stdin keeps it
out of the Vessel manifest and command-line arguments:

```sh
read -s LINEAR_TOKEN
printf '{"apiToken":"%s"}' "$LINEAR_TOKEN" | \
  verglas vessel curl linear /v1/config --method PUT --data-stdin
unset LINEAR_TOKEN
```

The current proof of concept retains the credential in the integration process only. Durable,
encrypted credential storage and scoped Verglas lakehouse-token minting belong to the shared OS
credential service; they must land before this integration is considered production-ready.

## Query

The integration exposes the authenticated user and organization plus bounded, cursor-paginated
team and issue reads:

```sh
verglas vessel query linear '{"operation":"viewer"}'
verglas vessel query linear '{"operation":"teams","limit":25}'
verglas vessel query linear '{"operation":"issues","limit":25,"cursor":"..."}'
```

Additional operations should be explicit typed reads or writes. The integration must not expose
an unrestricted GraphQL relay by default.
