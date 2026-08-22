#!/usr/bin/env python3
"""Mints the ES256 caller credential the hosted catalog requires.

The catalog validates callers against a JWKS published by the Cloudflare
Worker. This generates a throwaway P-256 key locally and emits both halves —
the JWKS the node trusts and one signed credential — so the local cluster can
serve real Iceberg traffic with no Worker and no network.

Test-only. The private key never leaves the generated directory.
"""
import json
import os
import sys
import time

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
import jwt

OUT = sys.argv[1] if len(sys.argv) > 1 else "generated"
ISSUER = os.environ.get("CATALOG_ISSUER", "https://verglas.local/issuer")
TENANT = os.environ.get("CATALOG_TENANT", "local")
KID = "verglas-local-key"


def b64url(value: int) -> str:
    import base64

    return base64.urlsafe_b64encode(value.to_bytes(32, "big")).rstrip(b"=").decode()


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    key = ec.generate_private_key(ec.SECP256R1())
    numbers = key.public_key().public_numbers()
    jwks = {
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "alg": "ES256",
                "use": "sig",
                "kid": KID,
                "x": b64url(numbers.x),
                "y": b64url(numbers.y),
            }
        ]
    }
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    # `resource: "/*"` with `action: "admin"` is the broadest grant the scope
    # matcher accepts. The subject under test is the catalog path, not the
    # permission algebra.
    token = jwt.encode(
        {
            "iss": ISSUER,
            "aud": "catalog",
            "sub": "verglas-local-principal",
            "jti": "verglas-local-token",
            "exp": int(time.time()) + 24 * 3600,
            "tenant_id": TENANT,
            "scope": [{"resource": "/*", "action": "admin"}],
        },
        pem,
        algorithm="ES256",
        headers={"kid": KID},
    )

    with open(os.path.join(OUT, "jwks.json"), "w") as handle:
        json.dump(jwks, handle)
    with open(os.path.join(OUT, "token"), "w") as handle:
        handle.write(token)
    print(f"issuer={ISSUER}")
    print(f"tenant={TENANT}")
    print(f"wrote {OUT}/jwks.json and {OUT}/token")
    return 0


if __name__ == "__main__":
    sys.exit(main())
