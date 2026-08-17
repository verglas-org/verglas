"""Mints a local Cloudflare-Access-shaped ES256 credential for `local-lite.sh`.

There is no Cloudflare tunnel in a local-lite run, so this stands in for it:
it produces the same JWKS + bearer-JWT shape (tenant_id, sub, jti, scope,
ES256/P-256, `aud=["lakekeeper"]`) that
`crates/authz-verglas/src/client.rs::DecisionClient` verifies in production,
so serve-craft's fail-closed authorizer has a real credential to check
against instead of the request being rejected outright.

Usage: local_lite_mint_credentials.py <tenant> <issuer>
Prints one JSON line: {"issuer", "tenant", "jwks", "jwt"}.
"""

import base64
import json
import sys
import time
import uuid

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def int_to_bytes(n: int, length: int) -> bytes:
    return n.to_bytes(length, "big")


def main() -> None:
    tenant = sys.argv[1] if len(sys.argv) > 1 else "local-lite"
    issuer = sys.argv[2] if len(sys.argv) > 2 else "https://local-lite.verglas.internal"

    priv = ec.generate_private_key(ec.SECP256R1())
    pub_numbers = priv.public_key().public_numbers()
    x = int_to_bytes(pub_numbers.x, 32)
    y = int_to_bytes(pub_numbers.y, 32)
    kid = "local-lite-1"

    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": b64u(x),
        "y": b64u(y),
        "kid": kid,
        "alg": "ES256",
        "use": "sig",
    }
    jwks = {"keys": [jwk]}

    now = int(time.time())
    header = {"alg": "ES256", "kid": kid, "typ": "JWT"}
    payload = {
        "iss": issuer,
        "aud": "lakekeeper",
        "tenant_id": tenant,
        "sub": "local-lite-cache-node",
        "jti": str(uuid.uuid4()),
        "exp": now + 3600,
        # "/*" matches every resource (see scope_matches in client.rs);
        # "admin" bypasses the specific-action check.
        "scope": [{"resource": "/*", "action": "admin"}],
    }
    signing_input = "{}.{}".format(
        b64u(json.dumps(header, separators=(",", ":")).encode()),
        b64u(json.dumps(payload, separators=(",", ":")).encode()),
    )
    der_sig = priv.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_sig)
    # jsonwebtoken's ES256 verifier expects the raw 64-byte r||s signature,
    # not the DER encoding `sign()` returns.
    raw_sig = int_to_bytes(r, 32) + int_to_bytes(s, 32)
    jwt = "{}.{}".format(signing_input, b64u(raw_sig))

    print(json.dumps({
        "issuer": issuer,
        "tenant": tenant,
        "jwks": json.dumps(jwks, separators=(",", ":")),
        "jwt": jwt,
    }))


if __name__ == "__main__":
    main()
