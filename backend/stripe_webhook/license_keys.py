from __future__ import annotations

import base64
import json
import os
import time


def generate_keypair() -> None:
    """Print a fresh Ed25519 keypair to stdout for webhook-side signing."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )

    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    pub_der = pub.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    priv_der = priv.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    print("PUBLIC_KEY_HEX  (embed in the proprietary ahana_codec binary verifier):")
    print(" ", pub_der.hex())
    print()
    print("PRIVATE_KEY_B64 (store as AHANAFLOW_SIGNING_KEY on the webhook server):")
    print(" ", base64.b64encode(priv_der).decode())


def generate_license_key(
    private_key_b64: str,
    customer_id: str,
    tier: str = "pro",
    days: int = 365,
    extra_claims: dict[str, object] | None = None,
) -> str:
    """Sign and return a JWT license key."""
    from cryptography.hazmat.primitives.serialization import load_der_private_key

    priv_der = base64.b64decode(private_key_b64)
    priv = load_der_private_key(priv_der, password=None)

    now = int(time.time())
    exp = now + days * 86400

    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "EdDSA", "crv": "Ed25519"}, separators=(",", ":")).encode()
    ).rstrip(b"=").decode()

    payload = {"sub": customer_id, "tier": tier, "iat": now, "exp": exp}
    if extra_claims:
        payload.update(extra_claims)

    payload_b64 = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode()
    ).rstrip(b"=").decode()

    message = f"{header}.{payload_b64}".encode()
    signature = base64.urlsafe_b64encode(priv.sign(message)).rstrip(b"=").decode()

    return f"{header}.{payload_b64}.{signature}"


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if not args or args[0] == "generate-keypair":
        generate_keypair()
    elif args[0] == "issue":
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--customer", required=True)
        parser.add_argument("--days", type=int, default=365)
        parser.add_argument("--tier", default="pro")
        ns = parser.parse_args(args[1:])
        priv = os.environ.get("AHANAFLOW_SIGNING_KEY", "")
        if not priv:
            print("ERROR: set AHANAFLOW_SIGNING_KEY env var", file=sys.stderr)
            sys.exit(1)
        print(generate_license_key(priv, ns.customer, ns.tier, ns.days))
    else:
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        sys.exit(1)