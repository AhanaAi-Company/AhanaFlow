# Secret Rotation Runbook

This runbook covers the three required hardening actions for AhanaFlow production deployments:

1. Rotate compromised or aging credentials.
2. Move live secrets into a secret manager or mounted runtime files.
3. Publish only the cleaned open-source package and distribute proprietary binaries separately.

## Rotate Immediately

Rotate these outside the repository and update your runtime secret store:

- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `AHANAFLOW_SIGNING_KEY`
- `AHANAFLOW_ADMIN_API_KEY`
- `AHANAFLOW_SERVICE_API_KEY`
- `AHANAFLOW_SEALED_POLICY_KEY`
- `SMTP_PASS`

## Generate New Signing Keys

```bash
python -m backend.stripe_webhook.license_keys generate-keypair
```

Store the private key as `AHANAFLOW_SIGNING_KEY` in your secret manager. Embed the new public key only in the proprietary `ahana_codec` verifier build.

## Generate New Admin And Service Keys

```bash
openssl rand -hex 32 > /secure/path/admin_api_key
openssl rand -hex 32 > /secure/path/service_api_key
```

Mount them at runtime with:

- `AHANAFLOW_ADMIN_API_KEY_FILE`
- `AHANAFLOW_SERVICE_API_KEY_FILE`

## Rotate Sealed Policy Key

```bash
python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

Store the output in your secret manager and mount it as `AHANAFLOW_SEALED_POLICY_KEY_FILE`.

## Rebuild The Sealed Policy

Use the same new key to rewrite the encrypted policy blob used by UniversalStateServer / VectorStateServerV2.

Example policy content:

```json
{
  "api_key_hashes": ["<sha256-hash>"],
  "require_auth": true,
  "command_whitelist": ["PING", "GET", "SET", "DEL", "MGET", "MSET", "INCR"],
  "rate_limit_per_ip": 250,
  "rate_limit_per_key": 1000
}
```

Write the encrypted file using `write_sealed_policy_file()` from `backend.universal_server.security` in a short admin script or notebook, then mount the resulting file as `AHANAFLOW_SEALED_POLICY_FILE`.

## Runtime Mounting

Recommended mount targets:

- `/run/secrets/ahanaflow/stripe_secret_key`
- `/run/secrets/ahanaflow/stripe_webhook_secret`
- `/run/secrets/ahanaflow/signing_key`
- `/run/secrets/ahanaflow/admin_api_key`
- `/run/secrets/ahanaflow/service_api_key`
- `/run/secrets/ahanaflow/sealed_policy_key`
- `/run/secrets/ahanaflow/security.policy`
- `/run/secrets/ahanaflow/smtp_pass`

Do not write these secrets into `.env.production`, image layers, or the public repository.

## Publication Boundary

Before publishing or syncing to GitHub:

- Verify `backend/ahana_codec/` is not present in the public repo state.
- Verify `backend/compression_service/` is not present in the public repo state.
- Verify no live `.env.production` or key material exists in the tree.
- Publish proprietary codec binaries through the separate commercial distribution path only.