# Release Note — Deploy Helper Rollout (2026-04-17)

## Scope

This release packages the deployment-helper workflow and the backend-managed
proprietary artifact distribution path for AhanaFlow.

## Included

- Admin `Deploy Kit` UI for local key entry and generation of:
  - `.env.production`
  - per-secret file payloads under `deploy/secrets/ahanaflow/`
  - Kubernetes `--from-file` secret creation commands
- Public `/deploy-helper` page for the same formatting flow without exposing the
  protected admin dashboard
- One-click per-file secret downloads from the helper UI
- Backend-signed proprietary artifact manifests with:
  - short-lived download grants
  - per-customer fingerprints
  - per-customer PUZZLE-AUTH unlock keys
- Registry audit trail for proprietary artifact issuance

## Validation

- Focused deploy/security suite: `20 passed`
- Live FastAPI verification:
  - `/deploy-helper` returned `200`
  - `/admin` returned `200`
  - `POST /admin/login` returned `200`
  - authenticated `GET /admin/summary` returned `200`
- Live CustomerDB-backed verification:
  - CustomerDB launched on `127.0.0.1:9633`
  - seeded runtime customer record visible through admin summary as `db_leads: 1`

## Operational Notes

- The deploy helper keeps key material in the browser session and formats output
  locally; it does not submit those secrets back to the backend.
- Runtime secrets must still be stored outside the repository and mounted through
  the documented `*_FILE` paths.
- The admin summary combines two data sources:
  - JSON API-key registry for entitled customers and issued keys
  - CustomerDB for lead and CRM-style customer records

## Recommended Tag

- `deploy-helper-2026-04-17`