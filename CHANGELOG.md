# Changelog

## 0.1.2 - 2026-09-03

- Republish the verified 0.1.1 artifact with corrected full source-commit provenance metadata.
- Withdraw the ClawHub 0.1.1 listing whose source commit used an invalid expansion of the correct short SHA.

## 0.1.1 - 2026-09-03

- Rename internal Photon provisioning methods to avoid ClawHub Inspector registrar-name collisions.
- Preserve the verified provisioning behavior and 131-test safety baseline.

## 0.1.0 - 2026-09-03

- Add an isolated Photon-backed iMessage channel for OpenClaw.
- Add OAuth device-flow provisioning for a dedicated Photon project and line.
- Add deny-by-default inbound and outbound E.164 allowlists.
- Add durable project-scoped ingress journaling, cross-process leases, replay ordering, backoff, and operator-action quarantine states.
- Add conservative delivery classification that never retries ambiguous sends.
- Add typing indicators, read receipts, exact session routing, bounded operations, and lifecycle fencing.
- Add 131 automated tests and zero-vulnerability dependency validation.

Known beta limitation: Photon Cloud returned an ambiguous gRPC `UNKNOWN` error during the latest outbound provider check. The plugin reports and quarantines that outcome without retrying it.
