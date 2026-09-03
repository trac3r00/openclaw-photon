# openclaw-photon

Private external OpenClaw channel plugin for Photon-backed iMessage DMs.

## Publication status

This repository is private while Photon Cloud's outbound iMessage endpoint is returning an
ambiguous gRPC `UNKNOWN` error for the provisioned line. The plugin reports that failure without
blind retries, but it will not be published to ClawHub until live outbound delivery is reliable
again. ClawHub publication also requires an operator workflow for resolving durable
`unknown_after_send` and `policy_blocked` journal entries, plus a deliberate review of the
`spectrum-ts` 8.0.0 compatibility pin.

## Provisioning

After installing the package locally, run:

```sh
openclaw-photon setup --phone +14155550123
```

The command authenticates with Photon's OAuth device flow and reuses a project only when its
project ID is pinned in the plugin metadata. Otherwise it creates a uniquely named installation-
owned project; it never adopts a project by name or rotates an unpinned project's secret. It then
registers the operator phone and writes only OpenClaw-owned state:

- credentials in `~/.openclaw/.env` (mode `0600`)
- non-secret project and assigned-line metadata in `~/.openclaw/photon/metadata.json`

OAuth instructions and errors go to stderr. Successful stdout contains only the assigned
iMessage line. The command never reads or writes Hermes state.

## Configuration

The setup command writes both required credentials in the Gateway environment file:

```sh
OPENCLAW_PHOTON_PROJECT_ID=...
OPENCLAW_PHOTON_PROJECT_SECRET=...
```

Configure a deny-by-default E.164 allowlist:

```json
{
  "channels": {
    "photon": {
      "enabled": true,
      "dmPolicy": "allowlist",
      "allowFrom": ["+14155550123"],
      "outboundAllowFrom": ["+14155550123"],
      "telemetry": false
    }
  }
}
```

`outboundAllowFrom` is optional and defaults to `allowFrom`; both inbound and outbound traffic
remain deny-by-default. Outbound delivery is additionally limited to 20 messages per destination
and 60 globally per minute. Inbound text is limited to 16,000 characters, 10 messages per sender
and 60 globally per minute, with at most 100 pending journal entries. The transport reader keeps
draining while agent turns run: accepted events are atomically journaled in a project-owned,
SHA-256-scoped file under `~/.openclaw/photon/projects/` (mode `0600`) before entering the
ordered processing queue, deduplicated across restarts, and replayed while retained. Raw,
unprocessed events expire after seven days. Staged unsent replies, interrupted `send_in_progress`
work, `unknown_after_send` quarantines, and `policy_blocked` records never expire automatically:
they consume the bounded 100-entry queue until an operator explicitly resolves/removes them, and a
full queue rejects new ingress rather than discarding uncertain delivery state. Replay scans enqueue
records in durable order without provider access; direct-space resolution occurs lazily at the head
of the processing queue. Unresolvable replay records persist bounded exponential backoff, and each
operator-action state transition is reported once per runtime generation. Completed-ID
deduplication is bounded by the smaller of seven days and the newest 1,000 completions. Dispatch
and ordered staged replies use renewable owner-fenced leases. The first staged reply durably marks
the agent turn dispatched, while no-reply turns are marked only after successful dispatch. Before
each provider send, the journal durably enters `send_in_progress`; successful sends atomically
checkpoint the reply, proven non-dispatch restores it to pending, and ambiguous outcomes or failed
checkpoints enter operator-action quarantine rather than being resent. The previous unscoped ingress
journal is migrated once to the active project.

Only one default account is supported. Group messages and non-text inbound content are ignored.
Transport startup, typing, iMessage read receipts, direct-space resolution, and pending scans are
bounded controls; typing and receipts remain best effort. The plugin publishes no synthetic
transport-activity timestamp and has no silence watchdog: a healthy quiet inbox is indistinguishable
from a half-open stream with Spectrum's API. Stream end/errors return control to OpenClaw's channel
lifecycle supervisor rather than triggering recursive plugin recovery.
Spectrum 8.0.0 exposes live events through the `app.messages` async iterable but no public history
or backfill API, so complete process downtime loses messages that never reach the plugin; the
journal closes the crash window only after an event reaches the process. Pinning Spectrum 8.0.0 is
current version debt. The current Photon provider outage is external to this plugin and cannot be
repaired by journal or transport recovery logic.

Build with `npm run build`; OpenClaw loads `dist/index.js` from the package manifest.
