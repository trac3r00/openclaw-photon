# Security policy

## Supported versions

Security fixes are applied to the latest published `0.1.x` release.

## Reporting a vulnerability

Please use GitHub private vulnerability reporting for this repository. Do not include Photon project secrets, OAuth tokens, phone numbers, message contents, or OpenClaw environment files in a public issue.

The plugin treats delivery outcomes conservatively: ambiguous provider failures are quarantined rather than retried, and only explicit pre-dispatch failures are eligible for retry. Reports that demonstrate a bypass of this boundary are especially valuable.
