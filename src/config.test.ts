import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { describe, expect, it } from "vitest";
import {
  PhotonConfigSchema,
  PROJECT_ID_ENV,
  PROJECT_SECRET_ENV,
  resolveOutboundAllowFrom,
  resolvePhotonAccount,
} from "./config.js";

function config(photon: unknown): OpenClawConfig {
  return { channels: { photon } };
}

describe("Photon configuration", () => {
  it("uses only isolated OpenClaw credential environment variables", () => {
    const account = resolvePhotonAccount(config({ allowFrom: ["+14155550123"] }), {
      [PROJECT_ID_ENV]: "project",
      [PROJECT_SECRET_ENV]: "secret",
    });

    expect(account).toMatchObject({
      configured: true,
      projectId: "project",
      projectSecret: "secret",
      config: { dmPolicy: "allowlist", allowFrom: ["+14155550123"], telemetry: false },
    });
  });

  it("is deny-by-default and rejects non-E.164 allowlist entries", () => {
    expect(resolvePhotonAccount(config(undefined), {}).config).toMatchObject({
      dmPolicy: "allowlist",
      allowFrom: [],
    });
    expect(
      PhotonConfigSchema.safeParse({ allowFrom: ["4155550123"] }).success,
    ).toBe(false);
  });

  it("defaults outbound authorization to allowFrom and supports a narrower override", () => {
    const fallback = resolvePhotonAccount(config({ allowFrom: ["+14155550123"] }), {});
    const narrowed = resolvePhotonAccount(config({
      allowFrom: ["+14155550123"],
      outboundAllowFrom: ["+14155550124"],
    }), {});
    expect(resolveOutboundAllowFrom(fallback)).toEqual(["+14155550123"]);
    expect(resolveOutboundAllowFrom(narrowed)).toEqual(["+14155550124"]);
  });

  it("requires both isolated credentials", () => {
    const account = resolvePhotonAccount(config({}), {
      [PROJECT_ID_ENV]: "project",
    });
    expect(account.configured).toBe(false);
  });
});
