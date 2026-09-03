import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { photonPlugin } from "./channel.js";

const cfg: OpenClawConfig = { channels: { photon: { allowFrom: ["+14155550123"] } } };

describe("Photon status", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("reports configured runtime state without credential values", async () => {
    vi.stubEnv("OPENCLAW_PHOTON_PROJECT_ID", "project-status-secret-check");
    vi.stubEnv("OPENCLAW_PHOTON_PROJECT_SECRET", "credential-status-secret-check");
    const account = photonPlugin.config.resolveAccount(cfg);
    const build = photonPlugin.status?.buildAccountSnapshot;
    if (!build) {
      throw new Error("Photon status adapter is missing");
    }
    const snapshot = await build({
      account,
      cfg,
      runtime: {
        accountId: "default",
        running: true,
        connected: true,
        lifecycle: "ready",
      },
    });
    expect(snapshot).toMatchObject({
      accountId: "default",
      running: true,
      connected: true,
      lifecycle: "ready",
      credentialSource: "environment",
    });
    expect(JSON.stringify(snapshot)).not.toContain(account.projectId);
    expect(JSON.stringify(snapshot)).not.toContain(account.projectSecret);
  });
});
