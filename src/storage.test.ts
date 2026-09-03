import { describe, expect, it } from "vitest";
import { persistProvisioning } from "./storage.js";
import { MemoryFilesystem } from "./test-storage.js";

describe("Photon provisioning storage", () => {
  it("atomically preserves unrelated env values and writes secrets mode 0600", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set(
      "/home/test/.openclaw/.env",
      "KEEP=value\nOPENCLAW_PHOTON_PROJECT_ID=old\nOPENCLAW_PHOTON_PROJECT_SECRET=old-secret\n",
    );

    await persistProvisioning({
      filesystem,
      home: "/home/test",
      projectSecret: "new-secret",
      metadata: {
        assignedLine: "+14155550999",
        operatorPhone: "+14155550123",
        projectId: "project-1",
        projectName: "OpenClaw",
        updatedAt: "2026-09-02T12:00:00.000Z",
      },
    });

    const envPath = "/home/test/.openclaw/.env";
    expect(filesystem.files.get(envPath)).toBe(
      'KEEP=value\nOPENCLAW_PHOTON_PROJECT_ID="project-1"\n' +
      'OPENCLAW_PHOTON_PROJECT_SECRET="new-secret"\n',
    );
    expect(filesystem.modes.get(envPath)).toBe(0o600);
    const metadata = filesystem.files.get("/home/test/.openclaw/photon/metadata.json") ?? "";
    expect(metadata).toContain('"assignedLine": "+14155550999"');
    expect(metadata).not.toContain("new-secret");
    expect([...filesystem.files.keys()].some((path) => path.includes(".hermes"))).toBe(false);
  });
});
