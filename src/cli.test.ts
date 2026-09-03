import { describe, expect, it, vi } from "vitest";
import { runCli, type CliDependencies } from "./cli-runner.js";
import type { PhotonProvisioningApi } from "./photon-api.js";

const api: PhotonProvisioningApi = {
  listProjects: vi.fn(),
  createProject: vi.fn(),
  mintProjectSecret: vi.fn(),
  listUsers: vi.fn(),
  registerUser: vi.fn(),
};

function dependencies(): CliDependencies {
  return {
    authenticate: vi.fn(async (notify) => {
      notify("Authorize with code CODE");
      return {
        identity: { email: "operator@example.com", name: "Operator Name" },
        token: "token",
      };
    }),
    createApi: vi.fn(() => api),
    home: "/home/test",
    now: () => new Date("2026-09-02T12:00:00Z"),
    provision: vi.fn(async () => "+14155550999"),
    stderr: vi.fn(),
    stdout: vi.fn(),
  };
}

describe("openclaw-photon CLI", () => {
  it("prints only the assigned iMessage line to stdout", async () => {
    const deps = dependencies();
    await runCli(["setup", "--phone", "+14155550123"], deps);
    expect(deps.stdout).toHaveBeenCalledOnce();
    expect(deps.stdout).toHaveBeenCalledWith("+14155550999\n");
    expect(deps.stderr).toHaveBeenCalledWith("Authorize with code CODE");
  });

  it.each([
    ["incomplete arguments", ["setup"]],
    ["a non-E.164 phone", ["setup", "--phone", "4155550123"]],
  ])("rejects %s before authentication", async (_label, args) => {
    const deps = dependencies();
    await expect(runCli(args, deps)).rejects.toThrow();
    expect(deps.authenticate).not.toHaveBeenCalled();
  });
});
