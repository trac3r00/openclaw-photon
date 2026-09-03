import { describe, expect, it, vi } from "vitest";
import {
  PhotonRequestError,
  type PhotonProject,
  type PhotonProvisioningApi,
} from "./photon-api.js";
import { provisionPhoton } from "./provision.js";
import { MemoryFilesystem } from "./test-storage.js";

const project = (id = "project-1"): PhotonProject => ({ id, name: "OpenClaw" });

function api(projects: PhotonProject[]): PhotonProvisioningApi {
  return {
    listProjects: vi.fn(async () => projects),
    createProject: vi.fn(async () => "created-project"),
    mintProjectSecret: vi.fn(async () => "minted-secret"),
    listUsers: vi.fn(async () => []),
    createUser: vi.fn(async (_projectId, _secret, _identity, phone) => ({
      phoneNumber: phone,
      assignedPhoneNumber: "+14155550999",
    })),
  };
}

const identity = { email: "operator@example.com", name: "Op Erator" };
const params = {
  home: "/home/test",
  identity,
  installationId: () => "installation-7",
  now: () => new Date("2026-09-02T12:00:00Z"),
};

function pinProject(filesystem: MemoryFilesystem, projectId = "project-1"): void {
  filesystem.files.set("/home/test/.openclaw/photon/metadata.json", JSON.stringify({
    assignedLine: "+14155550999",
    operatorPhone: "+14155550123",
    projectId,
    projectName: "OpenClaw owned",
    updatedAt: "2026-09-01T12:00:00.000Z",
  }));
}

describe("Photon provisioning", () => {
  it("reuses locally persisted credentials after Spectrum validates them", async () => {
    const photon = api([project()]);
    vi.mocked(photon.listUsers).mockResolvedValueOnce([{
      phoneNumber: "+14155550123",
      assignedPhoneNumber: "+14155550999",
    }]);
    const filesystem = new MemoryFilesystem();
    pinProject(filesystem);
    filesystem.files.set(
      "/home/test/.openclaw/.env",
      'OPENCLAW_PHOTON_PROJECT_ID="project-1"\n' +
      'OPENCLAW_PHOTON_PROJECT_SECRET="local-secret"\n',
    );

    await expect(provisionPhoton("+14155550123", {
      ...params,
      api: photon,
      filesystem,
    })).resolves.toBe("+14155550999");
    expect(photon.listUsers).toHaveBeenCalledWith("project-1", "local-secret");
    expect(photon.mintProjectSecret).not.toHaveBeenCalled();
    expect(photon.createUser).not.toHaveBeenCalled();
  });

  it("mints once for invalid local credentials and persists before user registration", async () => {
    const filesystem = new MemoryFilesystem();
    pinProject(filesystem);
    filesystem.files.set(
      "/home/test/.openclaw/.env",
      'OPENCLAW_PHOTON_PROJECT_ID="project-1"\n' +
      'OPENCLAW_PHOTON_PROJECT_SECRET="invalid-secret"\n',
    );
    const photon = api([project()]);
    vi.mocked(photon.listUsers)
      .mockRejectedValueOnce(new PhotonRequestError("invalid credentials", 401))
      .mockResolvedValueOnce([]);
    vi.mocked(photon.createUser).mockImplementationOnce(async (_id, secret, _identity, phone) => {
      expect(secret).toBe("minted-secret");
      expect(filesystem.files.get("/home/test/.openclaw/.env")).toContain("minted-secret");
      return { phoneNumber: phone, assignedPhoneNumber: "+14155550999" };
    });

    await provisionPhoton("+14155550123", { ...params, api: photon, filesystem });
    expect(photon.mintProjectSecret).toHaveBeenCalledOnce();
    expect(photon.listUsers).toHaveBeenLastCalledWith("project-1", "minted-secret");
  });

  it("does not rotate credentials after a transient Spectrum failure", async () => {
    const photon = api([project()]);
    vi.mocked(photon.listUsers).mockRejectedValueOnce(
      new PhotonRequestError("temporary Spectrum failure", 500),
    );
    const filesystem = new MemoryFilesystem();
    pinProject(filesystem);
    filesystem.files.set(
      "/home/test/.openclaw/.env",
      'OPENCLAW_PHOTON_PROJECT_ID="project-1"\n' +
      'OPENCLAW_PHOTON_PROJECT_SECRET="local-secret"\n',
    );
    await expect(provisionPhoton("+14155550123", {
      ...params,
      api: photon,
      filesystem,
    })).rejects.toThrow("temporary Spectrum failure");
    expect(photon.mintProjectSecret).not.toHaveBeenCalled();
  });

  it("creates a uniquely named project instead of adopting an unpinned name match", async () => {
    const filesystem = new MemoryFilesystem();
    const photon = api([project("someone-elses-project")]);
    await provisionPhoton("+14155550123", { ...params, api: photon, filesystem });
    expect(photon.createProject).toHaveBeenCalledWith(
      "OpenClaw 2026-09-02T12:00:00.000Z installation-7",
    );
    expect(photon.mintProjectSecret).toHaveBeenCalledWith("created-project");
    expect(photon.mintProjectSecret).not.toHaveBeenCalledWith("someone-elses-project");
  });

  it("reuses only the exact project id pinned in provisioning metadata", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set("/home/test/.openclaw/photon/metadata.json", JSON.stringify({
      assignedLine: "+14155550999",
      operatorPhone: "+14155550123",
      projectId: "project-2",
      projectName: "OpenClaw owned",
      updatedAt: "2026-09-01T12:00:00.000Z",
    }));
    filesystem.files.set(
      "/home/test/.openclaw/.env",
      'OPENCLAW_PHOTON_PROJECT_ID="project-2"\nOPENCLAW_PHOTON_PROJECT_SECRET="local-secret"\n',
    );
    const photon = api([project(), { id: "project-2", name: "renamed" }]);
    await provisionPhoton("+14155550123", { ...params, api: photon, filesystem });
    expect(photon.listUsers).toHaveBeenCalledWith("project-2", "local-secret");
    expect(photon.createProject).not.toHaveBeenCalled();
  });

  it("refuses to rotate when pinned ownership cannot be confirmed", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set("/home/test/.openclaw/photon/metadata.json", JSON.stringify({
      assignedLine: "+14155550999",
      operatorPhone: "+14155550123",
      projectId: "missing-project",
      projectName: "OpenClaw",
      updatedAt: "2026-09-01T12:00:00.000Z",
    }));
    const photon = api([project()]);
    await expect(provisionPhoton("+14155550123", { ...params, api: photon, filesystem }))
      .rejects.toThrow("pinned Photon project");
    expect(photon.mintProjectSecret).not.toHaveBeenCalled();
  });
});
