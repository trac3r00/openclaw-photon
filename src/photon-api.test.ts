import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  createPhotonApi,
  PHOTON_DASHBOARD_URL,
  PHOTON_SPECTRUM_URL,
} from "./photon-api.js";
import type { HttpRequest } from "./oauth.js";

function sequence(bodies: readonly unknown[]): HttpRequest {
  const remaining = [...bodies];
  return vi.fn(async () => {
    const body = remaining.shift();
    if (body === undefined) throw new Error("unexpected request");
    return { status: 200, body };
  });
}

describe("Photon provisioning API", () => {
  it.each([
    [{ id: "project-1" }],
    [{ data: { id: "project-1" } }],
  ])("creates a project with the exact Dashboard contract", async (response) => {
    const request = sequence([response]);
    const api = createPhotonApi(request, "dashboard-token");
    await expect(api.createProject("OpenClaw")).resolves.toBe("project-1");
    expect(request).toHaveBeenCalledWith(`${PHOTON_DASHBOARD_URL}/api/projects`, {
      method: "POST",
      headers: {
        Authorization: "Bearer dashboard-token",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        name: "OpenClaw",
        location: "United States",
        template: false,
        observability: false,
      }),
    });
  });

  it("lists Dashboard projects and mints the current projectSecret shape", async () => {
    const request = sequence([
      { data: [{ id: "project-1", name: "OpenClaw" }] },
      { projectSecret: "minted-secret" },
    ]);
    const api = createPhotonApi(request, "dashboard-token");
    await expect(api.listProjects()).resolves.toEqual([{ id: "project-1", name: "OpenClaw" }]);
    expect(request).toHaveBeenNthCalledWith(
      1,
      `${PHOTON_DASHBOARD_URL}/api/projects`,
      { headers: { Authorization: "Bearer dashboard-token" } },
    );
    await expect(api.mintProjectSecret("project-1")).resolves.toBe("minted-secret");
    expect(request).toHaveBeenNthCalledWith(
      2,
      `${PHOTON_DASHBOARD_URL}/api/projects/project-1/regenerate-secret`,
      expect.objectContaining({ method: "POST", body: "{}" }),
    );
  });

  it("unwraps the Dashboard nested items project list shape", async () => {
    const request = sequence([
      { data: { items: [{ id: "project-1", name: "OpenClaw" }] } },
    ]);
    const api = createPhotonApi(request, "dashboard-token");

    await expect(api.listProjects()).resolves.toEqual([
      { id: "project-1", name: "OpenClaw" },
    ]);
  });

  it("lists and creates users on Spectrum with project Basic auth", async () => {
    const request = sequence([
      { users: [{ phoneNumber: "+14155550123", assignedPhoneNumber: "+14155550999" }] },
      { user: { phoneNumber: "+14155550124", assignedPhoneNumber: "+14155550888" } },
    ]);
    const api = createPhotonApi(request, "dashboard-token");
    const basic = `Basic ${Buffer.from("project-1:project-secret", "utf8").toString("base64")}`;
    await api.listUsers("project-1", "project-secret");
    await api.registerUser(
      "project-1",
      "project-secret",
      { email: "operator@example.com", name: "Op Erator" },
      "+14155550124",
    );
    expect(request).toHaveBeenNthCalledWith(
      1,
      `${PHOTON_SPECTRUM_URL}/projects/project-1/users/`,
      { headers: { Authorization: basic } },
    );
    expect(request).toHaveBeenNthCalledWith(
      2,
      `${PHOTON_SPECTRUM_URL}/projects/project-1/users/`,
      {
        method: "POST",
        headers: { Authorization: basic, "content-type": "application/json" },
        body: JSON.stringify({
          type: "shared",
          phoneNumber: "+14155550124",
          firstName: "Op",
          lastName: "Erator",
          email: "operator@example.com",
        }),
      },
    );
  });

  it("does not include secret response bodies in errors", async () => {
    const request: HttpRequest = vi.fn(async () => ({
      status: 500,
      body: { projectSecret: "must-not-leak" },
    }));
    await expect(createPhotonApi(request, "token").listProjects()).rejects.toThrow(
      "Photon API request failed (500)",
    );
    await expect(createPhotonApi(request, "token").listProjects()).rejects.not.toThrow(
      "must-not-leak",
    );
  });
});
