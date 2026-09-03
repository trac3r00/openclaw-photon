import { describe, expect, it, vi } from "vitest";
import {
  authenticateWithDeviceFlow,
  PHOTON_API_URL,
  PHOTON_OAUTH_CLIENT_ID,
  PHOTON_OAUTH_SCOPE,
  type HttpRequest,
  type HttpResponse,
} from "./oauth.js";

function requestSequence(responses: HttpResponse[]): HttpRequest {
  return vi.fn(async () => {
    const response = responses.shift();
    if (!response) throw new Error("unexpected request");
    return response;
  });
}

const deviceCode = {
  status: 200,
  body: {
    device_code: "device",
    user_code: "ABCD-EFGH",
    verification_uri: "https://app.photon.codes/device",
    interval: 1,
    expires_in: 30,
  },
};
const session = {
  status: 200,
  body: { user: { email: "operator@example.com", name: "Op Erator" } },
};
const projects = { status: 200, body: [] };

describe("Photon OAuth", () => {
  it.each([
    [{ access_token: "token-a" }, undefined, "token-a"],
    [{ accessToken: "token-b" }, undefined, "token-b"],
    [{ data: { access_token: "token-c" } }, undefined, "token-c"],
    [{ session: { accessToken: "token-d" } }, undefined, "token-d"],
    [{ data: { session: { access_token: "token-f" } } }, undefined, "token-f"],
    [{}, { "set-auth-token": "Bearer token-e" }, "token-e"],
  ] as const)("accepts supported token response shapes", async (body, headers, expected) => {
    const request = requestSequence([
      deviceCode,
      { status: 200, body, ...(headers ? { headers } : {}) },
      session,
      projects,
    ]);
    await expect(authenticateWithDeviceFlow({
      request,
      sleep: async () => undefined,
      notify: () => undefined,
    })).resolves.toMatchObject({ token: expected });
    expect(request).toHaveBeenNthCalledWith(
      3,
      `${PHOTON_API_URL}/api/auth/get-session`,
      { headers: { Authorization: `Bearer ${expected}` } },
    );
    expect(request).toHaveBeenNthCalledWith(
      4,
      `${PHOTON_API_URL}/api/projects`,
      { headers: { Authorization: `Bearer ${expected}` } },
    );
  });

  it("uses the verified photon-cli device request contract", async () => {
    const request = requestSequence([
      deviceCode,
      { status: 200, body: { access_token: "token" } },
      session,
      projects,
    ]);
    await authenticateWithDeviceFlow({ request, sleep: async () => undefined, notify: vi.fn() });
    expect(request).toHaveBeenNthCalledWith(
      1,
      `${PHOTON_API_URL}/api/auth/device/code`,
      expect.objectContaining({
        body: JSON.stringify({ client_id: PHOTON_OAUTH_CLIENT_ID, scope: PHOTON_OAUTH_SCOPE }),
      }),
    );
  });

  it("rejects tokens that pass session validation but fail project validation", async () => {
    const request = requestSequence([
      deviceCode,
      { status: 200, body: { access_token: "not-project-valid" } },
      session,
      { status: 403, body: { error: "forbidden", secret: "must-not-leak" } },
    ]);
    await expect(authenticateWithDeviceFlow({
      request,
      sleep: async () => undefined,
      notify: () => undefined,
    })).rejects.toThrow("project validation failed (403)");
  });
});
