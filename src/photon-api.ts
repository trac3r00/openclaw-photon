import { Buffer } from "node:buffer";
import type { HttpRequest } from "./oauth.js";

export const PHOTON_DASHBOARD_URL = "https://app.photon.codes";
export const PHOTON_SPECTRUM_URL = "https://spectrum.photon.codes";

export class PhotonRequestError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

export interface PhotonProject {
  readonly id: string;
  readonly name: string;
}

export interface PhotonUser {
  readonly assignedPhoneNumber: string;
  readonly phoneNumber: string;
}

export interface PhotonProvisioningApi {
  createProject(name: string): Promise<string>;
  listProjects(): Promise<PhotonProject[]>;
  listUsers(projectId: string, projectSecret: string): Promise<PhotonUser[]>;
  mintProjectSecret(projectId: string): Promise<string>;
  registerUser(
    projectId: string,
    projectSecret: string,
    identity: { email: string; name: string },
    phone: string,
  ): Promise<PhotonUser>;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? Object.fromEntries(Object.entries(value))
    : null;
}

function requiredString(value: unknown, context: string): string {
  if (typeof value !== "string" || !value) throw new Error(`Photon API omitted ${context}`);
  return value;
}

function unwrapList(value: unknown): unknown[] {
  if (Array.isArray(value)) return value;
  const root = record(value);
  if (!root) throw new Error("Photon API returned an invalid list");
  for (const key of ["data", "projects", "users", "lines", "items"] as const) {
    const candidate = root[key];
    if (Array.isArray(candidate)) return candidate;
    const nested = record(candidate);
    if (!nested) continue;
    for (const nestedKey of ["projects", "users", "lines", "items"] as const) {
      const nestedCandidate = nested[nestedKey];
      if (Array.isArray(nestedCandidate)) return nestedCandidate;
    }
  }
  throw new Error("Photon API returned an invalid list");
}

function parseProject(value: unknown): PhotonProject {
  const item = record(value);
  if (!item) throw new Error("Photon API returned an invalid project");
  return { id: requiredString(item.id, "project id"), name: requiredString(item.name, "project name") };
}

function parseUser(value: unknown): PhotonUser {
  const item = record(value);
  if (!item) throw new Error("Photon API returned an invalid Spectrum user");
  return {
    assignedPhoneNumber: requiredString(item.assignedPhoneNumber, "assigned iMessage line"),
    phoneNumber: requiredString(item.phoneNumber, "operator phone"),
  };
}

function splitName(name: string): { firstName?: string; lastName?: string } {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  const firstName = parts.shift();
  const lastName = parts.join(" ") || undefined;
  return { ...(firstName ? { firstName } : {}), ...(lastName ? { lastName } : {}) };
}

export function createPhotonApi(request: HttpRequest, token: string): PhotonProvisioningApi {
  const dashboard = async (path: string, init?: RequestInit): Promise<unknown> => {
    const response = await request(`${PHOTON_DASHBOARD_URL}${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(init?.body ? { "content-type": "application/json" } : {}),
      },
    });
    if (response.status < 200 || response.status >= 300) {
      throw new PhotonRequestError(
        `Photon API request failed (${response.status}) at ${path}`,
        response.status,
      );
    }
    return response.body;
  };
  const spectrum = async (
    projectId: string,
    projectSecret: string,
    init?: RequestInit,
  ): Promise<unknown> => {
    const basic = Buffer.from(`${projectId}:${projectSecret}`, "utf8").toString("base64");
    const url = `${PHOTON_SPECTRUM_URL}/projects/${encodeURIComponent(projectId)}/users/`;
    const response = await request(url, {
      ...init,
      headers: {
        Authorization: `Basic ${basic}`,
        ...(init?.body ? { "content-type": "application/json" } : {}),
      },
    });
    if (response.status < 200 || response.status >= 300) {
      throw new PhotonRequestError(
        `Photon Spectrum request failed (${response.status})`,
        response.status,
      );
    }
    return response.body;
  };
  return {
    listProjects: async () => unwrapList(await dashboard("/api/projects")).map(parseProject),
    createProject: async (name) => {
      const body = record(await dashboard("/api/projects", {
        method: "POST",
        body: JSON.stringify({
          name,
          location: "United States",
          template: false,
          observability: false,
        }),
      }));
      const data = record(body?.data);
      return requiredString(body?.id ?? data?.id, "created project id");
    },
    mintProjectSecret: async (projectId) => {
      const body = record(await dashboard(
        `/api/projects/${encodeURIComponent(projectId)}/regenerate-secret`,
        { method: "POST", body: "{}" },
      ));
      const data = record(body?.data);
      return requiredString(body?.projectSecret ?? data?.projectSecret, "project secret");
    },
    listUsers: async (projectId, projectSecret) =>
      unwrapList(await spectrum(projectId, projectSecret)).map(parseUser),
    registerUser: async (projectId, projectSecret, identity, phone) => {
      const body = record(await spectrum(projectId, projectSecret, {
        method: "POST",
        body: JSON.stringify({
          type: "shared",
          phoneNumber: phone,
          ...splitName(identity.name),
          ...(identity.email ? { email: identity.email } : {}),
        }),
      }));
      return parseUser(body?.user ?? body?.data ?? body);
    },
  };
}
