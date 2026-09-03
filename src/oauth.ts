import { sanitizeError } from "./security.js";

export const PHOTON_API_URL = "https://app.photon.codes";
export const PHOTON_OAUTH_CLIENT_ID = "photon-cli";
export const PHOTON_OAUTH_SCOPE = "openid profile email";

export interface HttpResponse {
  readonly body: unknown;
  readonly headers?: Readonly<Record<string, string>>;
  readonly status: number;
}

export type HttpRequest = (url: string, init?: RequestInit) => Promise<HttpResponse>;

export interface OperatorIdentity {
  readonly email: string;
  readonly name: string;
}

export interface DeviceFlowDependencies {
  readonly notify: (message: string) => void;
  readonly request: HttpRequest;
  readonly sleep: (milliseconds: number) => Promise<void>;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? Object.fromEntries(Object.entries(value))
    : null;
}

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`Photon OAuth response omitted ${field}`);
  }
  return value.trim();
}

function bearer(token: string): HeadersInit {
  return { Authorization: `Bearer ${token}` };
}

export async function fetchPhoton(url: string, init?: RequestInit): Promise<HttpResponse> {
  const response = await fetch(url, init);
  const body: unknown = await response.json().catch(() => null);
  return { body, headers: Object.fromEntries(response.headers.entries()), status: response.status };
}

function headerValue(headers: HttpResponse["headers"], name: string): string | undefined {
  if (!headers) return undefined;
  const match = Object.entries(headers).find(([key]) => key.toLowerCase() === name.toLowerCase());
  return match?.[1];
}

function cleanToken(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const token = value.trim().replace(/^Bearer\s+/i, "");
  return token || null;
}

function tokenCandidates(response: HttpResponse): string[] {
  const body = record(response.body);
  const data = record(body?.data);
  const session = record(body?.session);
  const dataSession = record(data?.session);
  const raw = [
    body?.access_token,
    body?.accessToken,
    session?.access_token,
    session?.accessToken,
    data?.access_token,
    data?.accessToken,
    dataSession?.access_token,
    dataSession?.accessToken,
    headerValue(response.headers, "set-auth-token"),
  ];
  const tokens = raw.map(cleanToken).filter((token): token is string => token !== null);
  return [...new Set(tokens)];
}

function identityFromSession(body: unknown): OperatorIdentity {
  const root = record(body);
  const data = record(root?.data);
  const session = record(root?.session);
  const user = record(root?.user) ?? record(data?.user) ?? record(session?.user);
  if (!user) throw new Error("Photon OAuth session omitted user");
  return {
    email: requiredString(user.email, "user.email"),
    name: requiredString(user.name, "user.name"),
  };
}

async function validateToken(
  request: HttpRequest,
  token: string,
): Promise<OperatorIdentity> {
  const headers = bearer(token);
  const session = await request(`${PHOTON_API_URL}/api/auth/get-session`, { headers });
  if (session.status < 200 || session.status >= 300) {
    throw new Error(`Photon OAuth session validation failed (${session.status})`);
  }
  const projects = await request(`${PHOTON_API_URL}/api/projects`, { headers });
  if (projects.status < 200 || projects.status >= 300) {
    throw new Error(`Photon OAuth project validation failed (${projects.status})`);
  }
  return identityFromSession(session.body);
}

async function validateCandidates(
  request: HttpRequest,
  candidates: readonly string[],
): Promise<{ identity: OperatorIdentity; token: string }> {
  let lastError: Error | undefined;
  for (const token of candidates) {
    try {
      return { token, identity: await validateToken(request, token) };
    } catch (error) {
      lastError = new Error(sanitizeError(error, [token]));
    }
  }
  throw lastError ?? new Error("Photon OAuth token response contained no usable token");
}

export async function authenticateWithDeviceFlow(
  deps: DeviceFlowDependencies,
): Promise<{ identity: OperatorIdentity; token: string }> {
  const codeResponse = await deps.request(`${PHOTON_API_URL}/api/auth/device/code`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ client_id: PHOTON_OAUTH_CLIENT_ID, scope: PHOTON_OAUTH_SCOPE }),
  });
  if (codeResponse.status !== 200) {
    throw new Error(`Photon OAuth device authorization failed (${codeResponse.status})`);
  }
  const code = record(codeResponse.body);
  if (!code) throw new Error("Photon OAuth returned an invalid device authorization response");
  const deviceCode = requiredString(code.device_code, "device_code");
  const verificationUri = requiredString(code.verification_uri, "verification_uri");
  const userCode = requiredString(code.user_code, "user_code");
  let pollSeconds = typeof code.interval === "number" && code.interval > 0 ? code.interval : 5;
  const expiresIn = typeof code.expires_in === "number" && code.expires_in > 0 ? code.expires_in : 1800;
  deps.notify(`Authorize Photon at ${verificationUri} with code ${userCode}`);

  let elapsed = 0;
  while (elapsed < expiresIn) {
    await deps.sleep(pollSeconds * 1000);
    elapsed += pollSeconds;
    const response = await deps.request(`${PHOTON_API_URL}/api/auth/device/token`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
        device_code: deviceCode,
        client_id: PHOTON_OAUTH_CLIENT_ID,
      }),
    });
    if (response.status === 200) {
      return await validateCandidates(deps.request, tokenCandidates(response));
    }
    const error = record(response.body)?.error;
    if (error === "authorization_pending") continue;
    if (error === "slow_down" || response.status === 429) {
      pollSeconds += error === "slow_down" ? 5 : 10;
      continue;
    }
    throw new Error(`Photon OAuth device flow failed (${String(error ?? response.status)})`);
  }
  throw new Error("Photon OAuth device authorization expired");
}
