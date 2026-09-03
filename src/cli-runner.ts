import { normalizeE164 } from "./config.js";
import type { OperatorIdentity } from "./oauth.js";
import type { PhotonProvisioningApi } from "./photon-api.js";

export interface CliDependencies {
  authenticate(notify: (message: string) => void): Promise<{
    identity: OperatorIdentity;
    token: string;
  }>;
  createApi(token: string): PhotonProvisioningApi;
  home: string;
  now(): Date;
  provision(phone: string, params: {
    api: PhotonProvisioningApi;
    home: string;
    identity: OperatorIdentity;
    now: () => Date;
  }): Promise<string>;
  stderr(message: string): void;
  stdout(message: string): void;
}

function phoneArgument(args: readonly string[]): string {
  if (args[0] !== "setup") {
    throw new Error("Usage: openclaw-photon setup --phone <E.164>");
  }
  const phoneIndex = args.indexOf("--phone");
  const phone = phoneIndex >= 0 ? args[phoneIndex + 1] : undefined;
  if (!phone || args.length !== 3) {
    throw new Error("Usage: openclaw-photon setup --phone <E.164>");
  }
  const normalized = normalizeE164(phone);
  if (!normalized) throw new Error("--phone must be an E.164 phone number");
  return normalized;
}

export async function runCli(args: readonly string[], deps: CliDependencies): Promise<void> {
  const phone = phoneArgument(args);
  const auth = await deps.authenticate(deps.stderr);
  const assignedLine = await deps.provision(phone, {
    api: deps.createApi(auth.token),
    home: deps.home,
    identity: auth.identity,
    now: deps.now,
  });
  deps.stdout(`${assignedLine}\n`);
}
