import { randomUUID } from "node:crypto";
import { normalizeE164 } from "./config.js";
import type { OperatorIdentity } from "./oauth.js";
import {
  PhotonRequestError,
  type PhotonProject,
  type PhotonProvisioningApi,
  type PhotonUser,
} from "./photon-api.js";
import {
  loadProjectCredentials,
  loadProvisioningMetadata,
  persistProjectCredentials,
  persistProvisioningMetadata,
  type ProvisionedMetadata,
  type StorageFilesystem,
} from "./storage.js";


export interface ProvisionDependencies {
  readonly api: PhotonProvisioningApi;
  readonly filesystem?: StorageFilesystem;
  readonly home: string;
  readonly identity: OperatorIdentity;
  readonly installationId?: () => string;
  readonly now: () => Date;
}

async function resolveProject(deps: ProvisionDependencies): Promise<PhotonProject> {
  const projects = await deps.api.listProjects();
  const metadata = await loadProvisioningMetadata(deps.home, deps.filesystem);
  if (metadata) {
    const pinned = projects.find((project) => project.id === metadata.projectId);
    if (!pinned) {
      throw new Error(`The pinned Photon project ${metadata.projectId} is unavailable; refusing secret rotation`);
    }
    return pinned;
  }
  const name = `OpenClaw ${deps.now().toISOString()} ${
    (deps.installationId ?? randomUUID)()
  }`;
  return { id: await deps.api.createProject(name), name };
}

async function credentialsAndUsers(
  project: PhotonProject,
  deps: ProvisionDependencies,
): Promise<{ projectSecret: string; users: PhotonUser[] }> {
  const local = await loadProjectCredentials(deps.home, deps.filesystem);
  if (local?.projectId === project.id) {
    try {
      return {
        projectSecret: local.projectSecret,
        users: await deps.api.listUsers(project.id, local.projectSecret),
      };
    } catch (error) {
      if (!(error instanceof PhotonRequestError) || ![401, 403].includes(error.status)) {
        throw error;
      }
    }
  }
  const projectSecret = await deps.api.mintProjectSecret(project.id);
  await persistProjectCredentials({
    filesystem: deps.filesystem,
    home: deps.home,
    projectId: project.id,
    projectSecret,
  });
  return { projectSecret, users: await deps.api.listUsers(project.id, projectSecret) };
}

export async function provisionPhoton(
  phoneInput: string,
  deps: ProvisionDependencies,
): Promise<string> {
  const phone = normalizeE164(phoneInput);
  if (!phone) throw new Error("--phone must be an E.164 phone number");
  const project = await resolveProject(deps);
  const { projectSecret, users } = await credentialsAndUsers(project, deps);
  const existing = users.find((user) => user.phoneNumber === phone);
  const user = existing ?? await deps.api.registerUser(
    project.id,
    projectSecret,
    deps.identity,
    phone,
  );
  const metadata: ProvisionedMetadata = {
    assignedLine: user.assignedPhoneNumber,
    operatorPhone: phone,
    projectId: project.id,
    projectName: project.name,
    updatedAt: deps.now().toISOString(),
  };
  await persistProvisioningMetadata({
    filesystem: deps.filesystem,
    home: deps.home,
    metadata,
  });
  return user.assignedPhoneNumber;
}
