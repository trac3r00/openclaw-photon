import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { PROJECT_ID_ENV, PROJECT_SECRET_ENV } from "./config.js";

export interface ProjectCredentials {
  readonly projectId: string;
  readonly projectSecret: string;
}

export interface ProvisionedMetadata {
  readonly assignedLine: string;
  readonly operatorPhone: string;
  readonly projectId: string;
  readonly projectName: string;
  readonly updatedAt: string;
}

export interface StorageFilesystem {
  chmod(path: string, mode: number): Promise<void>;
  mkdir(path: string, options: { mode: number; recursive: true }): Promise<unknown>;
  readFile(path: string, encoding: "utf8"): Promise<string>;
  rename(from: string, to: string): Promise<void>;
  unlink(path: string): Promise<void>;
  writeFile(path: string, data: string, options: { flag: "wx"; mode: number }): Promise<void>;
}

const nodeFilesystem: StorageFilesystem = { chmod, mkdir, readFile, rename, unlink, writeFile };

function envValue(content: string, key: string): string | null {
  const line = content.split(/\r?\n/).find((entry) => entry.startsWith(`${key}=`));
  if (!line) return null;
  const raw = line.slice(key.length + 1).trim();
  if (raw.startsWith('"')) {
    try {
      const parsed: unknown = JSON.parse(raw);
      return typeof parsed === "string" && parsed ? parsed : null;
    } catch {
      return null;
    }
  }
  return raw || null;
}

function replaceEnv(content: string, values: Readonly<Record<string, string>>): string {
  const pending = new Map(Object.entries(values));
  const lines = content.split(/\r?\n/).filter((line) => {
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=/.exec(line);
    return !match || !pending.has(match[1] ?? "");
  });
  while (lines.at(-1) === "") lines.pop();
  for (const [key, value] of pending) lines.push(`${key}=${JSON.stringify(value)}`);
  return `${lines.join("\n")}\n`;
}

export async function readStoredText(fs: StorageFilesystem, path: string): Promise<string> {
  try {
    return await fs.readFile(path, "utf8");
  } catch (error) {
    const code = typeof error === "object" && error !== null ? Reflect.get(error, "code") : null;
    if (code === "ENOENT") return "";
    throw error;
  }
}

export async function atomicWriteStoredText(
  fs: StorageFilesystem,
  path: string,
  content: string,
): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    await fs.writeFile(temporary, content, { flag: "wx", mode: 0o600 });
    await fs.rename(temporary, path);
    await fs.chmod(path, 0o600);
  } catch (error) {
    await fs.unlink(temporary).catch(() => undefined);
    throw error;
  }
}

export async function loadProjectCredentials(
  home: string,
  filesystem: StorageFilesystem = nodeFilesystem,
): Promise<ProjectCredentials | null> {
  const content = await readStoredText(filesystem, join(home, ".openclaw", ".env"));
  const projectId = envValue(content, PROJECT_ID_ENV);
  const projectSecret = envValue(content, PROJECT_SECRET_ENV);
  return projectId && projectSecret ? { projectId, projectSecret } : null;
}

export async function persistProjectCredentials(params: {
  readonly filesystem?: StorageFilesystem;
  readonly home: string;
  readonly projectId: string;
  readonly projectSecret: string;
}): Promise<void> {
  const fs = params.filesystem ?? nodeFilesystem;
  const directory = join(params.home, ".openclaw");
  const path = join(directory, ".env");
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const next = replaceEnv(await readStoredText(fs, path), {
    [PROJECT_ID_ENV]: params.projectId,
    [PROJECT_SECRET_ENV]: params.projectSecret,
  });
  await atomicWriteStoredText(fs, path, next);
}

export async function persistProvisioningMetadata(params: {
  readonly filesystem?: StorageFilesystem;
  readonly home: string;
  readonly metadata: ProvisionedMetadata;
}): Promise<void> {
  const fs = params.filesystem ?? nodeFilesystem;
  const directory = join(params.home, ".openclaw", "photon");
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  await atomicWriteStoredText(
    fs,
    join(directory, "metadata.json"),
    `${JSON.stringify(params.metadata, null, 2)}\n`,
  );
}

export async function loadProvisioningMetadata(
  home: string,
  filesystem: StorageFilesystem = nodeFilesystem,
): Promise<ProvisionedMetadata | null> {
  const content = await readStoredText(
    filesystem,
    join(home, ".openclaw", "photon", "metadata.json"),
  );
  if (!content) return null;
  try {
    const value: unknown = JSON.parse(content);
    if (typeof value !== "object" || value === null) return null;
    const projectId = Reflect.get(value, "projectId");
    const assignedLine = Reflect.get(value, "assignedLine");
    const operatorPhone = Reflect.get(value, "operatorPhone");
    const projectName = Reflect.get(value, "projectName");
    const updatedAt = Reflect.get(value, "updatedAt");
    if (
      typeof projectId !== "string" || !projectId ||
      typeof assignedLine !== "string" || !assignedLine ||
      typeof operatorPhone !== "string" || !operatorPhone ||
      typeof projectName !== "string" || !projectName ||
      typeof updatedAt !== "string" || !updatedAt
    ) return null;
    return { projectId, assignedLine, operatorPhone, projectName, updatedAt };
  } catch {
    return null;
  }
}

export function defaultStorageFilesystem(): StorageFilesystem {
  return nodeFilesystem;
}

export async function persistProvisioning(params: {
  readonly filesystem?: StorageFilesystem;
  readonly home: string;
  readonly metadata: ProvisionedMetadata;
  readonly projectSecret: string;
}): Promise<void> {
  await persistProjectCredentials({
    filesystem: params.filesystem,
    home: params.home,
    projectId: params.metadata.projectId,
    projectSecret: params.projectSecret,
  });
  await persistProvisioningMetadata(params);
}
