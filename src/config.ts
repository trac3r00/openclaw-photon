import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { z } from "zod";

export const PROJECT_ID_ENV = "OPENCLAW_PHOTON_PROJECT_ID";
export const PROJECT_SECRET_ENV = "OPENCLAW_PHOTON_PROJECT_SECRET";
export const E164_PATTERN = /^\+[1-9]\d{1,14}$/;

const E164Schema = z.string().trim().regex(E164_PATTERN, "must be an E.164 phone number");

export const PhotonConfigSchema = z.object({
  enabled: z.boolean().optional(),
  allowFrom: z.array(E164Schema).default([]),
  outboundAllowFrom: z.array(E164Schema).optional(),
  dmPolicy: z.enum(["allowlist", "disabled"]).default("allowlist"),
  telemetry: z.boolean().default(false),
});

export type PhotonConfig = z.infer<typeof PhotonConfigSchema>;

export interface ResolvedPhotonAccount {
  accountId: "default";
  enabled: boolean;
  configured: boolean;
  projectId: string;
  projectSecret: string;
  config: PhotonConfig;
}

function channelConfig(cfg: OpenClawConfig): unknown {
  const channels: unknown = cfg.channels;
  if (typeof channels !== "object" || channels === null) {
    return undefined;
  }
  return Reflect.get(channels, "photon");
}

export function normalizeE164(value: string): string | null {
  const normalized = value.trim();
  return E164_PATTERN.test(normalized) ? normalized : null;
}

export function resolveOutboundAllowFrom(account: ResolvedPhotonAccount): readonly string[] {
  return account.config.outboundAllowFrom ?? account.config.allowFrom;
}

export function resolvePhotonAccount(
  cfg: OpenClawConfig,
  env: NodeJS.ProcessEnv = process.env,
): ResolvedPhotonAccount {
  const parsed = PhotonConfigSchema.safeParse(channelConfig(cfg));
  const config = parsed.success
    ? parsed.data
    : { enabled: true, allowFrom: [], dmPolicy: "allowlist" as const, telemetry: false };
  const projectId = env[PROJECT_ID_ENV]?.trim() ?? "";
  const projectSecret = env[PROJECT_SECRET_ENV]?.trim() ?? "";
  return {
    accountId: "default",
    enabled: config.enabled !== false,
    configured: projectId.length > 0 && projectSecret.length > 0,
    projectId,
    projectSecret,
    config,
  };
}
