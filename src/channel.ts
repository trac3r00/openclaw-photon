import {
  buildChannelConfigSchema,
  buildChannelOutboundSessionRoute,
  createChatChannelPlugin,
  stripChannelTargetPrefix,
  type ChannelPlugin,
  type OpenClawConfig,
} from "openclaw/plugin-sdk/channel-core";
import { createChannelMessageAdapterFromOutbound } from "openclaw/plugin-sdk/channel-outbound";
import { chunkMarkdownTextWithMode } from "openclaw/plugin-sdk/reply-chunking";
import { attachChannelToResult } from "openclaw/plugin-sdk/channel-send-result";
import { buildPassiveChannelStatusSummary } from "openclaw/plugin-sdk/extension-shared";
import {
  createComputedAccountStatusAdapter,
  createDefaultChannelRuntimeState,
} from "openclaw/plugin-sdk/status-helpers";
import type { ChannelOutboundAdapter } from "openclaw/plugin-sdk/channel-contract";
import { missingTargetError } from "openclaw/plugin-sdk/channel-feedback";
import { settlePhotonOperation } from "./bounded-operation.js";
import type { ResolvedPhotonAccount } from "./config.js";
import {
  normalizeE164,
  PhotonConfigSchema,
  resolveOutboundAllowFrom,
  resolvePhotonAccount,
} from "./config.js";
import {
  getActivePhotonTransport,
  getActivePhotonTransportIdentity,
  notePhotonOutbound,
  notePhotonOutboundFailure,
  runPhotonAccount,
  stopPhotonAccount,
} from "./runtime.js";
import {
  enforcePhotonOutboundRate,
  type OutboundRateGate,
} from "./outbound-rate.js";
import { notDispatchedBoundaryError, sanitizeOutboundText } from "./security.js";
import { sendToSpace, type PhotonTransport } from "./transport.js";
import { photonTyping } from "./typing.js";

export async function sendPhotonOutbound(
  transport: PhotonTransport,
  to: string,
  body: string,
  allowFrom: readonly string[],
  rateGate?: OutboundRateGate,
  now?: number,
  isCurrent: () => boolean = () => true,
): Promise<{ messageId: string; to: string }> {
  const target = normalizeE164(to);
  if (!target) {
    throw new Error("Photon target must be an E.164 phone number");
  }
  if (!allowFrom.includes(target)) {
    throw new Error("Photon target is not in the outbound allowlist");
  }
  const safeBody = sanitizeOutboundText(body);
  if (!safeBody) throw new Error("Photon outbound text is empty after sanitization");
  enforcePhotonOutboundRate(target, rateGate, now);
  let space;
  try {
    space = await settlePhotonOperation(
      transport.resolveDirectSpace(target),
      "direct-space resolution",
    );
  } catch (error) {
    throw notDispatchedBoundaryError(error);
  }
  if (!isCurrent()) {
    throw notDispatchedBoundaryError(new Error("Photon transport is not running"));
  }
  const messageId = await sendToSpace(space, safeBody, "markdown");
  return { to: target, messageId };
}

const outbound: ChannelOutboundAdapter = {
  deliveryMode: "direct",
  textChunkLimit: 4000,
  chunkerMode: "markdown",
  chunker: (text, limit) => chunkMarkdownTextWithMode(text, limit, "length"),
  sanitizeText: ({ text }) => sanitizeOutboundText(text),
  targetsMatchForReplySuppression: ({ originTarget, targetKey }) =>
    originTarget.replace(/^photon:/, "") === targetKey.replace(/^photon:/, ""),
  resolveTarget: ({ cfg, to, allowFrom }) => {
    const normalized = normalizeE164(to ?? "");
    const allowed = cfg
      ? resolveOutboundAllowFrom(resolvePhotonAccount(cfg))
      : (allowFrom ?? []);
    return normalized && allowed.includes(normalized)
      ? { ok: true, to: normalized }
      : { ok: false, error: normalized
        ? new Error("Photon target is not in the outbound allowlist")
        : missingTargetError("Photon", "<E.164 phone number>") };
  },
  sendText: async ({ cfg, to, text }) => {
    const identity = getActivePhotonTransportIdentity();
    const transport = identity?.transport;
    if (!transport || !identity) {
      throw notDispatchedBoundaryError(new Error("Photon transport is not running"));
    }
    try {
      const result = await sendPhotonOutbound(
        transport,
        to,
        text,
        resolveOutboundAllowFrom(resolvePhotonAccount(cfg)),
        undefined,
        undefined,
        () => getActivePhotonTransport() === transport,
      );
      notePhotonOutbound(identity);
      return attachChannelToResult("photon", result);
    } catch (error) {
      notePhotonOutboundFailure(identity, error);
      throw error;
    }
  },
};

function allowFrom(cfg: OpenClawConfig): string[] {
  return resolvePhotonAccount(cfg).config.allowFrom;
}

export const photonPlugin: ChannelPlugin<ResolvedPhotonAccount> = createChatChannelPlugin({
  base: {
    id: "photon",
    meta: {
      id: "photon",
      label: "Photon",
      selectionLabel: "Photon (iMessage)",
      docsPath: "/channels/photon",
      blurb: "Private iMessage transport through Photon Spectrum",
      order: 100,
    },
    capabilities: { chatTypes: ["direct"], media: false },
    reload: { configPrefixes: ["channels.photon"] },
    configSchema: buildChannelConfigSchema(PhotonConfigSchema),
    config: {
      listAccountIds: () => ["default"],
      defaultAccountId: () => "default",
      resolveAccount: (cfg) => resolvePhotonAccount(cfg),
      isEnabled: (account) => account.enabled,
      isConfigured: (account) => account.configured,
      resolveAllowFrom: ({ cfg }) => allowFrom(cfg),
      formatAllowFrom: ({ allowFrom: entries }) =>
        entries.flatMap((entry) => {
          const normalized = normalizeE164(String(entry));
          return normalized ? [normalized] : [];
        }),
      hasConfiguredState: ({ env = process.env }) =>
        Boolean(env.OPENCLAW_PHOTON_PROJECT_ID?.trim()) &&
        Boolean(env.OPENCLAW_PHOTON_PROJECT_SECRET?.trim()),
    },
    messaging: {
      targetPrefixes: ["photon"],
      normalizeTarget: (target) => normalizeE164(target) ?? target.trim(),
      inferTargetChatType: ({ to }) => (normalizeE164(to) ? "direct" : undefined),
      resolveOutboundSessionRoute: ({ cfg, agentId, accountId, target }) => {
        const peerId = normalizeE164(stripChannelTargetPrefix(target, "photon"));
        if (!peerId) return null;
        const identity = `photon:${peerId}`;
        return buildChannelOutboundSessionRoute({
          cfg,
          agentId,
          channel: "photon",
          accountId,
          recipientSessionExact: true,
          peer: { kind: "direct", id: peerId },
          chatType: "direct",
          from: identity,
          to: identity,
        });
      },
      targetResolver: {
        looksLikeId: (input) => normalizeE164(input) !== null,
        hint: "<E.164 phone number>",
      },
    },
    message: createChannelMessageAdapterFromOutbound({ id: "photon", outbound }),
    status: createComputedAccountStatusAdapter<ResolvedPhotonAccount>({
      defaultRuntime: createDefaultChannelRuntimeState("default"),
      buildChannelSummary: ({ snapshot }) =>
        buildPassiveChannelStatusSummary(snapshot, {
          connected: snapshot.connected === true,
          lifecycle: snapshot.lifecycle ?? "stopped",
        }),
      resolveAccountSnapshot: ({ account, runtime }) => ({
        accountId: account.accountId,
        enabled: account.enabled,
        configured: account.configured,
        extra: {
          credentialSource: account.configured ? "environment" : "missing",
          dmPolicy: account.config.dmPolicy,
          allowFrom: account.config.allowFrom,
          connected: runtime?.connected === true,
          lifecycle: runtime?.lifecycle ?? "stopped",
        },
      }),
    }),
    gateway: {
      startAccount: runPhotonAccount,
      stopAccount: async () => await stopPhotonAccount(),
    },
    heartbeat: {
      sendTyping: async ({ cfg, to }) => {
        const target = normalizeE164(to);
        if (!target || !resolveOutboundAllowFrom(resolvePhotonAccount(cfg)).includes(target)) return;
        const transport = getActivePhotonTransport();
        if (!transport) return;
        const space = await settlePhotonOperation(
          transport.resolveDirectSpace(target), "typing space resolution",
        );
        if (getActivePhotonTransport() !== transport) return;
        await photonTyping.start(space, "heartbeat");
      },
      clearTyping: async ({ cfg, to }) => {
        const target = normalizeE164(to);
        if (!target || !resolveOutboundAllowFrom(resolvePhotonAccount(cfg)).includes(target)) return;
        const transport = getActivePhotonTransport();
        if (!transport) return;
        const space = await settlePhotonOperation(
          transport.resolveDirectSpace(target), "typing space resolution",
        );
        if (getActivePhotonTransport() !== transport) return;
        await photonTyping.stop(space, "heartbeat");
      },
    },
  },
  security: {
    resolveDmPolicy: ({ account }) => ({
      policy: account.config.dmPolicy,
      allowFrom: account.config.allowFrom,
      allowFromPath: "channels.photon.allowFrom",
      policyPath: "channels.photon.dmPolicy",
      approveHint: "Add the sender's E.164 number to channels.photon.allowFrom.",
    }),
  },
  outbound,
});
