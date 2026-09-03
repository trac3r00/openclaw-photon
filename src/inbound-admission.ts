import { normalizeE164 } from "./config.js";
import type { PhotonGatewayContext } from "./runtime.js";
import type { PhotonInboundMessage } from "./transport.js";

const MAX_INBOUND_LENGTH = 16_000;

export function updatePhotonTraffic(
  ctx: PhotonGatewayContext,
  field: "lastInboundAt" | "lastOutboundAt",
  at: number,
): void {
  ctx.setStatus({ ...(ctx.getStatus?.() ?? { accountId: "default" }), [field]: at });
}

export function admitPhotonMessage(
  ctx: PhotonGatewayContext,
  event: PhotonInboundMessage,
): { body: string; sender: string } | null {
  if (event.space.type !== "dm" || event.direction !== "inbound") return null;
  const sender = event.senderAddress ? normalizeE164(event.senderAddress) : null;
  const body = event.body?.trim() ? event.body : null;
  if (!sender || !body || body.length > MAX_INBOUND_LENGTH ||
    !Number.isFinite(event.timestamp.getTime()) ||
    ctx.account.config.dmPolicy === "disabled") return null;
  if (!ctx.account.config.allowFrom.includes(sender)) {
    ctx.log?.debug?.(`[default] blocked Photon sender ${sender}`);
    return null;
  }
  return { body, sender };
}
