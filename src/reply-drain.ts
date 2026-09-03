import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { resolveOutboundAllowFrom } from "./config.js";
import type { DispatchLeaseGuard } from "./dispatch-lease.js";
import type { IngressJournalStore } from "./journal.js";
import { updatePhotonTraffic } from "./inbound-admission.js";
import { enforcePhotonOutboundRate } from "./outbound-rate.js";
import type { PhotonGatewayContext, PhotonRuntimeDependencies } from "./runtime.js";
import { sanitizeError } from "./security.js";
import { sendToSpace, type PhotonInboundMessage } from "./transport.js";

export async function drainStagedReplies(params: {
  readonly ctx: PhotonGatewayContext;
  readonly dependencies: PhotonRuntimeDependencies;
  readonly event: PhotonInboundMessage;
  readonly journal: IngressJournalStore;
}, lease: DispatchLeaseGuard): Promise<boolean> {
  const record = (await params.journal.pending()).find((entry) => entry.id === params.event.id);
  if (!record || !record.agentDispatched || record.deliveryState === "unknown_after_send" ||
    record.deliveryState === "send_in_progress" || record.deliveryState === "policy_blocked") return false;
  if (!resolveOutboundAllowFrom(params.ctx.account).includes(record.sender)) {
    if ((record.stagedReplies?.length ?? 0) > 0 || record.agentDispatched) {
      await params.journal.markPolicyBlocked?.(record.id, lease.owner);
      params.ctx.log?.error?.(
        `Photon journal entry ${record.id} is policy_blocked; operator action required`,
      );
      return false;
    }
  } else {
    for (const reply of record.stagedReplies ?? []) {
      if (!await lease.renew()) return false;
      enforcePhotonOutboundRate(record.sender);
      if (!lease.owned || await params.journal.beginReplySend?.(record.id, lease.owner) !== true) {
        lease.lose();
        return false;
      }
      try {
        await sendToSpace(params.event.space, reply, "markdown");
      } catch (error) {
        const failedAt = (params.dependencies.now ?? Date.now)();
        params.dependencies.outboundHealth?.failure(error, failedAt);
        if (error instanceof PlatformMessageNotDispatchedError) {
          if (Reflect.get(error, "retryable") === false) {
            if (await params.journal.markPolicyBlocked?.(record.id, lease.owner) !== true) {
              lease.lose();
            }
            params.ctx.log?.error?.(
              `Photon journal entry ${record.id} is policy_blocked; operator action required`,
            );
            return false;
          }
          if (await params.journal.replyNotDispatched?.(record.id, lease.owner) !== true) lease.lose();
          throw error;
        }
        const lastError = sanitizeError(error, [params.ctx.account.projectSecret]);
        const quarantined = await params.journal.quarantineReply?.(
          record.id, lease.owner, lastError, failedAt,
        );
        if (quarantined !== true) lease.lose();
        params.ctx.log?.error?.(
          `Photon reply delivery is unknown_after_send; operator action required: ${lastError}`,
        );
        return false;
      }
      params.dependencies.outboundHealth?.success((params.dependencies.now ?? Date.now)());
      try {
        if (await params.journal.checkpointReply?.(record.id, lease.owner) !== true) {
          throw new Error("Photon reply checkpoint was not owner-fenced");
        }
      } catch (error) {
        const failedAt = (params.dependencies.now ?? Date.now)();
        const lastError = sanitizeError(error, [params.ctx.account.projectSecret]);
        try { await params.journal.quarantineReply?.(record.id, lease.owner, lastError, failedAt); }
        catch { /* send_in_progress remains a durable no-resend quarantine */ }
        params.ctx.log?.error?.(
          `Photon reply delivery is unknown_after_send; operator action required: ${lastError}`,
        );
        return false;
      }
      if (lease.owned) {
        updatePhotonTraffic(params.ctx, "lastOutboundAt", (params.dependencies.now ?? Date.now)());
      }
    }
  }
  if (!await lease.renew()) return false;
  const completed = await params.journal.complete(record.id, lease.owner);
  if (completed !== true) lease.lose();
  return completed === true;
}
