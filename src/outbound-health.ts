import { classifyProviderSendError, sanitizeError } from "./security.js";

export interface PhotonOutboundHealth {
  failure(error: unknown, at: number): void;
  success(at: number): void;
}

export function createPhotonOutboundHealth(options: {
  readonly isCurrent: () => boolean;
  readonly onRecovery: () => void;
  readonly secrets: readonly string[];
  readonly updateStatus: (patch: {
    readonly lastError?: string;
    readonly lastOutboundAt?: number;
    readonly lastOutboundErrorAt?: number;
  }) => void;
}): PhotonOutboundHealth {
  let consecutiveTransportFailures = 0;
  return {
    failure(error, at) {
      if (!options.isCurrent()) return;
      let permanent = false;
      if ((typeof error === "object" && error !== null) || typeof error === "function") {
        try { permanent = Reflect.get(error, "retryable") === false; }
        catch { permanent = false; }
      }
      consecutiveTransportFailures = !permanent &&
        classifyProviderSendError(error) === "not_dispatched"
        ? consecutiveTransportFailures + 1
        : 0;
      options.updateStatus({
        lastError: sanitizeError(error, options.secrets),
        lastOutboundErrorAt: at,
      });
      if (consecutiveTransportFailures >= 3) options.onRecovery();
    },
    success(at) {
      if (!options.isCurrent()) return;
      consecutiveTransportFailures = 0;
      options.updateStatus({
        lastError: undefined,
        lastOutboundAt: at,
        lastOutboundErrorAt: undefined,
      });
    },
  };
}
