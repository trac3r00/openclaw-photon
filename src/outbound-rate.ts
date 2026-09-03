import { SlidingWindowRateGate } from "./rate-limit.js";

export class OutboundRateGate {
  readonly #gate: SlidingWindowRateGate;

  constructor(options: {
    readonly globalLimit?: number;
    readonly perDestinationLimit?: number;
    readonly windowMs?: number;
  } = {}) {
    this.#gate = new SlidingWindowRateGate({
      globalLimit: options.globalLimit ?? 60,
      perSenderLimit: options.perDestinationLimit ?? 20,
      windowMs: options.windowMs,
    });
  }

  admit(destination: string, now: number): boolean {
    return this.#gate.admit(destination, now);
  }
}

export const photonOutboundRate = new OutboundRateGate();

export function enforcePhotonOutboundRate(
  destination: string,
  gate: OutboundRateGate = photonOutboundRate,
  now = Date.now(),
): void {
  if (!gate.admit(destination, now)) {
    throw new Error("Photon outbound rate limit exceeded");
  }
}
