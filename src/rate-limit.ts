export interface InboundRateGate {
  admit(sender: string, now: number): boolean;
}

export class SlidingWindowRateGate implements InboundRateGate {
  readonly #globalLimit: number;
  readonly #perSenderLimit: number;
  readonly #windowMs: number;
  #global: number[] = [];
  readonly #senders = new Map<string, number[]>();

  constructor(options: {
    readonly globalLimit?: number;
    readonly perSenderLimit?: number;
    readonly windowMs?: number;
  } = {}) {
    this.#globalLimit = options.globalLimit ?? 60;
    this.#perSenderLimit = options.perSenderLimit ?? 10;
    this.#windowMs = options.windowMs ?? 60_000;
  }

  admit(sender: string, now: number): boolean {
    const threshold = now - this.#windowMs;
    this.#global = this.#global.filter((timestamp) => timestamp > threshold);
    const senderEvents = (this.#senders.get(sender) ?? [])
      .filter((timestamp) => timestamp > threshold);
    if (this.#global.length >= this.#globalLimit || senderEvents.length >= this.#perSenderLimit) {
      this.#senders.set(sender, senderEvents);
      return false;
    }
    this.#global.push(now);
    senderEvents.push(now);
    this.#senders.set(sender, senderEvents);
    return true;
  }
}
