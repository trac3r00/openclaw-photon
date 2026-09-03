import { describe, expect, it } from "vitest";
import { SlidingWindowRateGate } from "./rate-limit.js";

describe("Photon inbound rate gate", () => {
  it("enforces per-sender and global limits with a sliding window", () => {
    const gate = new SlidingWindowRateGate({ globalLimit: 2, perSenderLimit: 1, windowMs: 100 });
    expect(gate.admit("+14155550123", 1_000)).toBe(true);
    expect(gate.admit("+14155550123", 1_001)).toBe(false);
    expect(gate.admit("+14155550124", 1_001)).toBe(true);
    expect(gate.admit("+14155550125", 1_002)).toBe(false);
    expect(gate.admit("+14155550123", 1_101)).toBe(true);
  });
});
