import { describe, expect, it } from "vitest";
import { OutboundRateGate } from "./outbound-rate.js";

describe("Photon outbound rate gate", () => {
  it("bounds each destination and total sends independently", () => {
    const gate = new OutboundRateGate({ globalLimit: 3, perDestinationLimit: 2, windowMs: 100 });
    expect(gate.admit("+14155550123", 1_000)).toBe(true);
    expect(gate.admit("+14155550123", 1_001)).toBe(true);
    expect(gate.admit("+14155550123", 1_002)).toBe(false);
    expect(gate.admit("+14155550124", 1_002)).toBe(true);
    expect(gate.admit("+14155550125", 1_003)).toBe(false);
    expect(gate.admit("+14155550123", 1_101)).toBe(true);
  });
});
