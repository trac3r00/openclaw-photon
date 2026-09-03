import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { describe, expect, it } from "vitest";
import {
  classifyProviderSendError,
  notDispatchedBoundaryError,
  providerBoundaryError,
  sanitizeError,
  sanitizeOutboundText,
  sanitizeProviderError,
} from "./security.js";

describe("Photon sanitization", () => {
  it("removes assistant-internal scaffolding and can suppress empty output", () => {
    expect(sanitizeOutboundText("<think>private</think> hello")).toBe("hello");
    expect(sanitizeOutboundText("<tool_call>private</tool_call>")).toBe("");
    expect(sanitizeOutboundText("visible <analysis>private forever")).toBe("visible");
    expect(sanitizeOutboundText("visible <function_call>secret forever")).toBe("visible");
    expect(sanitizeOutboundText(
      "<analysis>outer SECRET <analysis>inner</analysis> TAILSECRET</analysis> visible",
    )).toBe("visible");
    expect(sanitizeOutboundText("visible <analysis>SECRET</think> TAILSECRET")).toBe("visible");
    expect(sanitizeOutboundText("visible <analysis>SECRET</think></analysis> tail")).toBe("visible");
  });

  it("redacts known and structured secrets without rendering arbitrary objects", () => {
    expect(sanitizeError(
      new Error("Bearer abc.def projectSecret=secret-value\nnext"),
      ["secret-value"],
    )).toBe("Bearer [REDACTED] projectSecret=[REDACTED] next");
    expect(sanitizeError(new Error(
      "request failed: {\"projectSecret\":\"quoted secret with spaces\",\"ok\":false}",
    ))).toBe("request failed: {\"projectSecret\":\"[REDACTED]\",\"ok\":false}");
    expect(sanitizeError(new Error("token='quoted secret with spaces' next")))
      .toBe("token=[REDACTED] next");
    expect(sanitizeError({ projectSecret: "must-not-render" })).toBe("Unknown Photon error");
  });

  it("preserves provider classification and sanitizes its cause chain", () => {
    const cause = Object.assign(new Error("connect secret"), {
      code: "ECONNREFUSED", errno: -61, syscall: "connect",
    });
    const source = Object.assign(new Error("RPC secret failed"), {
      name: "ConnectError", code: 14, grpcCode: "UNAVAILABLE", retryable: true, cause,
    });

    const safe = sanitizeProviderError(source, ["secret"]);

    expect(safe).toMatchObject({
      name: "ConnectError", message: "RPC [REDACTED] failed",
      code: 14, grpcCode: "UNAVAILABLE", retryable: true,
    });
    expect(safe.cause).toMatchObject({
      message: "connect [REDACTED]", code: "ECONNREFUSED", errno: -61, syscall: "connect",
    });
  });

  it("requires pre-connect proof before classifying a send as not dispatched", () => {
    const unavailable = Object.assign(new Error("unavailable"), { code: 14 });
    const preConnect = Object.assign(new Error("connect failed"), {
      code: "ECONNREFUSED",
      syscall: "connect",
    });
    const unknown = Object.assign(new Error("unknown"), { code: 2, retryable: true });

    expect(classifyProviderSendError(unavailable)).toBe("ambiguous");
    expect(classifyProviderSendError(preConnect)).toBe("not_dispatched");
    expect(classifyProviderSendError(unknown)).toBe("ambiguous");
    expect(classifyProviderSendError(Object.assign(new Error("dns"), {
      code: "ENOTFOUND", syscall: "getaddrinfo",
    }))).toBe("not_dispatched");
    expect(classifyProviderSendError(Object.assign(new Error("timeout"), {
      code: "ETIMEDOUT", syscall: "connect",
    }))).toBe("not_dispatched");
    expect(classifyProviderSendError(Object.assign(new Error("reset"), {
      code: "ECONNRESET", syscall: "connect",
    }))).toBe("ambiguous");
    expect(classifyProviderSendError(Object.assign(new Error("unknown"), {
      code: " UNKNOWN ",
    }))).toBe("ambiguous");
    expect(classifyProviderSendError(Object.assign(new Error("mixed codes"), {
      code: "ECONNREFUSED",
      grpcCode: " unknown ",
      syscall: "connect",
    }))).toBe("ambiguous");
    expect(sanitizeProviderError(unknown)).toMatchObject({ code: 2, retryable: false });
  });

  it("requires every aggregate and wrapper branch to prove pre-connect failure", () => {
    const proof = Object.assign(new Error("dns"), { code: "EAI_AGAIN", syscall: "getaddrinfo" });
    const ambiguous = Object.assign(new Error("rpc unavailable"), { code: 14 });
    expect(classifyProviderSendError({ errors: [proof, ambiguous] })).toBe("ambiguous");
    expect(classifyProviderSendError({ errors: [proof, proof] })).toBe("not_dispatched");
    expect(classifyProviderSendError({ original: { reason: proof } })).toBe("not_dispatched");
    expect(classifyProviderSendError({ ...proof, cause: ambiguous })).toBe("ambiguous");
    expect(classifyProviderSendError({ parent: proof, retryAttempts: [proof, ambiguous] }))
      .toBe("ambiguous");
    expect(classifyProviderSendError({ ...proof, reason: "UNKNOWN" })).toBe("ambiguous");
  });

  it("treats UNKNOWN anywhere in a proven pre-connect graph as ambiguous", () => {
    const proof = Object.assign(new Error("connect failed"), {
      code: "ECONNREFUSED", syscall: "connect",
    });
    for (const code of [2, "2", "UNKNOWN"] as const) {
      expect(classifyProviderSendError({ code, cause: proof })).toBe("ambiguous");
      expect(classifyProviderSendError({ ...proof, cause: { code } })).toBe("ambiguous");
      expect(classifyProviderSendError({ errors: [proof, { code }] })).toBe("ambiguous");
    }
    expect(classifyProviderSendError({
      code: 2, grpcCode: "ECONNREFUSED", syscall: "connect",
    })).toBe("ambiguous");
    expect(classifyProviderSendError({
      code: "ECONNREFUSED", grpcCode: "UNKNOWN", syscall: "connect",
    })).toBe("ambiguous");
  });

  it("preserves an authentic permanent non-dispatch marker and its retryability", () => {
    const marker = new PlatformMessageNotDispatchedError("permanent", {
      cause: new Error("covered cause"),
      retryable: false,
    });

    const boundary = providerBoundaryError(marker, [], true);

    expect(classifyProviderSendError(marker)).toBe("not_dispatched");
    expect(boundary).toBeInstanceOf(PlatformMessageNotDispatchedError);
    expect(boundary).toMatchObject({ retryable: false });
    expect(notDispatchedBoundaryError(marker)).toMatchObject({ retryable: false });
  });

  it("does not expose partial pre-connect proof after sanitizing an ambiguous graph", () => {
    const ambiguous = Object.assign(new Error("wrapper"), {
      code: "ECONNREFUSED",
      syscall: "connect",
      original: { code: " unknown " },
    });

    const boundary = providerBoundaryError(ambiguous, [], true);

    expect(boundary.name).toBe("PhotonAmbiguousDeliveryError");
    expect(boundary).toMatchObject({ retryable: false });
    expect(classifyProviderSendError(boundary)).toBe("ambiguous");
  });

  it("fails closed on throwing error accessors", () => {
    const hostile = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(hostile, "code", { get: () => { throw new Error("getter secret"); } });
    Object.defineProperty(hostile, "message", { get: () => { throw new Error("message secret"); } });
    expect(() => classifyProviderSendError(hostile)).not.toThrow();
    expect(classifyProviderSendError(hostile)).toBe("ambiguous");
    expect(sanitizeError(hostile)).toBe("Unknown Photon error");
    const hostilePrototype = new Proxy({}, {
      getPrototypeOf: () => { throw new Error("prototype trap secret"); },
    });
    expect(() => notDispatchedBoundaryError(hostilePrototype)).not.toThrow();
    expect(notDispatchedBoundaryError(hostilePrototype))
      .toBeInstanceOf(PlatformMessageNotDispatchedError);

    for (const key of ["sentBeforeError", "cause", "errors"] as const) {
      const proof = Object.assign(Object.create(null) as Record<string, unknown>, {
        code: "ECONNREFUSED", syscall: "connect",
      });
      Object.defineProperty(proof, key, { get: () => { throw new Error(`${key} secret`); } });
      expect(classifyProviderSendError(proof), key).toBe("ambiguous");
    }
  });

  it("snapshots hostile aggregate arrays without invoking their iterator and fails on element access", () => {
    const proof = Object.assign(new Error("dns"), { code: "ENOTFOUND", syscall: "getaddrinfo" });
    const withoutIterator = [proof];
    Object.defineProperty(withoutIterator, Symbol.iterator, {
      value: () => { throw new Error("iterator must not run"); },
    });
    expect(classifyProviderSendError({ errors: withoutIterator })).toBe("not_dispatched");

    const hostileElement = [proof];
    Object.defineProperty(hostileElement, "0", {
      configurable: true,
      get: () => { throw new Error("element secret"); },
    });
    expect(classifyProviderSendError({ errors: hostileElement })).toBe("ambiguous");
  });
});
