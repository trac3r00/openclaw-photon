import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import {
  sanitizeAssistantVisibleTextWithProfile,
  stripToolCallXmlTags,
} from "openclaw/plugin-sdk/text-chunking";

const AUTHORIZATION_VALUE = /\b(Bearer|Basic)\s+[A-Za-z0-9+/._~-]+={0,2}/gi;
const JSON_SECRET = /(["']?(?:projectSecret|access_token|accessToken|token)["']?\s*:\s*)"(?:\\.|[^"\\])*"/gi;
const SECRET_ASSIGNMENT = /\b(projectSecret|access_token|accessToken|token)\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,"'}]+)/gi;
const REASONING_TAG = /<(\/)?(think|analysis|tool_call|function_call)\b[^>]*>/gi;

function stripReasoningBlocks(text: string): string {
  const stack: string[] = [];
  let cursor = 0;
  let visible = "";
  for (const match of text.matchAll(REASONING_TAG)) {
    const index = match.index;
    const tag = match[0];
    const name = match[2]?.toLowerCase();
    if (index === undefined || tag === undefined || name === undefined) continue;
    if (stack.length === 0) visible += text.slice(cursor, index);
    if (match[1] === undefined) stack.push(name);
    else if (stack.at(-1) === name) stack.pop();
    else if (stack.length > 0) return visible;
    cursor = index + tag.length;
  }
  if (stack.length === 0) visible += text.slice(cursor);
  return visible;
}

export function sanitizeOutboundText(text: string): string {
  const withoutToolCalls = stripReasoningBlocks(text);
  return sanitizeAssistantVisibleTextWithProfile(
    stripToolCallXmlTags(withoutToolCalls, { stripFunctionCallsXmlPayloads: true }),
    "delivery",
  ).trim();
}

function safeField(error: unknown, key: string): unknown {
  if ((typeof error !== "object" || error === null) && typeof error !== "function") return undefined;
  try { return Reflect.get(error, key); } catch { return undefined; }
}

function safeMessage(error: unknown): string {
  try {
    if (!(error instanceof Error)) return "Unknown Photon error";
  } catch { return "Unknown Photon error"; }
  const message = safeField(error, "message");
  return typeof message === "string" ? message : "Unknown Photon error";
}

interface SafeRead { readonly ok: boolean; readonly value?: unknown }

function checkedField(error: unknown, key: PropertyKey): SafeRead {
  if ((typeof error !== "object" || error === null) && typeof error !== "function") {
    return { ok: true, value: undefined };
  }
  try { return { ok: true, value: Reflect.get(error, key) }; }
  catch { return { ok: false }; }
}

export function sanitizeError(error: unknown, secrets: readonly string[] = []): string {
  let message = safeMessage(error);
  for (const secret of secrets) {
    if (secret) message = message.replaceAll(secret, "[REDACTED]");
  }
  message = message
    .replace(JSON_SECRET, "$1\"[REDACTED]\"")
    .replace(AUTHORIZATION_VALUE, "$1 [REDACTED]")
    .replace(SECRET_ASSIGNMENT, "$1=[REDACTED]")
    .replace(/[\r\n\t]+/g, " ")
    .trim();
  return message || "Unknown Photon error";
}

const SAFE_FIELDS = [
  "code", "grpcCode", "errno", "syscall", "context", "address", "port", "host",
] as const;
const PRE_CONNECT_CODES = new Set([
  "ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "ENETDOWN", "ENETUNREACH",
  "EHOSTUNREACH", "ETIMEDOUT",
]);
const TRANSPORT_CODE =
  /^(?:E(?:AI_|CONN|NET|HOST|ADDR|PIPE|TIMEDOUT|SOCKET)|UND_ERR_|ERR_(?:NETWORK|HTTP2|QUIC|TLS|SSL))/;
const PROVEN_CONNECT_TIMEOUT_CODES = new Set([
  "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_DNS_RESOLVE_FAILED",
]);
const UNKNOWN_CODES = new Set<unknown>([2, "2", "UNKNOWN"]);
const OBJECT_BRANCHES = ["parent", "cause", "original", "error", "reason"] as const;
const ARRAY_BRANCHES = ["errors", "attempts", "attemptErrors", "retryErrors", "retryAttempts"] as const;
const UNPROVEN_BRANCH = "unproven delivery error branch";

function providerCodes(error: unknown): readonly unknown[] {
  return [safeField(error, "code"), safeField(error, "grpcCode")];
}

function isPlatformProof(error: unknown): boolean {
  try { return error instanceof PlatformMessageNotDispatchedError; } catch { return false; }
}

interface InspectedError {
  readonly accessFailed: boolean;
  readonly code?: string;
  readonly hasErrorsArray: boolean;
  readonly nested: readonly unknown[];
  readonly proof: boolean;
  readonly sent: boolean;
  readonly unknown: boolean;
}

function snapshotArray(value: unknown): { failed: boolean; values: unknown[] } {
  let array: boolean;
  try { array = Array.isArray(value); } catch { return { failed: true, values: [] }; }
  if (!array) return { failed: false, values: [] };
  let length: unknown;
  try { length = Reflect.getOwnPropertyDescriptor(value as object, "length")?.value; }
  catch { return { failed: true, values: [] }; }
  if (!Number.isSafeInteger(length) || typeof length !== "number" || length < 0 || length > 1_000) {
    return { failed: true, values: [] };
  }
  const values: unknown[] = [];
  for (let index = 0; index < length; index += 1) {
    let descriptor: PropertyDescriptor | undefined;
    try { descriptor = Reflect.getOwnPropertyDescriptor(value as object, String(index)); }
    catch { return { failed: true, values: [] }; }
    if (!descriptor) {
      values.push(UNPROVEN_BRANCH);
    } else if ("value" in descriptor) {
      values.push(descriptor.value ?? UNPROVEN_BRANCH);
    } else {
      const item = checkedField(value, String(index));
      if (!item.ok) return { failed: true, values: [] };
      values.push(item.value ?? UNPROVEN_BRANCH);
    }
  }
  return { failed: false, values };
}

function inspectError(error: unknown): InspectedError {
  const sent = checkedField(error, "sentBeforeError");
  const codeField = checkedField(error, "code");
  const grpcCode = checkedField(error, "grpcCode");
  const syscall = checkedField(error, "syscall");
  const nested: unknown[] = [];
  const platformProof = isPlatformProof(error);
  let accessFailed = !sent.ok || !codeField.ok || !grpcCode.ok || !syscall.ok;
  if (!platformProof) {
    for (const key of OBJECT_BRANCHES) {
      const branch = checkedField(error, key);
      accessFailed ||= !branch.ok;
      if (branch.value !== null && branch.value !== undefined) nested.push(branch.value);
    }
  }
  let hasErrorsArray = false;
  for (const key of ARRAY_BRANCHES) {
    if (platformProof && key === "errors") continue;
    const branch = checkedField(error, key);
    accessFailed ||= !branch.ok;
    const snapshot = snapshotArray(branch.value);
    accessFailed ||= snapshot.failed;
    if (key === "errors" && snapshot.values.length > 0) hasErrorsArray = true;
    nested.push(...snapshot.values);
  }
  const rawCode = typeof codeField.value === "string" ? codeField.value : grpcCode.value;
  const code = typeof rawCode === "string" ? rawCode.trim().toUpperCase() : undefined;
  const proof = platformProof || Boolean(code && PROVEN_CONNECT_TIMEOUT_CODES.has(code)) ||
    ((syscall.value === "connect" || syscall.value === "getaddrinfo") &&
      code !== undefined && PRE_CONNECT_CODES.has(code));
  const normalizeProviderCode = (value: unknown): unknown =>
    typeof value === "string" ? value.trim().toUpperCase() : value;
  const unknown = UNKNOWN_CODES.has(normalizeProviderCode(codeField.value)) ||
    UNKNOWN_CODES.has(normalizeProviderCode(grpcCode.value));
  return { accessFailed, code, hasErrorsArray, nested, proof, sent: sent.value === true, unknown };
}

export function classifyProviderSendError(error: unknown): "ambiguous" | "not_dispatched" {
  const queue: unknown[] = [error];
  const seen = new Set<unknown>();
  let foundProof = false;
  for (let cursor = 0; cursor < queue.length; cursor += 1) {
    const candidate = queue[cursor];
    if (candidate !== null && (typeof candidate === "object" || typeof candidate === "function")) {
      if (seen.has(candidate)) continue;
      seen.add(candidate);
    }
    const inspected = inspectError(candidate);
    if (inspected.accessFailed || inspected.sent || inspected.unknown) return "ambiguous";
    if (inspected.proof) foundProof = true;
    const aggregateSummary = inspected.hasErrorsArray && inspected.code !== undefined &&
      PRE_CONNECT_CODES.has(inspected.code);
    if (!inspected.proof && (inspected.nested.length === 0 ||
      (inspected.code !== undefined && !aggregateSummary &&
        (PRE_CONNECT_CODES.has(inspected.code) || TRANSPORT_CODE.test(inspected.code))))) {
      return "ambiguous";
    }
    queue.push(...inspected.nested);
  }
  return foundProof ? "not_dispatched" : "ambiguous";
}

export function sanitizeProviderError(
  error: unknown,
  secrets: readonly string[] = [],
  depth = 0,
): Error {
  const safe = new Error(sanitizeError(error, secrets));
  const name = safeField(error, "name");
  if (typeof name === "string" && /^[A-Za-z][A-Za-z0-9_.-]{0,79}$/.test(name)) safe.name = name;
  for (const key of SAFE_FIELDS) {
    const value = safeField(error, key);
    if (typeof value === "number") Reflect.set(safe, key, value);
    if (typeof value === "string") Reflect.set(safe, key, sanitizeError(new Error(value), secrets));
  }
  const unknown = providerCodes(error).some((code) =>
    UNKNOWN_CODES.has(code) || (typeof code === "string" && code.trim().toUpperCase() === "UNKNOWN"));
  const retryable = safeField(error, "retryable");
  if (unknown || typeof retryable === "boolean") Reflect.set(safe, "retryable", unknown ? false : retryable);
  const cause = safeField(error, "cause");
  if (cause !== undefined && cause !== error && depth < 4) {
    Reflect.set(safe, "cause", sanitizeProviderError(cause, secrets, depth + 1));
  }
  return safe;
}

export function notDispatchedBoundaryError(
  error: unknown,
  secrets: readonly string[] = [],
): PlatformMessageNotDispatchedError {
  const safe = sanitizeProviderError(error, secrets);
  const retryable = isPlatformProof(error)
    ? safeField(error, "retryable") !== false
    : true;
  return new PlatformMessageNotDispatchedError(safe.message, { cause: safe, retryable });
}

export function providerBoundaryError(
  error: unknown,
  secrets: readonly string[],
  classifyDispatch: boolean,
): Error {
  const safe = sanitizeProviderError(error, secrets);
  if (!classifyDispatch) return safe;
  if (classifyProviderSendError(error) !== "not_dispatched") {
    const ambiguous = new Error(safe.message);
    ambiguous.name = "PhotonAmbiguousDeliveryError";
    Reflect.set(ambiguous, "retryable", false);
    return ambiguous;
  }
  const retryable = safeField(safe, "retryable");
  return new PlatformMessageNotDispatchedError(safe.message, {
    cause: safe,
    retryable: retryable !== false,
  });
}
