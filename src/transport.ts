import type { ContentInput } from "spectrum-ts";
import { markdown, Spectrum, text, typing } from "spectrum-ts";
import { imessage } from "spectrum-ts/providers/imessage";
import { settlePhotonOperation } from "./bounded-operation.js";
import type { ResolvedPhotonAccount } from "./config.js";
import { providerBoundaryError } from "./security.js";

export type PhotonFormat = "text" | "markdown";
const PHOTON_SEND_TIMEOUT_MS = 30_000;

export interface PhotonSpace {
  readonly id: string;
  readonly type: "dm" | "group";
  send(content: ContentInput): Promise<{ readonly id: string } | undefined>;
}

export interface PhotonInboundMessage {
  readonly body: string | null;
  readonly direction: "inbound" | "outbound";
  readonly id: string;
  markRead(): Promise<void>;
  readonly senderAddress: string | null;
  readonly space: PhotonSpace;
  readonly timestamp: Date;
}

export interface PhotonTransport {
  readonly messages: AsyncIterable<PhotonInboundMessage>;
  resolveDirectSpace(address: string): Promise<PhotonSpace>;
  stop(): Promise<void>;
}

function sanitized(
  error: unknown,
  secrets: readonly string[] = [],
  classifyDispatch = false,
): Error {
  return providerBoundaryError(error, secrets, classifyDispatch);
}

export async function createPhotonTransport(
  account: ResolvedPhotonAccount,
): Promise<PhotonTransport> {
  const secrets = [account.projectSecret];
  let app: Awaited<ReturnType<typeof Spectrum>>;
  try {
    app = await Spectrum({
      projectId: account.projectId,
      projectSecret: account.projectSecret,
      providers: [imessage.config()],
      options: { flattenGroups: true },
      telemetry: account.config.telemetry,
    });
  } catch (error) {
    throw sanitized(error, secrets, true);
  }
  const platform = (() => {
    try { return imessage(app); } catch (error) { throw sanitized(error, secrets, true); }
  })();
  const safeSpace = (space: PhotonSpace): PhotonSpace => ({
    id: space.id,
    type: space.type,
    send: async (content) => {
      try { return await space.send(content); } catch (error) { throw sanitized(error, secrets, true); }
    },
  });
  const messages = async function* (): AsyncIterable<PhotonInboundMessage> {
    try {
      for await (const tuple of app.messages) {
        const [space, message] = tuple;
        if (!imessage.is(space) || !imessage.is(message)) continue;
        yield {
          body: message.content.type === "text" ? message.content.text : null,
          direction: message.direction,
          id: message.id,
          markRead: async () => {
            try { await message.read(); } catch (error) { throw sanitized(error, secrets); }
          },
          senderAddress: message.sender?.address ?? message.sender?.id ?? null,
          space: safeSpace(space),
          timestamp: message.timestamp,
        };
      }
    } catch (error) {
      throw sanitized(error, secrets);
    }
  };
  return {
    messages: messages(),
    resolveDirectSpace: async (address) => {
      try { return safeSpace(await platform.space.create(address)); }
      catch (error) { throw sanitized(error, secrets, true); }
    },
    stop: async () => {
      try { await app.stop(); } catch (error) { throw sanitized(error, secrets); }
    },
  };
}

export async function setSpaceTyping(
  space: PhotonSpace,
  state: "start" | "stop",
): Promise<void> {
  try { await space.send(typing(state)); }
  catch (error) { throw sanitized(error, [], true); }
}

export async function sendToSpace(
  space: PhotonSpace,
  body: string,
  format: PhotonFormat,
): Promise<string> {
  let message: { readonly id: string } | undefined;
  const hasRawUrl = /https?:\/\/[^\s)'"<>]+/i.test(body);
  try {
    message = await settlePhotonOperation(
      space.send(format === "markdown" && !hasRawUrl ? markdown(body) : text(body)),
      "message send",
      undefined,
      PHOTON_SEND_TIMEOUT_MS,
    );
  }
  catch (error) { throw sanitized(error, [], true); }
  if (!message) {
    throw new Error("Photon did not return an outbound message");
  }
  return message.id;
}
