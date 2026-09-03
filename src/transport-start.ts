import { settlePhotonOperation } from "./bounded-operation.js";
import type { ResolvedPhotonAccount } from "./config.js";
import type { PhotonTransport } from "./transport.js";

export async function startPhotonTransport(
  account: ResolvedPhotonAccount,
  createTransport: (account: ResolvedPhotonAccount, signal?: AbortSignal) => Promise<PhotonTransport>,
  signal: AbortSignal,
  timeoutMs: number,
): Promise<PhotonTransport> {
  const startupAbort = new AbortController();
  const abortStartup = () => startupAbort.abort();
  if (signal.aborted) abortStartup();
  else signal.addEventListener("abort", abortStartup, { once: true });
  const create = createTransport(account, startupAbort.signal);
  let abandoned = signal.aborted;
  void create.then(async (late) => {
    if (abandoned) {
      await settlePhotonOperation(late.stop(), "late transport stop").catch(() => undefined);
    }
  }, () => undefined);
  try {
    const transport = await settlePhotonOperation(create, "transport startup", signal, timeoutMs);
    signal.removeEventListener("abort", abortStartup);
    return transport;
  } catch (error) {
    abandoned = true;
    startupAbort.abort();
    signal.removeEventListener("abort", abortStartup);
    throw error;
  }
}
