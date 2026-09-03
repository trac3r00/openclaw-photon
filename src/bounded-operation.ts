export const PHOTON_CONTROL_TIMEOUT_MS = 5_000;

export async function settlePhotonOperation<T>(
  operation: Promise<T>,
  label: string,
  signal?: AbortSignal,
  timeoutMs = PHOTON_CONTROL_TIMEOUT_MS,
): Promise<T> {
  void operation.catch(() => undefined);
  if (signal?.aborted) throw new Error(`Photon ${label} aborted`);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let removeAbort = (): void => undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`Photon ${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    timer.unref?.();
  });
  const aborted = new Promise<never>((_resolve, reject) => {
    if (!signal) return;
    const onAbort = () => reject(new Error(`Photon ${label} aborted`));
    signal.addEventListener("abort", onAbort, { once: true });
    removeAbort = () => signal.removeEventListener("abort", onAbort);
  });
  try {
    return await Promise.race([operation, timeout, aborted]);
  } finally {
    if (timer) clearTimeout(timer);
    removeAbort();
  }
}
