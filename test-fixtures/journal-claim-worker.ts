import { IngressJournal } from "../src/journal.js";

const [home, projectId, owner, mode] = process.argv.slice(2);
if (!home || !projectId || !owner) throw new Error("missing worker arguments");
const journal = new IngressJournal(home, projectId);
const leaseMs = mode === "renew" ? 2_000 : 30_000;
const result = await journal.claim("shared", owner, leaseMs);
process.stdout.write(`${result}\n`);
if (result === "claimed" && mode === "renew") {
  let renewals = 0;
  const timer = setInterval(() => {
    void journal.renewClaim("shared", owner, leaseMs).then((renewed) => {
      if (!renewed) throw new Error("lease ownership lost");
      renewals += 1;
      if (renewals === 25) process.stdout.write("held\n");
    });
  }, 100);
  await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
  clearInterval(timer);
  await journal.releaseClaim("shared", owner);
  process.stdout.write("released\n");
}
