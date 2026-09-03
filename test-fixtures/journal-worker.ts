import { IngressJournal } from "../src/journal.js";

const [home, projectId, prefix] = process.argv.slice(2);
if (!home || !projectId || !prefix) throw new Error("missing worker arguments");
const journal = new IngressJournal(home, projectId, { maxPending: 100 });
for (let index = 0; index < 25; index += 1) {
  const id = `${prefix}-${index}`;
  const result = await journal.enqueue({
    body: id,
    id,
    sender: "+14155550123",
    spaceId: "space-1",
    timestamp: Date.now(),
  });
  if (result !== "accepted") throw new Error(`unexpected ${result} for ${id}`);
}
