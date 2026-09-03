import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";
import { IngressJournal, type IngressRecord } from "./journal.js";
import { MemoryFilesystem } from "./test-storage.js";

const PROJECT = "project/a";
const record = (id: string, enqueuedAt?: number): IngressRecord => ({
  body: "hello",
  id,
  sender: "+14155550123",
  spaceId: "space-1",
  timestamp: 1_788_364_800_000,
  ...(enqueuedAt === undefined ? {} : { enqueuedAt }),
});
const pathFor = (projectId: string): string =>
  `/home/test/.openclaw/photon/projects/${createHash("sha256").update(projectId).digest("hex")}/ingress.json`;

function journal(filesystem: MemoryFilesystem, options: ConstructorParameters<typeof IngressJournal>[2] = {}) {
  return new IngressJournal("/home/test", PROJECT, { filesystem, now: () => 2_000, ...options });
}

describe("Photon ingress journal", () => {
  it("persists project-owned work atomically and deduplicates across restart", async () => {
    const filesystem = new MemoryFilesystem();
    const first = journal(filesystem, { maxPending: 2 });
    await expect(first.enqueue(record("message-1"))).resolves.toBe("accepted");

    const second = journal(filesystem, { maxPending: 2 });
    await expect(second.enqueue(record("message-1"))).resolves.toBe("duplicate");
    await expect(second.pending()).resolves.toEqual([record("message-1", 2_000)]);
    expect(filesystem.modes.get(pathFor(PROJECT))).toBe(0o600);
    expect(filesystem.files.get(pathFor(PROJECT))).toContain(`"projectId":"${PROJECT}"`);

    await second.claim("message-1", "test-owner", 100);
    await second.complete("message-1", "test-owner");
    await expect(journal(filesystem).enqueue(record("message-1"))).resolves.toBe("duplicate");
  });

  it("durably stages every reply in order and checkpoints one at a time", async () => {
    const filesystem = new MemoryFilesystem();
    const instance = journal(filesystem);
    await instance.enqueue(record("message-1"));
    await instance.claim("message-1", "test-owner", 100);
    await instance.stageReply("message-1", "test-owner", "first reply");
    await instance.stageReply("message-1", "test-owner", "second reply");
    await expect(journal(filesystem).pending()).resolves.toEqual([
      {
        ...record("message-1", 2_000),
        agentDispatched: true,
        dispatchLease: { owner: "test-owner", expiresAt: 2_100 },
        stagedReplies: ["first reply", "second reply"],
      },
    ]);
    await instance.beginReplySend("message-1", "test-owner");
    await expect(journal(filesystem).pending()).resolves.toEqual([
      expect.objectContaining({ deliveryState: "send_in_progress", stagedReplies: ["first reply", "second reply"] }),
    ]);
    await instance.replyNotDispatched("message-1", "test-owner");
    await expect(journal(filesystem).pending()).resolves.toEqual([
      expect.objectContaining({ deliveryState: "pending", stagedReplies: ["first reply", "second reply"] }),
    ]);
    await instance.beginReplySend("message-1", "test-owner");
    await instance.checkpointReply("message-1", "test-owner");
    await expect(journal(filesystem).pending()).resolves.toEqual([
      {
        ...record("message-1", 2_000),
        agentDispatched: true,
        deliveryState: "pending",
        dispatchLease: { owner: "test-owner", expiresAt: 2_100 },
        stagedReplies: ["second reply"],
      },
    ]);
  });

  it("renews leases and fences every dispatch-state mutation by owner", async () => {
    const filesystem = new MemoryFilesystem();
    let now = 2_000;
    const first = new IngressJournal("/home/test", PROJECT, { filesystem, now: () => now });
    const second = new IngressJournal("/home/test", PROJECT, { filesystem, now: () => now });
    await first.enqueue(record("message-1"));

    await expect(first.claim("message-1", "worker-a", 100)).resolves.toBe("claimed");
    now = 2_080;
    await expect(first.renewClaim("message-1", "worker-a", 100)).resolves.toBe(true);
    now = 2_101;
    await expect(second.claim("message-1", "worker-b", 100)).resolves.toBe("busy");
    await expect(first.stageReply("message-1", "worker-b", "stale")).resolves.toBe(false);
    await expect(first.stageReply("message-1", "worker-a", "reply")).resolves.toBe(true);
    await expect(first.markAgentDispatched("message-1", "worker-a")).resolves.toBe(true);
    now = 2_181;
    await expect(first.renewClaim("message-1", "worker-a", 100)).resolves.toBe(false);
    await expect(first.stageReply("message-1", "worker-a", "expired")).resolves.toBe(false);
    await expect(second.claim("message-1", "worker-b", 100)).resolves.toBe("claimed");
    await expect(first.checkpointReply("message-1", "worker-a")).resolves.toBe(false);
    await expect(first.complete("message-1", "worker-a")).resolves.toBe(false);
    await expect(first.releaseClaim("message-1", "worker-a")).resolves.toBe(false);
  });

  it("isolates projects, validates ownership, and migrates the legacy journal once", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set("/home/test/.openclaw/photon/ingress.json", JSON.stringify({
      completed: [], pending: [record("legacy")],
    }));

    await expect(journal(filesystem).pending()).resolves.toEqual([record("legacy", record("legacy").timestamp)]);
    expect(filesystem.files.has("/home/test/.openclaw/photon/ingress.json")).toBe(false);
    await expect(new IngressJournal("/home/test", "other", { filesystem }).pending()).resolves.toEqual([]);

    filesystem.files.set(pathFor(PROJECT), JSON.stringify({
      projectId: "wrong", completed: [], pending: [],
    }));
    await expect(journal(filesystem).pending()).rejects.toThrow("ownership");
  });

  it("serializes concurrent writes from separate instances by canonical path", async () => {
    const filesystem = new MemoryFilesystem();
    const first = journal(filesystem);
    const second = journal(filesystem);

    await Promise.all([first.enqueue(record("one")), second.enqueue(record("two"))]);

    await expect(journal(filesystem).pending()).resolves.toEqual([
      record("one", 2_000), record("two", 2_000),
    ]);
  });

  it("serializes read-modify-write across independent OS processes", async () => {
    const home = await mkdtemp(join(tmpdir(), "photon-journal-"));
    const worker = resolve("test-fixtures/journal-worker.ts");
    const viteNode = resolve("node_modules/vite-node/vite-node.mjs");
    const run = (prefix: string) => new Promise<void>((resolveRun, reject) => {
      const child = spawn(process.execPath, [viteNode, worker, home, PROJECT, prefix], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stderr = "";
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      child.once("error", reject);
      child.once("exit", (code) => code === 0
        ? resolveRun()
        : reject(new Error(`journal worker exited ${String(code)}: ${stderr}`)));
    });
    try {
      await Promise.all([run("first"), run("second")]);
      const records = await new IngressJournal(home, PROJECT).pending();
      expect(records).toHaveLength(50);
      expect(new Set(records.map((entry) => entry.id)).size).toBe(50);
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  }, 10_000);

  it("prevents independent processes from holding the same dispatch lease", async () => {
    const home = await mkdtemp(join(tmpdir(), "photon-claim-"));
    const worker = resolve("test-fixtures/journal-claim-worker.ts");
    const viteNode = resolve("node_modules/vite-node/vite-node.mjs");
    const instance = new IngressJournal(home, PROJECT);
    await instance.enqueue(record("shared"));
    const start = (owner: string, renew = false) => {
      const child = spawn(process.execPath, [viteNode, worker, home, PROJECT, owner, renew ? "renew" : "once"], {
        stdio: ["pipe", "pipe", "pipe"],
      });
      const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
      const nextOutput = async (): Promise<string> => {
        const result = await lines.next();
        if (result.done) throw new Error(`journal worker ${owner} closed stdout early`);
        return result.value;
      };
      return { child, nextOutput };
    };
    try {
      const first = start("worker-a", true);
      await expect(first.nextOutput()).resolves.toBe("claimed");
      await expect(first.nextOutput()).resolves.toBe("held");
      const second = start("worker-b");
      await expect(second.nextOutput()).resolves.toBe("busy");
      first.child.stdin.write("release\n");
      await expect(first.nextOutput()).resolves.toBe("released");
      const successor = start("worker-b");
      await expect(successor.nextOutput()).resolves.toBe("claimed");
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  }, 20_000);

  it("fails closed when durable state is corrupt or oversized", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set(pathFor(PROJECT), "not-json");
    await expect(journal(filesystem).enqueue(record("message-1"))).rejects.toThrow("corrupt");

    filesystem.files.set(pathFor(PROJECT), JSON.stringify({
      projectId: PROJECT,
      completed: [],
      pending: [record("one"), record("two")],
    }));
    await expect(journal(filesystem, { maxPending: 1 }).pending()).rejects.toThrow("capacity");
  });

  it("expires pending work using enqueuedAt with a legacy timestamp fallback", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set(pathFor(PROJECT), JSON.stringify({
      projectId: PROJECT,
      completed: [],
      pending: [
        { ...record("legacy-old"), timestamp: 50 },
        record("new-old", 50),
        { ...record("legacy-fresh"), timestamp: 195 },
        record("fresh", 195),
      ],
    }));
    const retained = new IngressJournal("/home/test", PROJECT, {
      filesystem,
      retentionMs: 10,
      now: () => 200,
    });

    await expect(retained.pending()).resolves.toEqual([
      { ...record("legacy-fresh", 195), timestamp: 195 }, record("fresh", 195),
    ]);
    expect(filesystem.files.get(pathFor(PROJECT))).not.toContain("new-old");
  });

  it("never expires quarantined or staged work without operator resolution", async () => {
    const filesystem = new MemoryFilesystem();
    filesystem.files.set(pathFor(PROJECT), JSON.stringify({
      projectId: PROJECT,
      completed: [],
      pending: [
        { ...record("raw", 1), timestamp: 1 },
        { ...record("staged", 1), agentDispatched: true, stagedReplies: ["reply"] },
        { ...record("sending", 1), deliveryState: "send_in_progress", stagedReplies: ["reply"] },
        { ...record("blocked", 1), deliveryState: "policy_blocked", stagedReplies: ["reply"] },
        { ...record("quarantine", 1), deliveryState: "unknown_after_send", stagedReplies: ["reply"] },
      ],
    }));
    const retained = new IngressJournal("/home/test", PROJECT, {
      filesystem, retentionMs: 10, now: () => 200,
    });

    await expect(retained.pending()).resolves.toEqual([
      { ...record("staged", 1), agentDispatched: true, stagedReplies: ["reply"] },
      { ...record("sending", 1), deliveryState: "send_in_progress", stagedReplies: ["reply"] },
      { ...record("blocked", 1), deliveryState: "policy_blocked", stagedReplies: ["reply"] },
      { ...record("quarantine", 1), deliveryState: "unknown_after_send", stagedReplies: ["reply"] },
    ]);
  });

  it("bounds pending work and prunes expired completed ids", async () => {
    const filesystem = new MemoryFilesystem();
    const first = new IngressJournal("/home/test", PROJECT, {
      filesystem, maxPending: 1, retentionMs: 10, now: () => 100,
    });
    await first.enqueue(record("old"));
    await first.claim("old", "test-owner", 100);
    await first.complete("old", "test-owner");
    const later = new IngressJournal("/home/test", PROJECT, {
      filesystem, maxPending: 1, retentionMs: 10, now: () => 200,
    });
    await expect(later.enqueue(record("old"))).resolves.toBe("accepted");
    await expect(later.enqueue(record("overflow"))).resolves.toBe("full");
  });
});
