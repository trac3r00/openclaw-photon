import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

type Manifest = {
  readonly channelConfigs?: Readonly<Record<string, unknown>>;
};

describe("Photon plugin manifest", () => {
  it("publishes cold-path channel configuration metadata", async () => {
    const raw = await readFile(new URL("../openclaw.plugin.json", import.meta.url), "utf8");
    const manifest: Manifest = JSON.parse(raw);

    expect(manifest.channelConfigs).toHaveProperty("photon");
  });
});
