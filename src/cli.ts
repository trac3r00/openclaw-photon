#!/usr/bin/env node
import { homedir } from "node:os";
import { authenticateWithDeviceFlow, fetchPhoton } from "./oauth.js";
import { createPhotonApi } from "./photon-api.js";
import { provisionPhoton } from "./provision.js";
import { runCli } from "./cli-runner.js";
import { sanitizeError } from "./security.js";

runCli(process.argv.slice(2), {
  authenticate: async (notify) => await authenticateWithDeviceFlow({
    request: fetchPhoton,
    notify,
    sleep: async (milliseconds) =>
      await new Promise<void>((resolve) => setTimeout(resolve, milliseconds)),
  }),
  createApi: (token) => createPhotonApi(fetchPhoton, token),
  home: homedir(),
  now: () => new Date(),
  provision: provisionPhoton,
  stderr: (message) => process.stderr.write(`${message}\n`),
  stdout: (message) => process.stdout.write(message),
}).catch((error: unknown) => {
  process.stderr.write(`openclaw-photon: ${sanitizeError(error)}\n`);
  process.exitCode = 1;
});
