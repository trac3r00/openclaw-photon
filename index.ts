import { defineChannelPluginEntry } from "openclaw/plugin-sdk/channel-core";
import { photonPlugin } from "./src/channel.js";

export default defineChannelPluginEntry({
  id: "photon",
  name: "Photon",
  description: "Private Photon iMessage channel plugin for OpenClaw",
  plugin: photonPlugin,
});
