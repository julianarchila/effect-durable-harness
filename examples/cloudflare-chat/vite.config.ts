// The web app's build. `Cloudflare.Website.Vite` (`alchemy.run.ts`) adds the Cloudflare plugin for the front door.
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({ plugins: [react()] });
