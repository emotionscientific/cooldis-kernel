import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    watch: {
      ignored: ["**/build/**", "**/artifacts/**", "**/dist/**", "**/.cache/**"],
    },
  },
  preview: {
    host: "127.0.0.1",
    port: 4173,
  },
});
