import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";
import { workspaceVersion } from "./workspace-version.ts";

export default defineConfig({
  define: {
    __KGI_PACKAGE_VERSION__: JSON.stringify(workspaceVersion),
  },
  plugins: [react()],
  test: {
    environment: "jsdom",
  },
});
