import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const workspaceManifest = readFileSync(
  fileURLToPath(new URL("../Cargo.toml", import.meta.url)),
  "utf8",
);

export const workspaceVersion = workspaceManifest.match(
  /^\s*version\s*=\s*"([^"]+)"\s*$/m,
)?.[1];

if (!workspaceVersion) {
  throw new Error("workspace package version is missing from Cargo.toml");
}
