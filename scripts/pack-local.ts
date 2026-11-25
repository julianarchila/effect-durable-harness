/**
 * Builds the public packages and packs each into `.packs/<name>.tgz`, a stable
 * path a consumer can depend on with `file:`.
 *
 * `bun pm pack` rewrites `workspace:*` and `catalog:` but ignores
 * `publishConfig.exports`, so the packed `package.json` would still point at
 * `./src/*.ts`. Each tarball is therefore unpacked, its `exports` replaced by
 * `publishConfig.exports` (which points at `dist`), and packed again.
 */
import { $ } from "bun";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const packs = join(root, ".packs");

// Dependency order; the file name drops the scope so `file:` paths stay short.
const packages = [
  { directory: "packages/delta", file: "effect-durable-harness-delta.tgz" },
  { directory: "packages/durable", file: "effect-durable-harness-core.tgz" },
  { directory: "packages/opencode", file: "effect-durable-harness-opencode.tgz" },
  { directory: "packages/host", file: "effect-durable-harness-host.tgz" },
  { directory: "packages/cloudflare", file: "effect-durable-harness-cloudflare.tgz" },
  { directory: "packages/rivet", file: "effect-durable-harness-rivet.tgz" },
] as const;

await rm(packs, { recursive: true, force: true });
await $`mkdir -p ${packs}`;
for (const pkg of packages) {
  // A clean build, so no output of a deleted source file reaches a tarball.
  await rm(join(root, pkg.directory, "dist"), { recursive: true, force: true });
  await rm(join(root, pkg.directory, "tsconfig.build.tsbuildinfo"), { force: true });
}
await $`bun run build`.cwd(root);

const scratch = await mkdtemp(join(tmpdir(), "effect-durable-harness-pack-"));
try {
  for (const pkg of packages) {
    const work = join(scratch, pkg.file);
    await $`mkdir -p ${work}`;
    await $`bun pm pack --quiet --destination ${work}`.cwd(join(root, pkg.directory)).quiet();
    await $`tar -xzf ${work}/*.tgz -C ${work}`;
    const manifestPath = join(work, "package", "package.json");
    const manifest = await Bun.file(manifestPath).json();
    const { publishConfig, scripts: _scripts, devDependencies: _dev, ...published } = manifest;
    await Bun.write(
      manifestPath,
      JSON.stringify({ ...published, exports: publishConfig.exports }, null, 2) + "\n",
    );
    await $`cp ${join(root, "LICENSE")} ${join(work, "package", "LICENSE")}`;
    await $`tar -czf ${join(packs, pkg.file)} -C ${work} package`;
    console.log(`${published.name}@${published.version} -> .packs/${pkg.file}`);
  }
} finally {
  await rm(scratch, { recursive: true, force: true });
}
