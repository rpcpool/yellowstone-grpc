import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  clean: false,
  splitting: false,
  dts: true,
  sourcemap: true
});
