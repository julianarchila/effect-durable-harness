import { defineConfig } from "vitest/config";

/** The host over `TestEngine`, in Node: the alarm policy, the socket protocol, recovery and the clients. */
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
  },
});
