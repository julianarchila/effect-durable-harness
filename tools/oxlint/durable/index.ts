import { eslintCompatPlugin } from "@oxlint/plugins";

import { noExternalInCommitRule } from "./rules/no-external-in-commit.ts";
import { noInlineWakeupRule } from "./rules/no-inline-wakeup.ts";

/** effect-durable-harness's own Oxlint rules: conventions of this repository, not general anti-slop. */
const durablePlugin = eslintCompatPlugin({
  meta: { name: "durable" },
  rules: {
    "no-external-in-commit": noExternalInCommitRule,
    "no-inline-wakeup": noInlineWakeupRule,
  },
});

export default durablePlugin;
