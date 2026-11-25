import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import ProbeWorker from "./fixture/ProbeWorker.ts";

export default Alchemy.Stack(
  "DurableCloudflareTest",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* ProbeWorker;
    return { url: worker.url.as<string>() };
  }),
);
