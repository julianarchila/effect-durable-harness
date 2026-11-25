import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import LiveWorker from "./fixture/LiveWorker.ts";

export default Alchemy.Stack(
  "DurableCloudflareLive",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* LiveWorker;
    return { url: worker.url.as<string>() };
  }),
);
