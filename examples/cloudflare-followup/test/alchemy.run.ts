/** Both Workers with the fake model (`FOLLOWUP_MODEL=faux`, set by the test before it deploys). */ import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import Api from "../src/server/Api.ts";
import SinkLive, { Sink } from "../src/sink/Sink.ts";

export default Alchemy.Stack(
  "DurableFollowupTest",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    const sink = yield* Sink;
    return { api: api.url.as<string>(), sink: sink.url.as<string>() };
  }).pipe(Effect.provide(SinkLive)),
);
