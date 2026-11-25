import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import Api from "../src/server/Api.ts";

/** The API Worker alone, with the fake model (`CHAT_MODEL=faux`, set by the test before it deploys). */
export default Alchemy.Stack(
  "DurableChatTest",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const api = yield* Api;
    return { url: api.url.as<string>() };
  }),
);
