/**
 * The shop's refund policy, in KV: read on every call, so a change applies to the next refund without a deploy. A
 * missing or unreadable value falls back to the default.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Settings } from "../../infra.ts";
import { RefundPolicy } from "../../protocol.ts";

export const DEFAULT_REFUND_POLICY: RefundPolicy = { windowDays: 30, maxAgentCents: 10_000 };

const KEY = "refund-policy";

export class Policies extends Context.Service<
  Policies,
  {
    readonly refund: Effect.Effect<RefundPolicy>;
    readonly setRefund: (policy: RefundPolicy) => Effect.Effect<void>;
  }
>()("@effect-durable-harness/example-cloudflare-support/server/services/Policies") {}

export const PoliciesLive = Layer.effect(
  Policies,
  Effect.gen(function* () {
    const kv = yield* Cloudflare.KV.ReadWriteNamespace(Settings);
    const decode = Schema.decodeUnknownOption(Schema.fromJsonString(RefundPolicy));
    const encode = Schema.encodeSync(Schema.fromJsonString(RefundPolicy));
    return Policies.of({
      refund: Host.withRuntimeContext(kv.get(KEY)).pipe(
        Effect.map((raw) =>
          raw === null
            ? DEFAULT_REFUND_POLICY
            : Option.getOrElse(decode(raw), () => DEFAULT_REFUND_POLICY),
        ),
        Effect.orElseSucceed(() => DEFAULT_REFUND_POLICY),
      ),
      setRefund: (policy) =>
        Host.withRuntimeContext(kv.put(KEY, encode(policy))).pipe(Effect.orDie),
    });
  }),
).pipe(Layer.provide(Cloudflare.KV.ReadWriteNamespaceBinding));
