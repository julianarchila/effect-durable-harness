/**
 * Socket tokens in KV: the API issues one per customer, and the session socket's `accept` checks it. A sample: a
 * real shop issues tokens at sign-in, with an expiry.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import { CustomerTokens } from "../../infra.ts";
import { Customer } from "../../protocol.ts";

/** The token is missing, unknown or unreadable. */
export class InvalidToken extends Schema.TaggedError<InvalidToken>()("InvalidToken", {}) {}

const CustomerJson = Schema.fromJsonString(Customer);

export class CustomerAuth extends Context.Service<
  CustomerAuth,
  {
    readonly issue: (customer: Customer) => Effect.Effect<string>;
    readonly verify: (token: string) => Effect.Effect<Customer, InvalidToken>;
  }
>()("@effect-durable-harness/example-cloudflare-support/server/services/CustomerAuth") {}

export const CustomerAuthLive = Layer.effect(
  CustomerAuth,
  Effect.gen(function* () {
    const kv = yield* Cloudflare.KV.ReadWriteNamespace(CustomerTokens);
    const decode = Schema.decodeUnknownOption(CustomerJson);
    const encode = Schema.encodeSync(CustomerJson);
    return CustomerAuth.of({
      issue: (customer) =>
        Effect.gen(function* () {
          const token = `tok_${yield* Random.nextIntBetween(1e8, 1e9 - 1)}${yield* Random.nextIntBetween(1e8, 1e9 - 1)}`;
          yield* Host.withRuntimeContext(kv.put(`token:${token}`, encode(customer))).pipe(
            Effect.orDie,
          );
          return token;
        }),
      verify: (token) =>
        token === ""
          ? Effect.fail(new InvalidToken())
          : Host.withRuntimeContext(kv.get(`token:${token}`)).pipe(
              Effect.orElseSucceed(() => null),
              Effect.flatMap((raw) =>
                raw === null
                  ? Effect.fail(new InvalidToken())
                  : Option.match(decode(raw), {
                      onNone: () => Effect.fail(new InvalidToken()),
                      onSome: Effect.succeed,
                    }),
              ),
            ),
    });
  }),
).pipe(Layer.provide(Cloudflare.KV.ReadWriteNamespaceBinding));
