// A UUIDv7 (RFC 9562) from the `Clock` and `Random` services, as minted for `core.provider`.
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";

/** A new UUIDv7: 48 bits of Unix milliseconds, version 7, the RFC variant, and random bits. */
export const uuidv7: Effect.Effect<string> = Effect.gen(function* () {
  const millis = yield* Clock.currentTimeMillis;
  const bytes: Array<number> = [];
  for (let shift = 40; shift >= 0; shift -= 8) bytes.push(Math.floor(millis / 2 ** shift) & 0xff);
  for (let index = 6; index < 16; index++) bytes.push(yield* Random.nextIntBetween(0, 255));
  bytes[6] = 0x70 | ((bytes[6] ?? 0) & 0x0f);
  bytes[8] = 0x80 | ((bytes[8] ?? 0) & 0x3f);
  const hex = bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
});
