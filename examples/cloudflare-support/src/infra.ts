/**
 * The stack's Cloudflare resources. Services bind them in their Layers, at init, so Alchemy sees each binding when it
 * plans the Worker that hosts the objects.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";

/** The shop's orders, staff and escalations. */
export const ShopDb = Cloudflare.D1.Database("ShopDb", { migrations: "./migrations/shop" });

/** The payment gateway's own ledger (a separate system: only `PaymentGateway` binds it). */
export const PaymentsDb = Cloudflare.D1.Database("PaymentsDb", {
  migrations: "./migrations/payments",
});

/** Shop settings the agent reads per call, such as the refund policy. */
export const Settings = Cloudflare.KV.Namespace("Settings");

/** Socket tokens, issued by the API and checked by the session socket's `accept`. */
export const CustomerTokens = Cloudflare.KV.Namespace("CustomerTokens");
