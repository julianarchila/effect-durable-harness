import * as Docker from "@alchemy.run/docker";
import * as Effect from "effect/Effect";
import { api } from "../Api.ts";

/** Where the tests reach the API from the host. */
export const API_PORT = 3034;

/**
 * The API as a local Docker service: Rivet Cloud's gateway is public, so a caller anywhere reaches it.
 */
export default class Api extends Docker.Service<Api>()(
  "DurableHarnessCloudApi",
  Effect.succeed({
    main: import.meta.url,
    port: 3000,
    ports: [{ external: API_PORT, internal: 3000, mode: "host" as const }],
  }),
  api,
) {}
