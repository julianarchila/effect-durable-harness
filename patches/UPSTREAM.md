# Patched dependencies

Each patch here is a `bun patch` (`patchedDependencies` in the root `package.json`), applied until its upstream change lands; then it is deleted. Found while building the Rivet host.

| Patch                                      | Package                            | Upstream                                | What it fixes                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------ | ---------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@alchemy.run%2Frivet@2.0.0-beta.87.patch` | `@alchemy.run/rivet` 2.0.0-beta.87 | `alchemy-run/alchemy`, `packages/rivet` | Ours, for upstream later: `state.onStop` (the instance's in-memory work ends with the activation), `state.abort`, and alarm repair when an activation starts. It also publishes `RivetDocker.SwarmControlPlane`'s port in host mode (a workaround for Docker 29's swarm ingress, not meant for upstream). Patches both `src/` and the compiled `lib/`. |

No longer patched:

- **Rivet Cloud sign-in with several organizations**: fixed upstream (`5291b75950`, #3258), released in `2.0.0-beta.86`.
- **Packing from source**: `2.0.0-beta.86` publishes `@alchemy.run/rivet` and `@alchemy.run/test` on npm, so `vendor/alchemy` and its pack script are gone.

Not in a patch:

- **Rivet Cloud cluster tokens can't deploy**. `GET /v2/clusters/{cluster}` omits `admin_token` for a cluster token, so the CI path Alchemy documents for `RivetCloud.ControlPlane` fails. How to solve it is undecided.
- **WebSocket hibernation on Rivet 3**: already reported (`rivet-dev/rivet#5964`), with an open fix (`rivet-dev/rivet#5967`). Its stated limit (no server-initiated wake for a parked socket) matters to us.
