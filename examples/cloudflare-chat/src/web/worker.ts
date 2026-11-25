/**
 * The front door: serves the app's static assets and forwards `/api/*` (session creation and the session sockets)
 * to the API Worker over a service binding, so the app and its API share one origin. The pattern of Alchemy's
 * `cloudflare-git-service` example.
 */
interface Fetcher {
  fetch(request: Request): Promise<Response>;
}

interface Env {
  readonly API: Fetcher;
  readonly ASSETS: Fetcher;
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    return pathname.startsWith("/api/") ? env.API.fetch(request) : env.ASSETS.fetch(request);
  },
};
