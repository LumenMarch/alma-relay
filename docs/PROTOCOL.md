# Wire protocol

Both editions speak exactly this protocol. It is all the relay knows; anything
above it (Alma's API, end-to-end encryption) rides inside it opaquely.

```
desktop (agent)  ──WSS /agent?th=<tokenHash>──▶  relay  ◀── HTTPS/WSS /c/<token>/… ── phone (client)
```

## Addressing

- The desktop holds a 256-bit random **connect code**, generated locally.
- With end-to-end encryption on (the default), the phone addresses the desktop
  by a **routing token**, a one-way derivation of the code. With E2E off,
  the token is the code itself.
- With E2E on, the desktop serves **only** `wopen` for `/__alma_e2e__`. It
  answers any plaintext `hreq` with `403` and any other `wopen` with `werr`,
  because only the relay itself could have sent them (Alma ≥ 0.4.329).
- The desktop registers with `tokenHash = sha256_hex(token)`. The relay keeps
  only that hash, in memory, and finds the desktop for `/c/<token>/…` by
  hashing the token from the URL. Tokens are `[A-Za-z0-9_-]{16,128}`.
- `?th=<tokenHash>` on the agent URL lets path-routed relays (the Cloudflare
  edition) pick the tunnel before the register frame arrives.

## Agent socket

JSON text frames. Bodies and binary messages are base64 (`b`); text WebSocket
messages travel as `s`.

| Direction | Frame | Meaning |
| --- | --- | --- |
| agent → relay | `{t:'register', tokenHash, name?, ver?, key?}` | First frame. `key` is the registration key or relay token, if the relay wants one. |
| relay → agent | `{t:'registered'}` / `{t:'error', message}` | Then close codes: `4002` bad hash, `4003` bad key, `4401` bad relay token. |
| relay → agent | close `4000` | Another agent registered the same hash. Don't redial in a loop. |
| relay → agent | `{t:'hreq', id, method, path, headers, b?}` | A phone made an HTTP request. `path` includes the query. |
| agent → relay | `{t:'hres', id, status, headers}` | Response head. Send it promptly: the relay gives up after 120 s. |
| agent → relay | `{t:'hchunk', id, b}` | Response body chunk. Streamed to the phone as it arrives (SSE works). |
| agent → relay | `{t:'hend', id}` / `{t:'herr', id, message}` | Done, or failed. |
| relay → agent | `{t:'hcancel', id}` | The phone went away; stop working on `id`. |
| relay → agent | `{t:'wopen', id, path}` | A phone opened a WebSocket. |
| agent → relay | `{t:'wopened', id}` / `{t:'werr', id, message}` | The relay queues the phone's messages until `wopened`. |
| both | `{t:'wmsg', id, s}` / `{t:'wmsg', id, b}` | Text / binary message. |
| both | `{t:'wclose', id, code?, reason?}` | Close one bridged socket. |
| agent → relay | `{t:'push', id, notifications:[{token, environment, collapseId?, pushType?, priority?, payload}]}` | Ask the relay to deliver APNs notifications. `pushType: 'liveactivity'` sends a Live Activity update (to `<topic>.push-type.liveactivity`); `priority: 5` sends it at low priority. |
| relay → agent | `{t:'pushed', id, results:[{token, status, reason?}]}` / `{t:'pushed', id, unsupported:true}` | Per-token outcome, or "this relay has no APNs key". |

When the agent socket closes, the relay fails in-flight requests with `502`
and closes every bridged phone socket with `1012`, so phones redial instead of
talking into a dead tunnel.

## HTTP endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | `{ok, edition, version}` (+ `agents` on the Node edition) |
| `GET /agent` (WebSocket) | Desktop agents register here |
| `ANY /c/<token>/…` | Tunneled HTTP / WebSocket traffic to one desktop |
| `POST /turn` | Short-lived TURN credentials (`Authorization: Bearer <key or relay token>` when the relay is gated); `404` if TURN is not configured |

## Limits

| | Node | Cloudflare |
| --- | --- | --- |
| Request body | 128 MB (`MAX_BODY_MB`) | 24 MB (Durable Object WebSocket messages are capped at 32 MiB) |
| Waiting for the response head | 120 s | 120 s |
| Agent frame | 32 MiB | 32 MiB |
| Phone WebSocket message | 24 MiB | 24 MiB |
| Phone bytes held before `wopened` | 4 MiB (then close `1009`) | — |
| Request bodies buffered at once | 512 MB (`MAX_BUFFERED_MB`, then `503`) | per Durable Object |
| Desktops | 10 000 (`MAX_AGENTS`, then close `1013`) | — |
| Concurrent requests / sockets per desktop | 256 / 256 (`MAX_REQUESTS_PER_AGENT`, `MAX_SOCKETS_PER_AGENT`, then `503`) | — |
| Registering | within 10 s of connecting | — |

## Flow control (Node edition)

The relay never buffers a whole stream. When a phone reads slower than the
desktop sends, the relay stops reading the agent socket once that phone has
8 MiB queued, which pushes back on the desktop over TCP; it resumes below
1 MiB. A phone that drains nothing for `STALL_TIMEOUT_S` (60 s) is cut off
(`hcancel`, or the bridged socket is terminated). The other way round, phone
sockets are paused while the agent socket has more than 8 MiB unsent.

The pause covers the whole agent socket, so one stuck phone briefly holds up
the desktop's other streams until it drains or is cut off. Desktops should
wait for their own socket to drain before sending more (Alma does).
