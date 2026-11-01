// Alma Relay — Cloudflare Workers + Durable Objects edition.
//
// Same reverse-tunnel wire protocol as the Node edition (index.mjs); see
// docs/PROTOCOL.md for the frame reference. One Durable Object per desktop,
// named by its token hash:
//
//   desktop agent ──WSS /agent?th=<sha256>──▶ TunnelDO ◀── /c/<token>/* ── phone
//
// Both the agent socket and bridged phone sockets are server-accepted via the
// WebSocket Hibernation API, so an idle pairing costs ~nothing: the runtime
// auto-answers the agent's keepalive pings without waking the DO.
//
// Free-plan friendly: SQLite-backed DO class, no storage writes at all.

import { DurableObject } from 'cloudflare:workers';
import { apnsSignerFromEnv, apnsTopic, sendWithFetch } from '../apns.mjs';
import { VERSION, authorize, b64decode, b64encode, bearerOf, isTokenHash, parseClientPath, sanitizeCloseCode, sha256hex, turnCredentials } from '../shared.mjs';

const HEAD_TIMEOUT_MS = 120_000;
const MAX_BODY = 24 * 1024 * 1024; // keep hreq frames well under the 32 MiB DO WebSocket message cap

const json = (status, obj) =>
    new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json', 'access-control-allow-origin': '*' } });

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        if (url.pathname === '/healthz') {
            return json(200, { ok: true, edition: 'cloudflare', version: VERSION });
        }

        if (url.pathname === '/') {
            return new Response(`Alma relay ${VERSION} is running.\nUse this address in Alma → Settings → Remote Access → Use a self-hosted relay.\n`, {
                headers: { 'content-type': 'text/plain; charset=utf-8' },
            });
        }

        if (url.pathname === '/turn') {
            if (request.method !== 'POST' && request.method !== 'GET') return json(405, { error: 'Method not allowed' });
            const bearer = bearerOf(request.headers.get('authorization'));
            const auth = await authorize(env, bearer);
            if (!auth.ok) return json(401, { error: auth.message });
            const creds = await turnCredentials(env, auth.subject || bearer);
            if (!creds) return json(404, { error: 'TURN not configured on this relay' });
            return json(200, creds);
        }

        if (url.pathname === '/agent') {
            if ((request.headers.get('upgrade') || '').toLowerCase() !== 'websocket') {
                return json(426, { error: 'WebSocket upgrade required' });
            }
            const th = url.searchParams.get('th') || request.headers.get('x-alma-token-hash') || '';
            if (!isTokenHash(th)) {
                return json(400, { error: 'Missing or invalid token hash (th)' });
            }
            const fwd = new Request('https://tunnel/agent', request);
            fwd.headers.set('x-alma-tunnel-role', 'agent');
            return tunnelFor(env, th).fetch(fwd);
        }

        const parsed = parseClientPath(url.pathname);
        if (!parsed) return json(404, { error: 'Not found' });

        const fwd = new Request(`https://tunnel${parsed.rest}${url.search}`, request);
        fwd.headers.set('x-alma-tunnel-role', 'client');
        return tunnelFor(env, await sha256hex(parsed.code)).fetch(fwd);
    },
};

/**
 * A Durable Object lives in the region where it was first created — by
 * default near whoever dialled in first (the desktop). If your desktop sits
 * behind a proxy far from your phone, set LOCATION_HINT (e.g. "apac", "enam",
 * "weur") to pin tunnels near the phone instead. Tunnels hold no storage, so
 * changing it only affects where the next connection lands.
 */
function tunnelFor(env, th) {
    const hint = String(env.LOCATION_HINT || '').trim();
    return env.TUNNEL.get(env.TUNNEL.idFromName(th), hint ? { locationHint: hint } : undefined);
}

export class TunnelDO extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        // In-memory only: every entry belongs to a live request/socket, and
        // the DO cannot hibernate while one is active.
        this.pending = new Map(); // id -> {enqueue, close, fail, headResolve, timer, headersSent}
        this.preOpenQueues = new Map(); // client ws id -> frames queued until agent sends wopened
        this.counter = 0;
        // APNs signer (see ../../apns.mjs); null when the deployment has no
        // APNS_KEY secret — the agent is then told `unsupported` and the
        // desktop stops trying. The provider token is cached on the instance.
        this.apns = undefined;
    }

    apnsSigner() {
        if (this.apns === undefined) {
            try {
                this.apns = apnsSignerFromEnv(this.env);
            } catch (err) {
                console.error('[relay] APNs key misconfigured:', err instanceof Error ? err.message : err);
                this.apns = null;
            }
        }
        return this.apns;
    }

    // Agent asked us to notify its phone(s). The desktop never sees the APNs
    // key; it only learns per-token outcomes so it can forget dead tokens.
    async handlePush(frame) {
        const id = String(frame.id || '');
        const signer = this.apnsSigner();
        if (!signer) {
            this.sendToAgent({ t: 'pushed', id, unsupported: true });
            return;
        }
        const results = await sendWithFetch(signer, apnsTopic(this.env), frame.notifications);
        const failed = results.filter(r => r.status !== 200).length;
        if (failed)
            console.warn(
                `[relay] push: ${failed}/${results.length} rejected`,
                results.filter(r => r.status !== 200).map(r => r.reason || r.status)
            );
        this.sendToAgent({ t: 'pushed', id, results });
    }

    nextId(prefix) {
        return `${prefix}${++this.counter}-${Math.floor(Math.random() * 1e6)}`;
    }

    agentSocket() {
        const sockets = this.ctx.getWebSockets('agent');
        for (const ws of sockets) {
            if (ws.deserializeAttachment()?.registered) return ws;
        }
        return null;
    }

    sendToAgent(frame) {
        const agent = this.agentSocket();
        if (!agent) return false;
        try {
            agent.send(JSON.stringify(frame));
            return true;
        } catch {
            return false;
        }
    }

    async fetch(request) {
        const role = request.headers.get('x-alma-tunnel-role');
        if (role === 'agent') return this.acceptAgent();
        if (role === 'client') {
            if ((request.headers.get('upgrade') || '').toLowerCase() === 'websocket') {
                return this.acceptClientSocket(request);
            }
            return this.tunnelHttp(request);
        }
        return json(400, { error: 'Bad tunnel request' });
    }

    acceptAgent() {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        this.ctx.acceptWebSocket(server, ['agent']);
        server.serializeAttachment({ role: 'agent', registered: false });
        return new Response(null, { status: 101, webSocket: client });
    }

    acceptClientSocket(request) {
        const agent = this.agentSocket();
        if (!agent) return json(502, { error: 'Desktop is offline or the connect code is invalid' });

        const url = new URL(request.url);
        const id = this.nextId('w');
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        this.ctx.acceptWebSocket(server, ['client', `c:${id}`]);
        server.serializeAttachment({ role: 'client', id });
        this.preOpenQueues.set(id, []);
        this.sendToAgent({ t: 'wopen', id, path: url.pathname + url.search });
        return new Response(null, { status: 101, webSocket: client });
    }

    async tunnelHttp(request) {
        const agent = this.agentSocket();
        if (!agent) return json(502, { error: 'Desktop is offline or the connect code is invalid' });

        const url = new URL(request.url);
        const id = this.nextId('h');

        let body;
        try {
            const buf = await request.arrayBuffer();
            if (buf.byteLength > MAX_BODY) return json(413, { error: 'Request body too large' });
            body = buf.byteLength > 0 ? new Uint8Array(buf) : null;
        } catch {
            return json(400, { error: 'Failed to read request body' });
        }

        const headers = {};
        for (const [key, value] of request.headers) {
            const k = key.toLowerCase();
            if (k === 'host' || k === 'connection' || k === 'content-length' || k === 'accept-encoding' || k.startsWith('x-alma-tunnel') || k.startsWith('cf-'))
                continue;
            headers[k] = value;
        }

        const entry = {
            enqueue: null,
            close: null,
            fail: null,
            headResolve: null,
            headersSent: false,
            timer: null,
        };
        this.pending.set(id, entry);

        const head = new Promise(resolve => {
            entry.headResolve = resolve;
            entry.timer = setTimeout(() => {
                if (this.pending.get(id) === entry && !entry.headersSent) {
                    this.pending.delete(id);
                    this.sendToAgent({ t: 'hcancel', id });
                    resolve({ status: 504, error: true, message: 'Desktop did not respond in time' });
                }
            }, HEAD_TIMEOUT_MS);
        });

        const self = this;
        const stream = new ReadableStream({
            start(controller) {
                entry.enqueue = chunk => {
                    try {
                        controller.enqueue(chunk);
                    } catch {
                        /* client gone */
                    }
                };
                entry.close = () => {
                    try {
                        controller.close();
                    } catch {
                        /* already closed */
                    }
                };
                entry.fail = message => {
                    try {
                        controller.error(new Error(message));
                    } catch {
                        /* already closed */
                    }
                };
            },
            cancel() {
                cancelRequest();
            },
        });
        const cancelRequest = () => {
            if (self.pending.get(id) === entry) {
                self.pending.delete(id);
                clearTimeout(entry.timer);
                self.sendToAgent({ t: 'hcancel', id });
                entry.close?.();
            }
        };
        // The phone hung up (needs the enable_request_signal compatibility
        // flag; the signal survives the hop from the Worker into this DO).
        // Without it a long stream would keep the desktop busy until it ends.
        request.signal?.addEventListener?.('abort', cancelRequest);

        const sent = this.sendToAgent({
            t: 'hreq',
            id,
            method: request.method,
            path: url.pathname + url.search,
            headers,
            b: body ? b64encode(body) : undefined,
        });
        if (!sent) {
            this.pending.delete(id);
            clearTimeout(entry.timer);
            return json(502, { error: 'Desktop disconnected from relay' });
        }

        const res = await head;
        if (res.error) {
            this.pending.delete(id);
            return json(res.status, { error: res.message });
        }
        entry.headersSent = true;

        const resHeaders = new Headers();
        for (const [key, value] of Object.entries(res.headers || {})) {
            const k = key.toLowerCase();
            if (k === 'connection' || k === 'keep-alive' || k === 'transfer-encoding' || k === 'content-length') continue;
            resHeaders.append(key, value);
        }

        if (res.status === 204 || res.status === 304) {
            this.pending.delete(id);
            clearTimeout(entry.timer);
            return new Response(null, { status: res.status, headers: resHeaders });
        }
        return new Response(stream, { status: res.status, headers: resHeaders });
    }

    async webSocketMessage(ws, message) {
        const attachment = ws.deserializeAttachment() || {};

        if (attachment.role === 'client') {
            // Phone -> desktop traffic on a bridged socket.
            const frame =
                typeof message === 'string'
                    ? { t: 'wmsg', id: attachment.id, s: message }
                    : { t: 'wmsg', id: attachment.id, b: b64encode(new Uint8Array(message)) };
            const queue = this.preOpenQueues.get(attachment.id);
            if (queue) queue.push(frame);
            else this.sendToAgent(frame);
            return;
        }

        if (attachment.role !== 'agent') return;

        let frame;
        try {
            frame = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message));
        } catch {
            return;
        }

        if (frame.t === 'register') {
            // Access gate: a signed relay token (RELAY_TOKEN_SECRET), a shared
            // key (RELAY_REGISTRATION_KEY), or open — see shared.mjs.
            const auth = await authorize(this.env, frame.key);
            if (!auth.ok) {
                ws.send(JSON.stringify({ t: 'error', message: auth.message }));
                ws.close(auth.code, auth.message);
                return;
            }
            const account = auth.subject;
            // The DO is named by the token hash, so a mismatched register
            // frame means the agent dialled the wrong tunnel.
            if (frame.tokenHash !== this.ctx.id.name) {
                ws.send(JSON.stringify({ t: 'error', message: 'Token hash mismatch' }));
                ws.close(4002, 'Token hash mismatch');
                return;
            }
            let replaced = false;
            for (const other of this.ctx.getWebSockets('agent')) {
                if (other !== ws) {
                    replaced = true;
                    // Unregister first: if the old socket is half-open (the
                    // desktop's heartbeat already gave up on it), its close
                    // event may arrive minutes later or never, and must not
                    // then tear down tunnels that belong to this new agent.
                    try {
                        other.serializeAttachment({ role: 'agent', registered: false, replaced: true });
                    } catch {
                        /* ignore */
                    }
                    try {
                        other.close(4000, 'Replaced by a newer connection');
                    } catch {
                        /* ignore */
                    }
                }
            }
            // Every bridged phone socket was paired with a responder on the
            // previous agent connection, which the desktop has torn down. Left
            // open, the phone keeps sending into a socket id the new agent has
            // never heard of: the edge still answers its pings, so it never
            // redials, and every request times out (Node edition: Agent.destroy).
            if (replaced) this.dropTunnels('Desktop reconnected');
            ws.serializeAttachment({ role: 'agent', registered: true, account });
            ws.send(JSON.stringify({ t: 'registered' }));
            if (account) console.log(`[relay] agent registered for account ${account}`);
            return;
        }

        if (!attachment.registered) return;

        switch (frame.t) {
            case 'push':
                await this.handlePush(frame);
                return;
            case 'hres': {
                const entry = this.pending.get(frame.id);
                if (!entry) {
                    this.sendToAgent({ t: 'hcancel', id: frame.id });
                    return;
                }
                clearTimeout(entry.timer);
                entry.headResolve?.({ status: frame.status || 200, headers: frame.headers || {} });
                break;
            }
            case 'hchunk': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                entry.enqueue?.(b64decode(frame.b || ''));
                break;
            }
            case 'hend': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                this.pending.delete(frame.id);
                entry.close?.();
                break;
            }
            case 'herr': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                this.pending.delete(frame.id);
                clearTimeout(entry.timer);
                if (!entry.headersSent) {
                    entry.headResolve?.({ status: 502, error: true, message: String(frame.message || 'Upstream error') });
                } else {
                    entry.fail?.(String(frame.message || 'Upstream error'));
                }
                break;
            }
            case 'wopened': {
                const queue = this.preOpenQueues.get(String(frame.id));
                this.preOpenQueues.delete(String(frame.id));
                for (const queued of queue || []) this.sendToAgent(queued);
                break;
            }
            case 'werr': {
                this.preOpenQueues.delete(String(frame.id));
                this.closeClientSocket(String(frame.id), 1011, 'Desktop could not open the socket');
                break;
            }
            case 'wmsg': {
                const client = this.clientSocket(String(frame.id));
                if (!client) return;
                try {
                    if (typeof frame.s === 'string') client.send(frame.s);
                    else if (typeof frame.b === 'string') client.send(b64decode(frame.b));
                } catch {
                    /* client gone */
                }
                break;
            }
            case 'wclose': {
                this.preOpenQueues.delete(String(frame.id));
                this.closeClientSocket(String(frame.id), sanitizeCloseCode(frame.code), String(frame.reason || ''));
                break;
            }
        }
    }

    clientSocket(id) {
        return this.ctx.getWebSockets(`c:${id}`)[0] || null;
    }

    closeClientSocket(id, code, reason) {
        const client = this.clientSocket(id);
        if (!client) return;
        try {
            client.close(code, reason);
        } catch {
            /* ignore */
        }
    }

    async webSocketClose(ws, code, reason) {
        // Finish the close handshake: the runtime does not answer the peer's
        // close frame for us, and until we close our end the peer sits in
        // CLOSING until the edge drops the TCP connection (~10 s, code 1006).
        try {
            ws.close(sanitizeCloseCode(code), String(reason || '').slice(0, 120));
        } catch {
            /* already closed */
        }
        const attachment = ws.deserializeAttachment() || {};
        if (attachment.role === 'agent' && attachment.registered) {
            // Desktop went away: fail in-flight requests, drop bridged sockets.
            this.dropTunnels('Desktop disconnected');
        } else if (attachment.role === 'client') {
            this.preOpenQueues.delete(attachment.id);
            this.sendToAgent({ t: 'wclose', id: attachment.id, code: 1000 });
        }
    }

    /** Fail in-flight requests and close every bridged phone socket. */
    dropTunnels(reason) {
        for (const [id, entry] of this.pending) {
            clearTimeout(entry.timer);
            if (!entry.headersSent) entry.headResolve?.({ status: 502, error: true, message: 'Desktop disconnected from relay' });
            else entry.fail?.('Desktop disconnected from relay');
            this.pending.delete(id);
        }
        this.preOpenQueues.clear();
        for (const client of this.ctx.getWebSockets('client')) {
            try {
                client.close(1012, reason);
            } catch {
                /* ignore */
            }
        }
    }

    async webSocketError(ws) {
        return this.webSocketClose(ws, 1011, 'WebSocket error');
    }
}
