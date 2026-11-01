#!/usr/bin/env node
// Alma Relay — Node edition.
//
// A self-hostable reverse tunnel that lets the Alma mobile app reach a
// desktop Alma from anywhere:
//
//   desktop (agent)  --outbound WSS-->  relay  <--HTTPS/WSS--  phone (client)
//
// The desktop dials OUT to this server and registers with the SHA-256 hash of
// a routing token derived from its connect code. Phones talk plain
// HTTP/WebSocket to `https://relay.host/c/<token>/...`; the relay hashes the
// token from the URL, finds the matching agent, and tunnels the request over
// the agent socket. Nothing is written to disk; only hashes are kept, in memory.
//
// The wire protocol is documented in docs/PROTOCOL.md. Configuration is all
// environment variables — see README.md or .env.example.
//
// Run behind a TLS-terminating proxy (Caddy / nginx / Cloudflare) in production.

import { createServer } from 'node:http';
import { connect as http2Connect, constants as http2Constants } from 'node:http2';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { WebSocketServer, WebSocket } from 'ws';
import { apnsSignerFromEnv, apnsTopic, apnsHost, apnsRequestHeaders, clampBatch, validateNotification, resultFromResponse } from './apns.mjs';
import { VERSION, authorize, bearerOf, isTokenHash, parseClientPath, sanitizeCloseCode, turnCredentials } from './shared.mjs';

const env = process.env;
const PORT = Number(env.PORT || 8787);
const HOST = env.HOST || '0.0.0.0';
const MB = 1024 * 1024;
/** A non-negative number from the environment, or the default. 0 means "no limit". */
const limit = (value, fallback) => {
    const n = Number(value);
    return value !== undefined && value !== '' && Number.isFinite(n) && n >= 0 ? n : fallback;
};
const MAX_BODY = limit(env.MAX_BODY_MB, 128) * MB || Infinity; // one request body
const MAX_BUFFERED = limit(env.MAX_BUFFERED_MB, 512) * MB; // all request bodies being buffered at once
const MAX_AGENTS = limit(env.MAX_AGENTS, 10_000);
const MAX_REQUESTS_PER_AGENT = limit(env.MAX_REQUESTS_PER_AGENT, 256); // concurrent HTTP requests
const MAX_SOCKETS_PER_AGENT = limit(env.MAX_SOCKETS_PER_AGENT, 256); // concurrent bridged WebSockets
const MAX_CONNECTIONS = limit(env.MAX_CONNECTIONS, 0); // TCP connections to this process
const STALL_MS = limit(env.STALL_TIMEOUT_S, 60) * 1000; // a phone that drains nothing for this long is cut off
const HEAD_TIMEOUT_MS = 120_000; // waiting for the desktop to answer
const REGISTER_TIMEOUT_MS = 10_000; // an agent socket must register within this
const PING_INTERVAL_MS = 30_000;
// Frame caps. They match the Cloudflare edition (Durable Object messages are
// capped at 32 MiB), so a desktop that works with one works with the other.
const AGENT_MAX_FRAME = 32 * MB;
const CLIENT_MAX_MESSAGE = 24 * MB; // base64 makes it at most 32 MiB in a wmsg frame
const OPEN_QUEUE_MAX = 4 * MB; // phone bytes held while the desktop opens its side
// Flow control: above HIGH_WATER buffered bytes the sender is paused until
// the buffer drains below LOW_WATER.
const HIGH_WATER = 8 * MB;
const LOW_WATER = 1 * MB;
const FLOW_TICK_MS = 25;

// ---------------------------------------------------------------------------
// APNs (optional). With no key the relay answers `unsupported` and the
// desktop stops asking. The key is read once at startup and never logged.
// ---------------------------------------------------------------------------
const apnsEnv = {
    ...env,
    APNS_KEY: env.APNS_KEY || (env.APNS_KEY_FILE ? readFileSync(env.APNS_KEY_FILE, 'utf8') : ''),
};
let apnsSigner = null;
try {
    apnsSigner = apnsSignerFromEnv(apnsEnv);
} catch (err) {
    console.error('[relay] APNs key misconfigured:', err instanceof Error ? err.message : err);
}
const APNS_TOPIC = apnsTopic(apnsEnv);

/** One notification over a fresh HTTP/2 session (Node's fetch is HTTP/1.1 only). */
function apnsPostHttp2(host, path, headers, body) {
    return new Promise((resolve, reject) => {
        const session = http2Connect(`https://${host}`);
        const finish = fn => value => {
            session.close();
            fn(value);
        };
        session.on('error', finish(reject));
        const req = session.request({ ...headers, [http2Constants.HTTP2_HEADER_METHOD]: 'POST', [http2Constants.HTTP2_HEADER_PATH]: path });
        let status = 0;
        const chunks = [];
        req.on('response', h => {
            status = Number(h[http2Constants.HTTP2_HEADER_STATUS] || 0);
        });
        req.on('data', c => chunks.push(c));
        req.on(
            'end',
            finish(() => resolve({ status, body: Buffer.concat(chunks).toString('utf8') }))
        );
        req.on('error', finish(reject));
        req.setTimeout(15_000, () => req.close(http2Constants.NGHTTP2_CANCEL));
        req.end(body);
    });
}

async function sendPushBatch(notifications) {
    const results = [];
    for (const n of clampBatch(notifications)) {
        const invalid = validateNotification(n);
        if (invalid) {
            results.push({ token: typeof n?.token === 'string' ? n.token : '', status: 400, reason: invalid });
            continue;
        }
        try {
            const auth = await apnsSigner.token();
            const { status, body } = await apnsPostHttp2(
                apnsHost(n.environment),
                `/3/device/${n.token}`,
                apnsRequestHeaders(auth, APNS_TOPIC, n),
                JSON.stringify(n.payload)
            );
            results.push({ token: n.token, ...resultFromResponse(status, body) });
        } catch (err) {
            results.push({ token: n.token, status: 0, reason: err instanceof Error ? err.message : String(err) });
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/** tokenHash -> Agent */
const agents = new Map();
/** Bytes of phone request bodies held in memory right now (see MAX_BUFFERED). */
let bufferedBodies = 0;

const sha256 = s => createHash('sha256').update(s).digest('hex');

class Agent {
    constructor(ws, tokenHash, name) {
        this.ws = ws;
        this.tokenHash = tokenHash;
        this.name = String(name || 'desktop').slice(0, 64);
        this.nextId = 1;
        /** id -> {res, headSent, timer} for in-flight HTTP requests */
        this.http = new Map();
        /** id -> {client, opened, queue} for bridged WebSockets */
        this.sockets = new Map();
        /** HTTP requests from phones that are still uploading or waiting */
        this.inflight = 0;
        this.isAlive = true;
        // Flow control. Downstream (desktop -> phone): when a phone can't keep
        // up, stop reading the agent socket, so TCP pushes back on the desktop
        // (which waits for its socket to drain). Upstream (phone -> desktop):
        // when the agent socket's send buffer is full, stop reading the phones.
        /** key -> {level, kill, last, since}: phone sinks over HIGH_WATER */
        this.sinks = new Map();
        /** phone WebSockets paused until the agent socket drains */
        this.waiting = new Set();
        this.paused = false;
        this.flowTimer = null;
    }

    send(frame) {
        if (this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(frame));
        }
    }

    id(prefix) {
        return `${prefix}${this.nextId++}`;
    }

    /**
     * A phone sink has more than HIGH_WATER bytes queued. `level()` returns
     * its buffered bytes, or -1 once it is gone; `kill()` cuts it off when it
     * drains nothing for STALL_MS.
     */
    congested(key, level, kill) {
        if (!this.sinks.has(key)) this.sinks.set(key, { level, kill, last: level(), since: Date.now() });
        if (!this.paused) {
            this.paused = true;
            this.ws.pause();
        }
        this.startFlow();
    }

    /** Pause a phone socket while the agent socket is backed up. */
    throttle(clientWs) {
        if (this.ws.bufferedAmount <= HIGH_WATER || this.waiting.has(clientWs)) return;
        clientWs.pause();
        this.waiting.add(clientWs);
        this.startFlow();
    }

    startFlow() {
        if (!this.flowTimer) this.flowTimer = setInterval(() => this.flowTick(), FLOW_TICK_MS);
    }

    flowTick() {
        const now = Date.now();
        for (const [key, sink] of this.sinks) {
            const level = sink.level();
            if (level < LOW_WATER) {
                this.sinks.delete(key);
            } else if (level < sink.last) {
                sink.last = level;
                sink.since = now;
            } else if (STALL_MS && now - sink.since > STALL_MS) {
                this.sinks.delete(key);
                try {
                    sink.kill();
                } catch {}
            }
        }
        if (this.paused && !this.sinks.size) {
            this.paused = false;
            this.isAlive = true; // pongs were not read while paused
            if (this.ws.readyState === WebSocket.OPEN) this.ws.resume();
        }
        if (this.waiting.size && (this.ws.bufferedAmount < LOW_WATER || this.ws.readyState !== WebSocket.OPEN)) {
            for (const clientWs of this.waiting) clientWs.resume();
            this.waiting.clear();
        }
        if (!this.sinks.size && !this.waiting.size) this.stopFlow();
    }

    stopFlow() {
        clearInterval(this.flowTimer);
        this.flowTimer = null;
    }

    /** Fail in-flight requests and close every bridged phone socket. */
    destroy(reason = 'Desktop disconnected') {
        this.stopFlow();
        this.sinks.clear();
        this.waiting.clear();
        for (const [id, entry] of this.http) {
            clearTimeout(entry.timer);
            if (!entry.headSent) {
                respondJson(entry.res, 502, { error: 'Desktop disconnected from relay' });
            } else {
                entry.res.destroy();
            }
            this.http.delete(id);
        }
        for (const [id, bridge] of this.sockets) {
            try {
                bridge.client.close(1012, reason);
            } catch {}
            this.sockets.delete(id);
        }
    }
}

function respondJson(res, status, obj) {
    if (res.headersSent) {
        res.destroy();
        return;
    }
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify(obj));
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const server = createServer((req, res) => {
    const url = req.url || '';
    const pathname = url.split('?')[0];

    if (pathname === '/healthz') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, edition: 'node', version: VERSION, agents: agents.size }));
        return;
    }

    if (pathname === '/') {
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
        res.end(`Alma relay ${VERSION} is running.\nUse this address in Alma → Settings → Remote Access → Use a self-hosted relay.\n`);
        return;
    }

    if (pathname === '/turn') {
        if (req.method !== 'POST' && req.method !== 'GET') {
            respondJson(res, 405, { error: 'Method not allowed' });
            return;
        }
        (async () => {
            const auth = await authorize(env, bearerOf(req.headers.authorization));
            if (!auth.ok) return respondJson(res, 401, { error: auth.message });
            const creds = await turnCredentials(env, auth.subject || bearerOf(req.headers.authorization));
            if (!creds) return respondJson(res, 404, { error: 'TURN not configured on this relay' });
            respondJson(res, 200, creds);
        })().catch(() => respondJson(res, 500, { error: 'TURN credentials failed' }));
        return;
    }

    const parsed = parseClientPath(url);
    if (!parsed) {
        respondJson(res, 404, { error: 'Not found' });
        return;
    }

    const agent = agents.get(sha256(parsed.code));
    if (!agent) {
        respondJson(res, 502, { error: 'Desktop is offline or the connect code is invalid' });
        return;
    }
    if (Number(req.headers['content-length'] || 0) > MAX_BODY) {
        respondJson(res, 413, { error: 'Request body too large' });
        req.resume();
        return;
    }
    if (MAX_REQUESTS_PER_AGENT && agent.inflight >= MAX_REQUESTS_PER_AGENT) {
        respondJson(res, 503, { error: 'Too many concurrent requests to this desktop' });
        req.resume();
        return;
    }
    agent.inflight++;
    res.on('close', () => agent.inflight--);

    const chunks = [];
    let size = 0;
    let aborted = false;
    // Bytes of this body counted against MAX_BUFFERED, released once the
    // body has been handed to the agent socket or the request is dropped.
    let held = 0;
    const release = () => {
        bufferedBodies -= held;
        held = 0;
    };
    res.on('close', release);

    req.on('data', chunk => {
        if (aborted) return;
        size += chunk.length;
        if (size > MAX_BODY) {
            aborted = true;
            release();
            respondJson(res, 413, { error: 'Request body too large' });
            req.destroy();
            return;
        }
        if (MAX_BUFFERED && bufferedBodies + chunk.length > MAX_BUFFERED) {
            aborted = true;
            release();
            respondJson(res, 503, { error: 'Relay is busy, try again shortly' });
            req.destroy();
            return;
        }
        bufferedBodies += chunk.length;
        held += chunk.length;
        chunks.push(chunk);
    });

    req.on('end', () => {
        if (aborted) return;
        if (agents.get(agent.tokenHash) !== agent || agent.ws.readyState !== WebSocket.OPEN) {
            release();
            respondJson(res, 502, { error: 'Desktop disconnected from relay' });
            return;
        }
        const id = agent.id('h');
        const headers = { ...req.headers };
        delete headers.host;
        delete headers.connection;
        delete headers['content-length'];
        delete headers['accept-encoding']; // the agent requests identity to keep lengths honest

        const timer = setTimeout(() => {
            const entry = agent.http.get(id);
            if (entry && !entry.headSent) {
                agent.http.delete(id);
                agent.send({ t: 'hcancel', id });
                respondJson(res, 504, { error: 'Desktop did not respond in time' });
            }
        }, HEAD_TIMEOUT_MS);

        agent.http.set(id, { res, headSent: false, timer });

        res.on('close', () => {
            if (agent.http.has(id)) {
                agent.http.delete(id);
                clearTimeout(timer);
                agent.send({ t: 'hcancel', id });
            }
        });

        const body = chunks.length ? Buffer.concat(chunks) : null;
        chunks.length = 0;
        agent.send({
            t: 'hreq',
            id,
            method: req.method,
            path: parsed.rest,
            headers,
            b: body ? body.toString('base64') : undefined,
        });
        release();
    });

    req.on('error', () => {});
});

// ---------------------------------------------------------------------------
// WebSockets — one server in noServer mode for both /agent and /c/... paths
// ---------------------------------------------------------------------------

const agentWss = new WebSocketServer({ noServer: true, maxPayload: AGENT_MAX_FRAME });
const clientWss = new WebSocketServer({ noServer: true, maxPayload: CLIENT_MAX_MESSAGE });
if (MAX_CONNECTIONS) server.maxConnections = MAX_CONNECTIONS;

function refuseUpgrade(socket, status) {
    socket.write(`HTTP/1.1 ${status}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
    socket.destroy();
}

server.on('upgrade', (req, socket, head) => {
    const url = req.url || '';
    if (url === '/agent' || url.startsWith('/agent?')) {
        agentWss.handleUpgrade(req, socket, head, ws => handleAgent(ws));
        return;
    }
    const parsed = parseClientPath(url);
    if (parsed) {
        const agent = agents.get(sha256(parsed.code));
        if (!agent) return refuseUpgrade(socket, '502 Bad Gateway');
        if (MAX_SOCKETS_PER_AGENT && agent.sockets.size >= MAX_SOCKETS_PER_AGENT) return refuseUpgrade(socket, '503 Service Unavailable');
        clientWss.handleUpgrade(req, socket, head, ws => handleClientSocket(ws, agent, parsed.rest));
        return;
    }
    refuseUpgrade(socket, '404 Not Found');
});

function handleAgent(ws) {
    let agent = null;
    let registering = false;
    // Sockets that never register would otherwise sit here forever.
    const registerTimer = setTimeout(() => {
        if (!agent) ws.terminate();
    }, REGISTER_TIMEOUT_MS);

    ws.on('message', async data => {
        let frame;
        try {
            frame = JSON.parse(data.toString());
        } catch {
            return;
        }

        if (frame.t === 'register') {
            if (agent || registering) return;
            registering = true;
            const auth = await authorize(env, frame.key);
            if (!auth.ok) {
                ws.send(JSON.stringify({ t: 'error', message: auth.message }));
                ws.close(auth.code, auth.message);
                return;
            }
            if (!isTokenHash(frame.tokenHash)) {
                ws.send(JSON.stringify({ t: 'error', message: 'Invalid token hash' }));
                ws.close(4002, 'Invalid token hash');
                return;
            }
            if (ws.readyState !== WebSocket.OPEN) return;
            const existing = agents.get(frame.tokenHash);
            if (!existing && MAX_AGENTS && agents.size >= MAX_AGENTS) {
                ws.send(JSON.stringify({ t: 'error', message: 'This relay is full' }));
                ws.close(1013, 'Relay is full');
                return;
            }
            clearTimeout(registerTimer);
            if (existing) {
                existing.destroy('Desktop reconnected');
                try {
                    existing.ws.close(4000, 'Replaced by a newer connection');
                } catch {}
            }
            agent = new Agent(ws, frame.tokenHash, frame.name);
            agents.set(frame.tokenHash, agent);
            ws.send(JSON.stringify({ t: 'registered' }));
            console.log(`[relay] agent registered${auth.subject ? ` for ${auth.subject}` : ''}; agents=${agents.size}`);
            return;
        }

        if (!agent) return;

        switch (frame.t) {
            case 'push': {
                const id = String(frame.id || '');
                if (!apnsSigner) {
                    agent.send({ t: 'pushed', id, unsupported: true });
                    return;
                }
                sendPushBatch(frame.notifications).then(results => {
                    const failed = results.filter(r => r.status !== 200);
                    if (failed.length)
                        console.warn(
                            `[relay] push: ${failed.length}/${results.length} rejected`,
                            failed.map(r => r.reason || r.status)
                        );
                    agent.send({ t: 'pushed', id, results });
                });
                return;
            }
            case 'hres': {
                const entry = agent.http.get(frame.id);
                if (!entry) {
                    agent.send({ t: 'hcancel', id: frame.id });
                    return;
                }
                clearTimeout(entry.timer);
                entry.headSent = true;
                const headers = { ...(frame.headers || {}) };
                delete headers.connection;
                delete headers['keep-alive'];
                delete headers['transfer-encoding'];
                try {
                    entry.res.writeHead(frame.status || 200, headers);
                    entry.res.flushHeaders();
                } catch {
                    agent.http.delete(frame.id);
                    agent.send({ t: 'hcancel', id: frame.id });
                    try {
                        entry.res.destroy();
                    } catch {}
                }
                break;
            }
            case 'hchunk': {
                const entry = agent.http.get(frame.id);
                if (!entry) return;
                const { res } = entry;
                try {
                    res.write(Buffer.from(frame.b || '', 'base64'));
                } catch {}
                if (res.writableLength > HIGH_WATER) {
                    const id = frame.id;
                    agent.congested(
                        `h:${id}`,
                        () => (res.destroyed ? -1 : res.writableLength),
                        () => {
                            agent.http.delete(id);
                            agent.send({ t: 'hcancel', id });
                            res.destroy();
                        }
                    );
                }
                break;
            }
            case 'hend': {
                const entry = agent.http.get(frame.id);
                if (!entry) return;
                agent.http.delete(frame.id);
                try {
                    entry.res.end();
                } catch {}
                break;
            }
            case 'herr': {
                const entry = agent.http.get(frame.id);
                if (!entry) return;
                agent.http.delete(frame.id);
                clearTimeout(entry.timer);
                if (!entry.headSent) {
                    respondJson(entry.res, 502, { error: frame.message || 'Upstream error' });
                } else {
                    entry.res.destroy();
                }
                break;
            }
            case 'wopened': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) {
                    agent.send({ t: 'wclose', id: frame.id, code: 1000 });
                    return;
                }
                bridge.opened = true;
                for (const queued of bridge.queue) agent.send(queued);
                bridge.queue = [];
                bridge.queued = 0;
                agent.throttle(bridge.client);
                break;
            }
            case 'werr': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) return;
                agent.sockets.delete(frame.id);
                try {
                    bridge.client.close(1011, 'Desktop could not open the socket');
                } catch {}
                break;
            }
            case 'wmsg': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge || bridge.client.readyState !== WebSocket.OPEN) return;
                const { client } = bridge;
                if (typeof frame.s === 'string') client.send(frame.s);
                else if (typeof frame.b === 'string') client.send(Buffer.from(frame.b, 'base64'));
                if (client.bufferedAmount > HIGH_WATER) {
                    const id = frame.id;
                    agent.congested(
                        `w:${id}`,
                        () => (client.readyState === WebSocket.OPEN ? client.bufferedAmount : -1),
                        () => client.terminate() // its close handler tells the desktop
                    );
                }
                break;
            }
            case 'wclose': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) return;
                agent.sockets.delete(frame.id);
                try {
                    bridge.client.close(sanitizeCloseCode(frame.code), String(frame.reason || '').slice(0, 120));
                } catch {}
                break;
            }
        }
    });

    ws.on('pong', () => {
        if (agent) agent.isAlive = true;
    });

    ws.on('close', () => {
        clearTimeout(registerTimer);
        if (agent) agent.stopFlow();
        if (agent && agents.get(agent.tokenHash) === agent) {
            agents.delete(agent.tokenHash);
            agent.destroy();
            console.log(`[relay] agent disconnected; agents=${agents.size}`);
        }
    });

    ws.on('error', () => {});
}

function handleClientSocket(clientWs, agent, path) {
    const id = agent.id('w');
    const bridge = { client: clientWs, opened: false, queue: [], queued: 0 };
    agent.sockets.set(id, bridge);
    agent.send({ t: 'wopen', id, path });

    clientWs.on('message', (data, isBinary) => {
        const frame = isBinary ? { t: 'wmsg', id, b: Buffer.from(data).toString('base64') } : { t: 'wmsg', id, s: data.toString() };
        if (bridge.opened) {
            agent.send(frame);
            agent.throttle(clientWs);
            return;
        }
        bridge.queued += data.length;
        if (bridge.queued > OPEN_QUEUE_MAX) {
            bridge.queue = [];
            clientWs.close(1009, 'Too much data before the desktop answered');
            return;
        }
        bridge.queue.push(frame);
    });

    clientWs.on('close', (code, reason) => {
        if (agent.sockets.has(id)) {
            agent.sockets.delete(id);
            agent.send({ t: 'wclose', id, code: sanitizeCloseCode(code), reason: reason?.toString() });
        }
    });

    clientWs.on('error', () => {});
}

// Terminate dead agent connections.
const pinger = setInterval(() => {
    for (const agent of agents.values()) {
        if (agent.paused) continue; // pongs aren't read while paused; flow control has its own timeout
        if (!agent.isAlive) {
            try {
                agent.ws.terminate();
            } catch {}
            continue;
        }
        agent.isAlive = false;
        try {
            agent.ws.ping();
        } catch {}
    }
}, PING_INTERVAL_MS);

server.listen(PORT, HOST, () => {
    const gate = env.RELAY_TOKEN_SECRET ? 'signed relay tokens' : env.RELAY_REGISTRATION_KEY ? 'registration key' : 'open';
    const extras = [apnsSigner && 'APNs', env.TURN_URLS && env.TURN_SECRET && 'TURN'].filter(Boolean);
    console.log(`[relay] Alma relay ${VERSION} listening on ${HOST}:${PORT} (access: ${gate}${extras.length ? `; ${extras.join(', ')}` : ''})`);
});

// Docker sends SIGTERM; close cleanly so desktops reconnect right away.
for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, () => {
        clearInterval(pinger);
        for (const agent of agents.values()) {
            agent.destroy('Relay shutting down');
            try {
                agent.ws.close(1012, 'Relay restarting');
            } catch {}
        }
        server.close(() => process.exit(0));
        setTimeout(() => process.exit(0), 3000).unref();
    });
}
