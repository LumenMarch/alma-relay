#!/usr/bin/env node
// End-to-end tests for both relay editions.
//
//   fake backend (http+ws)  <--  FakeAgent (desktop)  -->  relay  <--  this script (phone)
//
//   node test/e2e.mjs                          # Node edition (spawns index.mjs)
//   node test/e2e.mjs --cloudflare             # Cloudflare edition (spawns `wrangler dev --local`)
//   node test/e2e.mjs https://relay.example.com  # smoke-test a deployed, open relay
//
// Exit status is the number of failed checks (0 = all pass).

import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { createHash, createHmac, randomBytes } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocketServer, WebSocket } from 'ws';
import { FakeAgent } from './fake-agent.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ARGS = process.argv.slice(2);
const EXTERNAL = ARGS.find(a => !a.startsWith('--'))?.replace(/\/+$/, '') || null;
const EDITION = EXTERNAL ? 'external' : ARGS.includes('--cloudflare') ? 'cloudflare' : 'node';
const SLACK = EDITION === 'node' ? 1 : 4; // timing tolerance multiplier
const BACKEND_PORT = 23000 + Math.floor(Math.random() * 500);
let nextRelayPort = 18800 + Math.floor(Math.random() * 500);

let failures = 0;
const check = (name, ok, detail = '') => {
    console.log(`${ok ? '  ✓' : '  ✗'} ${name}${ok || !detail ? '' : ` — ${detail}`}`);
    if (!ok) failures++;
};
const newCode = () => randomBytes(32).toString('base64url');
const sha = b => createHash('sha256').update(b).digest('hex');

// ---------------------------------------------------------------------------
// Fake desktop backend
// ---------------------------------------------------------------------------
const bigPayload = randomBytes(5 * 1024 * 1024);
let lastSlowClosed = false;

const backend = createServer((req, res) => {
    if (req.url === '/api/health') {
        res.writeHead(200, { 'content-type': 'application/json' });
        return res.end(JSON.stringify({ ok: true }));
    }
    if (req.url.startsWith('/api/echo')) {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            res.writeHead(200, { 'content-type': 'application/json', 'x-echo-header': req.headers['x-test-header'] || '' });
            res.end(JSON.stringify({ body: Buffer.concat(chunks).toString(), method: req.method, url: req.url }));
        });
        return;
    }
    if (req.url === '/api/upload') {
        const hash = createHash('sha256');
        let n = 0;
        req.on('data', c => {
            hash.update(c);
            n += c.length;
        });
        req.on('end', () => {
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ sha: hash.digest('hex'), n }));
        });
        return;
    }
    if (req.url === '/api/sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        let n = 0;
        const timer = setInterval(() => {
            res.write(`data: event-${++n}\n\n`);
            if (n >= 3) {
                clearInterval(timer);
                res.end();
            }
        }, 300);
        res.on('close', () => clearInterval(timer));
        return;
    }
    if (req.url === '/api/slow') {
        lastSlowClosed = false;
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write('data: first\n\n');
        res.on('close', () => (lastSlowClosed = true));
        return;
    }
    if (req.url === '/api/big') {
        res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': String(bigPayload.length) });
        return res.end(bigPayload);
    }
    if (req.url === '/api/empty') {
        res.writeHead(204);
        return res.end();
    }
    if (req.url === '/api/drop') {
        // The desktop's own request fails before any response head → herr.
        return req.socket.destroy();
    }
    res.writeHead(404).end();
});
const backendWss = new WebSocketServer({ server: backend });
backendWss.on('connection', (ws, req) => {
    if (req.url === '/ws/hello') ws.send('hello-from-desktop');
    ws.on('message', (data, isBinary) => ws.send(data, { binary: isBinary }));
});
await new Promise(r => backend.listen(BACKEND_PORT, '127.0.0.1', r));

// ---------------------------------------------------------------------------
// Relay launchers
// ---------------------------------------------------------------------------
async function startRelay(vars = {}) {
    if (EXTERNAL) return { origin: EXTERNAL, stop() {} };
    const port = nextRelayPort++;
    const origin = `http://127.0.0.1:${port}`;
    let child;
    if (EDITION === 'cloudflare') {
        const args = ['wrangler', 'dev', '--local', '--ip', '127.0.0.1', '--port', String(port), '--inspector-port', String(port + 1000)];
        for (const [k, v] of Object.entries(vars)) args.push('--var', `${k}:${v}`);
        child = spawn('npx', args, { cwd: ROOT, env: { ...process.env, CI: '1', WRANGLER_SEND_METRICS: 'false' }, stdio: ['ignore', 'pipe', 'pipe'] });
    } else {
        child = spawn(process.execPath, [path.join(ROOT, 'index.mjs')], {
            env: { PATH: process.env.PATH, PORT: String(port), HOST: '127.0.0.1', ...vars },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
    }
    let log = '';
    child.stdout.on('data', d => (log += d));
    child.stderr.on('data', d => (log += d));
    for (let i = 0; i < (EDITION === 'cloudflare' ? 600 : 100); i++) {
        try {
            if ((await fetch(`${origin}/healthz`)).ok) return { origin, log: () => log, stop: () => child.kill('SIGTERM') };
        } catch {}
        await sleep(100);
    }
    child.kill('SIGKILL');
    throw new Error(`relay did not start:\n${log}`);
}

async function connected(opts) {
    const agent = new FakeAgent({ backendPort: BACKEND_PORT, ...opts });
    await agent.connect();
    return agent;
}

async function rejectedWith(opts) {
    const agent = new FakeAgent({ backendPort: BACKEND_PORT, ...opts });
    try {
        await agent.connect();
        agent.close();
        return 'registered';
    } catch {
        for (let i = 0; i < 30 && !agent.closed; i++) await sleep(50);
        return agent.closed?.code ?? 'no-close';
    }
}

function openPhoneSocket(url) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(url);
        const messages = [];
        ws.messages = messages;
        ws.closedWith = new Promise(r => ws.on('close', code => r(code)));
        ws.on('message', (data, isBinary) => messages.push(isBinary ? Buffer.from(data) : data.toString()));
        ws.on('open', () => resolve(ws));
        ws.on('error', reject);
    });
}

async function waitFor(fn, ms = 3000) {
    const end = Date.now() + ms * SLACK;
    while (Date.now() < end) {
        if (fn()) return true;
        await sleep(25);
    }
    return false;
}

function mintToken(secret, claims) {
    const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
    const input = `${b64({ alg: 'HS256', typ: 'JWT' })}.${b64(claims)}`;
    return `${input}.${createHmac('sha256', secret).update(input).digest('base64url')}`;
}

// ---------------------------------------------------------------------------
// Suite 1: an open relay — the tunnel itself
// ---------------------------------------------------------------------------
console.log(`[e2e] edition: ${EDITION}`);
{
    const relay = await startRelay();
    const CODE = newCode();
    const base = `${relay.origin}/c/${CODE}`;
    const wsBase = base.replace(/^http/, 'ws');
    try {
        const health = await (await fetch(`${relay.origin}/healthz`)).json();
        check('GET /healthz', health.ok === true, JSON.stringify(health));

        const agent = await connected({ relay: relay.origin, code: CODE, wopenDelayMs: 150 });
        check('desktop registers', true);

        {
            const res = await fetch(`${base}/api/health`);
            const body = await res.json().catch(() => null);
            check('GET through the tunnel', res.status === 200 && body?.ok === true, `status=${res.status}`);
        }
        {
            const res = await fetch(`${base}/api/echo?x=1&y=two`, {
                method: 'POST',
                headers: { 'content-type': 'application/json', 'x-test-header': 'hello-relay' },
                body: JSON.stringify({ ping: 'pong' }),
            });
            const body = await res.json().catch(() => null);
            check(
                'method, body, headers and query pass through',
                res.status === 200 && body?.method === 'POST' && body?.body === '{"ping":"pong"}' && body?.url === '/api/echo?x=1&y=two' && res.headers.get('x-echo-header') === 'hello-relay',
                JSON.stringify(body)
            );
        }
        {
            const res = await fetch(`${base}/api/empty`, { method: 'DELETE' });
            check('204 with no body', res.status === 204, `status=${res.status}`);
        }
        {
            const res = await fetch(`${base}/api/drop`);
            check('desktop error before the head → 502', res.status === 502, `status=${res.status}`);
        }
        {
            const t0 = Date.now();
            const reader = (await fetch(`${base}/api/sse`)).body.getReader();
            let first = 0;
            let text = '';
            for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                if (!first) first = Date.now();
                text += Buffer.from(value).toString();
            }
            const ok = text.includes('event-1') && text.includes('event-3') && first - t0 < 700 * SLACK && Date.now() - first > 400;
            check('SSE streams incrementally', ok, `first=${first - t0}ms total=${Date.now() - t0}ms`);
        }
        {
            const buf = Buffer.from(await (await fetch(`${base}/api/big`)).arrayBuffer());
            check('5 MB download intact', buf.length === bigPayload.length && sha(buf) === sha(bigPayload), `len=${buf.length}`);
        }
        {
            const up = randomBytes(3 * 1024 * 1024);
            const body = await (await fetch(`${base}/api/upload`, { method: 'POST', body: up })).json();
            check('3 MB upload intact', body.n === up.length && body.sha === sha(up), JSON.stringify(body));
        }
        {
            const ws = await openPhoneSocket(`${wsBase}/ws/echo`);
            ws.send('queued-before-open'); // the desktop answers wopened 150 ms later
            ws.send(Buffer.from([1, 2, 3, 4, 5]));
            const ok = await waitFor(() => ws.messages.length >= 2);
            check(
                'WebSocket text + binary, queued until the desktop is ready',
                ok && ws.messages[0] === 'queued-before-open' && Buffer.isBuffer(ws.messages[1]) && ws.messages[1].equals(Buffer.from([1, 2, 3, 4, 5])),
                JSON.stringify(ws.messages.map(String))
            );
            ws.close();
        }
        {
            const ws = await openPhoneSocket(`${wsBase}/ws/hello`);
            check('desktop-initiated WebSocket message', await waitFor(() => ws.messages[0] === 'hello-from-desktop'));
            const t0 = Date.now();
            ws.close();
            check('phone close reaches the desktop', await waitFor(() => agent.sockets.size === 0));
            const closeCode = await Promise.race([ws.closedWith, sleep(2000 * SLACK).then(() => 'open')]);
            check('…and the close handshake completes', closeCode === 1000 || closeCode === 1005, `code=${closeCode} after ${Date.now() - t0}ms`);
        }
        if (EDITION === 'cloudflare') {
            // `wrangler dev` fronts workerd with its own proxy, which keeps the
            // upstream request alive after the phone hangs up, so neither the
            // stream cancel nor request.signal fires locally.
            console.log('  - phone abort cancels the desktop request (skipped: wrangler dev does not forward client disconnects)');
        } else {
            const controller = new AbortController();
            const res = await fetch(`${base}/api/slow`, { signal: controller.signal });
            await res.body.getReader().read();
            controller.abort();
            check('phone abort cancels the desktop request', await waitFor(() => lastSlowClosed));
        }
        {
            const res = await fetch(`${relay.origin}/c/${'x'.repeat(43)}/api/health`);
            check('unknown code → 502', res.status === 502, `status=${res.status}`);
            const res2 = await fetch(`${relay.origin}/nope`);
            check('unknown path → 404', res2.status === 404, `status=${res2.status}`);
            const err = await openPhoneSocket(`${relay.origin.replace(/^http/, 'ws')}/c/${'x'.repeat(43)}/ws/echo`).then(
                ws => (ws.close(), 'opened'),
                e => e.message
            );
            check('unknown code WebSocket refused', err !== 'opened', err);
        }
        if (!EXTERNAL) {
            const res = await fetch(`${relay.origin}/turn`, { method: 'POST' });
            check('/turn → 404 when TURN is not configured', res.status === 404, `status=${res.status}`);
            agent.send({ t: 'push', id: 'p1', notifications: [] });
            check('push → unsupported without an APNs key', await waitFor(() => agent.pushReplies[0]?.unsupported === true), JSON.stringify(agent.pushReplies));
        }
        {
            // A newer desktop with the same code takes over; the old one is
            // told so and phone sockets bridged through it are dropped.
            const ws = await openPhoneSocket(`${wsBase}/ws/echo`);
            await waitFor(() => agent.sockets.size === 1);
            const agent2 = await connected({ relay: relay.origin, code: CODE });
            const oldClosed = await waitFor(() => agent.closed);
            const phoneCode = await Promise.race([ws.closedWith, sleep(3000 * SLACK).then(() => 'open')]);
            const res = await fetch(`${base}/api/health`);
            check('newer desktop replaces the older one', oldClosed && agent.closed.code === 4000 && res.status === 200, JSON.stringify(agent.closed));
            check('…and bridged phone sockets are dropped', phoneCode === 1012, String(phoneCode));

            const ws2 = await openPhoneSocket(`${wsBase}/ws/echo`);
            agent2.close();
            const code2 = await Promise.race([ws2.closedWith, sleep(3000 * SLACK).then(() => 'open')]);
            check('desktop going away closes phone sockets', code2 === 1012, String(code2));
            await sleep(100);
            const after = await fetch(`${base}/api/health`);
            check('…and later requests get 502', after.status === 502, `status=${after.status}`);
        }
    } catch (e) {
        check('suite: open relay', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 2: a relay gated by a shared registration key, with TURN
// ---------------------------------------------------------------------------
if (!EXTERNAL) {
    const KEY = 'k-' + randomBytes(12).toString('hex');
    const TURN_SECRET = 's-' + randomBytes(12).toString('hex');
    const relay = await startRelay({ RELAY_REGISTRATION_KEY: KEY, TURN_URLS: 'stun:turn.test:3478,turn:turn.test:3478?transport=udp', TURN_SECRET });
    try {
        check('wrong registration key → 4003', (await rejectedWith({ relay: relay.origin, code: newCode(), key: 'nope' })) === 4003);
        check('missing registration key → 4003', (await rejectedWith({ relay: relay.origin, code: newCode() })) === 4003);
        const agent = await connected({ relay: relay.origin, code: newCode(), key: KEY });
        check('right registration key registers', true);
        agent.close();

        const denied = await fetch(`${relay.origin}/turn`, { method: 'POST' });
        check('/turn without the key → 401', denied.status === 401, `status=${denied.status}`);
        const res = await fetch(`${relay.origin}/turn`, { method: 'POST', headers: { authorization: `Bearer ${KEY}` } });
        const body = await res.json().catch(() => null);
        const ice = body?.iceServers?.[0];
        const expected = ice && createHmac('sha1', TURN_SECRET).update(ice.username).digest('base64');
        check('/turn mints coturn credentials', res.status === 200 && ice.urls.length === 2 && ice.credential === expected, JSON.stringify(body));
    } catch (e) {
        check('suite: registration key', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 3: a relay gated by signed per-user tokens
// ---------------------------------------------------------------------------
if (!EXTERNAL) {
    const SECRET = randomBytes(24).toString('hex');
    const now = Math.floor(Date.now() / 1000);
    const relay = await startRelay({ RELAY_TOKEN_SECRET: SECRET });
    try {
        check('no token → 4401', (await rejectedWith({ relay: relay.origin, code: newCode() })) === 4401);
        check(
            'expired token → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken(SECRET, { sub: 'u1', aud: 'alma-relay', exp: now - 10 }) })) === 4401
        );
        check(
            'token for another audience → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken(SECRET, { sub: 'u1', aud: 'other', exp: now + 600 }) })) === 4401
        );
        check(
            'token signed with another secret → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken('wrong', { sub: 'u1', aud: 'alma-relay', exp: now + 600 }) })) === 4401
        );
        const minted = await new Promise((resolve, reject) => {
            const child = spawn(process.execPath, [path.join(ROOT, 'scripts', 'mint-token.mjs'), 'alice', '1'], { env: { ...process.env, RELAY_TOKEN_SECRET: SECRET } });
            let out = '';
            child.stdout.on('data', d => (out += d));
            child.on('exit', c => (c === 0 ? resolve(out.trim()) : reject(new Error(`mint-token exited ${c}`))));
        });
        const CODE = newCode();
        const agent = await connected({ relay: relay.origin, code: CODE, key: minted });
        const res = await fetch(`${relay.origin}/c/${CODE}/api/health`);
        check('token from scripts/mint-token.mjs registers and tunnels', res.status === 200, `status=${res.status}`);
        agent.close();
    } catch (e) {
        check('suite: relay tokens', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

backend.close();
for (const client of backendWss.clients) client.terminate();
console.log(failures === 0 ? '\n[e2e] ALL PASS' : `\n[e2e] ${failures} FAILURE(S)`);
process.exit(failures);
