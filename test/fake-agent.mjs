// A minimal desktop-side agent for tests: dials the relay, registers, and
// forwards tunneled HTTP/WebSocket traffic to a local backend. It speaks the
// same protocol as Alma's desktop (docs/PROTOCOL.md), minus reconnects/E2E.

import { request as httpRequest } from 'node:http';
import { createHash } from 'node:crypto';
import { WebSocket } from 'ws';

export const tokenHashOf = code => createHash('sha256').update(code).digest('hex');

export class FakeAgent {
    /**
     * @param {{relay: string, code: string, backendPort: number, key?: string, wopenDelayMs?: number}} opts
     */
    constructor(opts) {
        this.opts = opts;
        this.http = new Map();
        this.sockets = new Map();
        this.closed = null; // {code, reason} once the relay closes us
        this.errors = [];
        this.pushReplies = [];
    }

    /** Resolves once registered; rejects with the relay's error / close. */
    connect() {
        const { relay, code, key } = this.opts;
        const th = tokenHashOf(code);
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(`${relay.replace(/^http/, 'ws')}/agent?th=${th}`, { maxPayload: 256 * 1024 * 1024 });
            this.ws = ws;
            let settled = false;
            const settle = (fn, v) => {
                if (!settled) {
                    settled = true;
                    fn(v);
                }
            };
            ws.on('open', () => ws.send(JSON.stringify({ t: 'register', tokenHash: th, name: 'test-desktop', ver: 'test', key })));
            ws.on('message', data => {
                const frame = JSON.parse(data.toString());
                if (frame.t === 'registered') return settle(resolve);
                if (frame.t === 'error') {
                    this.errors.push(frame.message);
                    return settle(reject, new Error(frame.message));
                }
                this.onFrame(frame);
            });
            ws.on('close', (c, r) => {
                this.closed = { code: c, reason: r.toString() };
                for (const s of this.sockets.values()) s.terminate();
                settle(reject, new Error(`closed ${c} ${r}`));
            });
            ws.on('error', err => settle(reject, err));
        });
    }

    send(frame) {
        if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(frame));
    }

    onFrame(frame) {
        const { backendPort } = this.opts;
        switch (frame.t) {
            case 'hreq': {
                const req = httpRequest(
                    {
                        host: '127.0.0.1',
                        port: backendPort,
                        method: frame.method,
                        path: frame.path,
                        headers: { ...frame.headers, 'accept-encoding': 'identity' },
                    },
                    res => {
                        this.send({ t: 'hres', id: frame.id, status: res.statusCode, headers: res.headers });
                        res.on('data', chunk => this.send({ t: 'hchunk', id: frame.id, b: chunk.toString('base64') }));
                        res.on('end', () => {
                            this.http.delete(frame.id);
                            this.send({ t: 'hend', id: frame.id });
                        });
                    }
                );
                req.on('error', err => {
                    this.http.delete(frame.id);
                    this.send({ t: 'herr', id: frame.id, message: err.message });
                });
                this.http.set(frame.id, req);
                req.end(frame.b ? Buffer.from(frame.b, 'base64') : undefined);
                break;
            }
            case 'hcancel':
                this.http.get(frame.id)?.destroy();
                this.http.delete(frame.id);
                break;
            case 'wopen': {
                const ws = new WebSocket(`ws://127.0.0.1:${backendPort}${frame.path}`);
                this.sockets.set(frame.id, ws);
                ws.on('open', () => setTimeout(() => this.send({ t: 'wopened', id: frame.id }), this.opts.wopenDelayMs || 0));
                ws.on('message', (data, isBinary) =>
                    this.send(isBinary ? { t: 'wmsg', id: frame.id, b: Buffer.from(data).toString('base64') } : { t: 'wmsg', id: frame.id, s: data.toString() })
                );
                ws.on('close', (code, reason) => {
                    if (this.sockets.delete(frame.id)) this.send({ t: 'wclose', id: frame.id, code, reason: reason.toString() });
                });
                ws.on('error', err => {
                    if (this.sockets.delete(frame.id)) this.send({ t: 'werr', id: frame.id, message: err.message });
                });
                break;
            }
            case 'wmsg': {
                const ws = this.sockets.get(frame.id);
                if (!ws || ws.readyState !== WebSocket.OPEN) return;
                if (typeof frame.s === 'string') ws.send(frame.s);
                else ws.send(Buffer.from(frame.b, 'base64'));
                break;
            }
            case 'wclose': {
                const ws = this.sockets.get(frame.id);
                this.sockets.delete(frame.id);
                ws?.close();
                break;
            }
            case 'pushed':
                this.pushReplies.push(frame);
                break;
        }
    }

    close() {
        this.ws?.close();
    }
}
