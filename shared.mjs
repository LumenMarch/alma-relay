// Pieces shared by both relay editions (Node and Cloudflare Workers).
// Everything here uses WebCrypto + standard globals, so it runs unchanged in
// Node >= 20 and inside a Worker.

export const VERSION = '1.0.1';

/** Phone-facing paths look like /c/<code>/rest?query. Never log the code. */
const CLIENT_PATH = /^\/c\/([A-Za-z0-9_-]{16,128})(\/.*)?$/;

/** Parse `/c/<code>/rest?query` -> {code, rest} or null. */
export function parseClientPath(pathWithQuery) {
    const m = CLIENT_PATH.exec(pathWithQuery || '');
    if (!m) return null;
    return { code: m[1], rest: m[2] || '/' };
}

export const isTokenHash = s => typeof s === 'string' && /^[a-f0-9]{64}$/.test(s);

const enc = new TextEncoder();

export async function sha256hex(text) {
    const digest = await crypto.subtle.digest('SHA-256', enc.encode(text));
    return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
}

export function b64encode(bytes) {
    let binary = '';
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
}

export function b64decode(text) {
    const binary = atob(text);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
}

function b64urlDecode(s) {
    const p = s.replace(/-/g, '+').replace(/_/g, '/');
    return b64decode(p + '='.repeat((4 - (p.length % 4)) % 4));
}

/** Constant-time string compare (length leaks, content doesn't). */
export function safeEqual(a, b) {
    const x = enc.encode(String(a));
    const y = enc.encode(String(b));
    if (x.length !== y.length) return false;
    let diff = 0;
    for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
    return diff === 0;
}

/** WebSocket close codes a server may legally send on. */
export function sanitizeCloseCode(code) {
    const n = Number(code);
    if (n === 1000 || (n >= 3000 && n <= 4999)) return n;
    return 1000;
}

// ---------------------------------------------------------------------------
// Signed relay tokens (optional, for running one relay for many users).
//
// An HS256 JWT signed with RELAY_TOKEN_SECRET, audience "alma-relay", with a
// `sub` (the user id) and an optional `exp`. Mint them with any JWT library,
// or with `node scripts/mint-token.mjs <user-id>`.
// ---------------------------------------------------------------------------
export const RELAY_AUDIENCE = 'alma-relay';

/** Returns the token's claims if it is valid, unexpired and for this relay; else null. */
export async function verifyRelayToken(token, secret) {
    try {
        const parts = String(token || '').split('.');
        if (parts.length !== 3) return null;
        const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
        const ok = await crypto.subtle.verify('HMAC', key, b64urlDecode(parts[2]), enc.encode(`${parts[0]}.${parts[1]}`));
        if (!ok) return null;
        const header = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[0])));
        if (header.alg !== 'HS256') return null;
        const claims = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[1])));
        if (claims.aud !== RELAY_AUDIENCE) return null;
        if (typeof claims.exp === 'number' && claims.exp < Math.floor(Date.now() / 1000)) return null;
        if (!claims.sub) return null;
        return claims;
    } catch {
        return null;
    }
}

/**
 * Who may register a desktop (and mint TURN credentials).
 *   RELAY_TOKEN_SECRET set     -> a valid signed relay token is required
 *   RELAY_REGISTRATION_KEY set -> that shared key is required
 *   neither                    -> open relay (fine for personal use: a desktop
 *                                 is only reachable with its 256-bit code)
 * Returns {ok:true, subject} or {ok:false, message, code}.
 */
export async function authorize(env, presented) {
    if (env.RELAY_TOKEN_SECRET) {
        const claims = await verifyRelayToken(presented, env.RELAY_TOKEN_SECRET);
        if (!claims) return { ok: false, message: 'A valid relay token is required', code: 4401 };
        return { ok: true, subject: String(claims.sub) };
    }
    if (env.RELAY_REGISTRATION_KEY && !safeEqual(presented || '', env.RELAY_REGISTRATION_KEY)) {
        return { ok: false, message: 'Invalid registration key', code: 4003 };
    }
    return { ok: true, subject: null };
}

// ---------------------------------------------------------------------------
// TURN credentials for the phone<->desktop WebRTC stream (TURN REST API, as
// spoken by coturn's `use-auth-secret`): username "<expiry>:<id>", credential
// = base64(HMAC-SHA1(secret, username)). The relay only mints them; media
// never passes through it. Unset TURN_URLS / TURN_SECRET -> null (404).
// ---------------------------------------------------------------------------
export async function turnCredentials(env, subject) {
    const urls = String(env.TURN_URLS || '')
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
    if (urls.length === 0 || !env.TURN_SECRET) return null;
    const ttl = Math.min(86400, Math.max(300, Number(env.TURN_TTL) || 43200));
    const expiresAt = Math.floor(Date.now() / 1000) + ttl;
    // The id only feeds coturn's per-user quota; it is never the account itself.
    const id = (await sha256hex(String(subject || 'anon'))).slice(0, 12);
    const username = `${expiresAt}:${id}`;
    const key = await crypto.subtle.importKey('raw', enc.encode(env.TURN_SECRET), { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
    const mac = new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(username)));
    return { iceServers: [{ urls, username, credential: b64encode(mac) }], ttl, expiresAt };
}

/** Bearer value of an Authorization header. */
export const bearerOf = header => String(header || '').replace(/^Bearer\s+/i, '').trim();
