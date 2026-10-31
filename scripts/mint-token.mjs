#!/usr/bin/env node
// Mint a relay token for one user of a relay that sets RELAY_TOKEN_SECRET.
//
//   RELAY_TOKEN_SECRET=… node scripts/mint-token.mjs <user-id> [days]
//
// Paste the printed token into Alma → Settings → Remote Access. Rotate
// RELAY_TOKEN_SECRET to revoke every token at once; set `days` to make them
// expire on their own (default: 365).
import { createHmac } from 'node:crypto';
import { RELAY_AUDIENCE } from '../shared.mjs';

const [sub, daysArg] = process.argv.slice(2);
const secret = process.env.RELAY_TOKEN_SECRET;
if (!sub || !secret) {
    console.error('usage: RELAY_TOKEN_SECRET=… node scripts/mint-token.mjs <user-id> [days]');
    process.exit(2);
}
const days = Number(daysArg || 365);
const now = Math.floor(Date.now() / 1000);
const b64url = obj => Buffer.from(JSON.stringify(obj)).toString('base64url');
const input = `${b64url({ alg: 'HS256', typ: 'JWT' })}.${b64url({ sub, aud: RELAY_AUDIENCE, iat: now, exp: now + Math.round(days * 86400) })}`;
console.log(`${input}.${createHmac('sha256', secret).update(input).digest('base64url')}`);
