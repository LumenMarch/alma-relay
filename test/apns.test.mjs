// node --test test
import assert from 'node:assert/strict';
import test from 'node:test';

import { apnsRequestHeaders } from '../apns.mjs';

test('an alert goes to the app topic', () => {
    const h = apnsRequestHeaders('jwt', 'me.yetone.alma', { collapseId: 'x' });
    assert.equal(h['apns-topic'], 'me.yetone.alma');
    assert.equal(h['apns-push-type'], 'alert');
    assert.equal(h['apns-priority'], '10');
    assert.equal(h['apns-collapse-id'], 'x');
});

test('a Live Activity update goes to its own topic', () => {
    const h = apnsRequestHeaders('jwt', 'me.yetone.alma', { pushType: 'liveactivity', priority: 5 });
    assert.equal(h['apns-topic'], 'me.yetone.alma.push-type.liveactivity');
    assert.equal(h['apns-push-type'], 'liveactivity');
    assert.equal(h['apns-priority'], '5');
});
