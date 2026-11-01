// Test-only entry for `wrangler dev`: an agent that dials /agent?legacy=1 is
// routed as if LOCATION_HINT were unset, i.e. into the un-hinted tunnel a
// desktop is still in after an operator sets a hint. Phones use the real
// routing, so this exercises the fallback to legacy tunnels.
import worker, { TunnelDO } from '../cloudflare/worker.mjs';

export { TunnelDO };

export default {
    fetch(request, env, ctx) {
        if (new URL(request.url).searchParams.has('legacy')) env = { ...env, LOCATION_HINT: '' };
        return worker.fetch(request, env, ctx);
    },
};
