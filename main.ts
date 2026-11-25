// Entry point for the Pump.fun Bot Sprint (agent-fun/replay/v2).
// Run by the grader as `node /bot/main.ts` (Node 22 strips the types; no build,
// no install). The strategy lives in strategy.ts; the protocol shim is the kit's
// own sdk/ts file, vendored under vendor/ as its header instructs.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot({
  onBatch: createStrategy(DEFAULT_PARAMS),
});
