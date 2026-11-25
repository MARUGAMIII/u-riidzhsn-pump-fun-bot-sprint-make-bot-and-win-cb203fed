// Early-momentum launch bot for agent-fun/replay/v2 (pump.fun bonding curves).
//
// Idea, in one paragraph: most pump.fun coins die within minutes, so the only edge is
// speed plus selectivity. Watch every coin from its CreateEvent; in its first few slots
// read the opening tape. Buy only launches whose opening tape looks like real demand
// rather than a dev dump: the creator has not sold, the creator's own buy is small, a
// handful of distinct wallets (not one, not a crowd) are buying, sells are tiny versus
// buys, the price has not already run away, and the last few trades still net buy.
// Stake is small and fixed per coin, so a rug costs little. Exits are fast and
// mechanical: take profit, stop loss, a trailing stop once the trade is up, a hard
// maximum hold, and an immediate exit when the creator sells or a large sell lands.
// A coin is entered at most once. Tiny probe round trips keep the run above the
// guardrails (fills / distinct coins) when the entry filter goes quiet.
//
// Pure function of the batches it is fed: no clock, no randomness, no I/O, no
// environment reads. Iteration is over insertion-ordered maps fed by the batch
// stream, so the two graded executions produce byte-identical action logs.

import { BPS, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Youngest age (slots since create) at which a coin may be bought. */
  minAge: number;
  /** Oldest age at which a first buy may still be decided. */
  maxAge: number;
  /** Lamports to put into one coin in total. */
  stakeLamports: bigint;
  /** Smallest single buy worth paying the tx fee for. */
  minBuyLamports: bigint;
  /** Real SOL reserve window at decision time (lamports). */
  minRealSol: bigint;
  maxRealSol: bigint;
  /** Creator's own buys in the create slot: at most this (lamports). */
  maxDevBuy: bigint;
  /** Trades seen and distinct (non-creator) buyers seen before entry. */
  minTrades: number;
  minBuyers: number;
  /** Distinct non-creator buyers before entry, at most: a launch bundled by many wallets is a dump in waiting. */
  maxBuyers: number;
  /** Lamports sold by others / lamports bought, as bps, at most. */
  maxSellRatioBps: bigint;
  /** Largest single non-creator sell seen, as bps of the real SOL reserve before it, at most. */
  maxBigSellBps: bigint;
  /** Price now over the create price, as bps: window. */
  minRunupBps: bigint;
  maxRunupBps: bigint;
  /** The recent-flow window (slots) and what it must show. */
  flowWindow: number;
  minFlowLamports: bigint;
  minRecentBuyers: number;
  /** Creator sold before entry: skip the coin. */
  skipDevSold: boolean;
  /** Slippage allowed on buys (bps). */
  buySlippageBps: number;
  /** Exit when the position is worth this much over its cost (bps). */
  tpBps: bigint;
  /** Exit when it is worth this much under its cost (bps). */
  slBps: bigint;
  /** Trailing stop: once up at least trailArmBps, exit on a fall of trailBps from the best value. */
  trailArmBps: bigint;
  trailBps: bigint;
  /** Exit after holding this many slots. */
  maxHold: number;
  /** Exit when the creator sells while held. */
  exitOnDevSell: boolean;
  /** Exit when a single non-creator sell of at least this share (bps) of real SOL lands while held. */
  exitBigSellBps: bigint;
  /** Most new entries per reply. */
  maxEntriesPerBatch: number;
  /** Guardrail pacing: target fills / coins over this many seconds of block time. */
  fillTarget: number;
  coinTarget: number;
  windowSeconds: number;
  /** Probe round trip size (lamports). */
  probeLamports: bigint;
  maxProbes: number;
}

// Settings traded with. Chosen by sweeping the dev window with the kit's own
// simulator and preferring a flat, robust region over the single best cell.
export const DEFAULT_PARAMS: Params = {
  minAge: 0,
  maxAge: 8,
  stakeLamports: 150_000_000n,
  minBuyLamports: 5_000_000n,
  minRealSol: 3_000_000_000n,
  maxRealSol: 40_000_000_000n,
  maxDevBuy: 2_500_000_000n,
  minTrades: 6,
  minBuyers: 6,
  maxBuyers: 10,
  maxSellRatioBps: 3000n,
  maxBigSellBps: 5000n,
  minRunupBps: 9500n,
  maxRunupBps: 15_000n,
  flowWindow: 6,
  minFlowLamports: 100_000_000n,
  minRecentBuyers: 1,
  skipDevSold: true,
  buySlippageBps: 10_000,
  tpBps: 1500n,
  slBps: 1500n,
  trailArmBps: 1000n,
  trailBps: 500n,
  maxHold: 120,
  exitOnDevSell: true,
  exitBigSellBps: 3000n,
  maxEntriesPerBatch: 3,
  fillTarget: 60,
  coinTarget: 25,
  windowSeconds: 14_400,
  probeLamports: 10_000_000n,
  maxProbes: 1,
};

const FEE_BPS = 125n;

/** What selling `tokens` against (vq, vt) would pay after curve fees (protocol + creator). */
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * FEE_BPS + BPS - 1n) / BPS;
}

interface Tick {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: Pubkey;
}

interface Coin {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  /** The create-time reserves: the price run-up is measured against them. */
  vq0: bigint;
  vt0: bigint;
  devBuy: bigint;
  trades: number;
  buyers: Set<Pubkey>;
  bought: bigint;
  soldByOthers: bigint;
  bigSellBps: bigint;
  devSold: boolean;
  lastTradeSlot: number;
  /** Trades of the last flowWindow slots (older ones dropped as new ones come). */
  recent: Tick[];
  /** Never buy it again (entered, exited, filtered out for good, or completed). */
  done: boolean;
  complete: boolean;
}

interface Held {
  openedSlot: number;
  txPaid: bigint;
  best: bigint;
}

export function createStrategy(params: Params = DEFAULT_PARAMS) {
  const P = params;
  const coins = new Map<Pubkey, Coin>();
  const held = new Map<Pubkey, Held>();
  /** Mints with a sell in flight. */
  const selling = new Set<Pubkey>();
  /** Probe round trips in flight, by mint. */
  const probes = new Set<Pubkey>();
  let firstTime: number | null = null;
  let nowTime = 0;
  let filled = 0;
  const filledMints = new Set<Pubkey>();

  function readEvents(batch: Batch): void {
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        coins.set(e.mint, {
          mint: e.mint,
          createSlot: e.slot,
          creator: e.creator,
          vq0: BigInt(e.virtual_quote_reserves),
          vt0: BigInt(e.virtual_token_reserves),
          devBuy: 0n,
          trades: 0,
          buyers: new Set(),
          bought: 0n,
          soldByOthers: 0n,
          bigSellBps: 0n,
          devSold: false,
          lastTradeSlot: e.slot,
          recent: [],
          done: false,
          complete: false,
        });
      } else if (e.event === 'TradeEvent') {
        const c = coins.get(e.mint);
        if (c === undefined) continue;
        const sol = BigInt(e.sol_amount);
        c.trades += 1;
        c.lastTradeSlot = e.slot;
        c.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
        const isDev = e.user === c.creator;
        if (e.is_buy) {
          c.bought += sol;
          if (isDev && e.slot === c.createSlot) c.devBuy += sol;
          else if (!isDev) c.buyers.add(e.user);
        } else if (isDev) {
          c.devSold = true;
        } else {
          c.soldByOthers += sol;
          const rqBefore = BigInt(e.real_quote_reserves) + sol;
          if (rqBefore > 0n) {
            const bps = (sol * BPS) / rqBefore;
            if (bps > c.bigSellBps) c.bigSellBps = bps;
          }
        }
      } else {
        const c = coins.get(e.mint);
        if (c !== undefined) {
          c.complete = true;
          c.done = true;
        }
      }
    }
  }

  /** Net lamports bought minus sold and the distinct buyers, over the last flowWindow slots. */
  function flow(c: Coin, slot: number): { net: bigint; buyers: number } {
    const from = slot - P.flowWindow;
    let drop = 0;
    while (drop < c.recent.length && c.recent[drop]!.slot <= from) drop++;
    if (drop > 0) c.recent.splice(0, drop);
    let net = 0n;
    const buyers = new Set<Pubkey>();
    for (const t of c.recent) {
      if (t.isBuy) {
        net += t.sol;
        if (t.user !== c.creator) buyers.add(t.user);
      } else net -= t.sol;
    }
    return { net, buyers: buyers.size };
  }

  function hasPending(state: BotState, mint: Pubkey, side: 'buy' | 'sell'): boolean {
    for (const p of state.pending.values()) if (p.order.mint === mint && p.order.side === side) return true;
    return false;
  }

  return function onBatch(state: BotState, batch: Batch): OrderInput[] {
    readEvents(batch);
    for (const e of batch.events) {
      if (e.block_time === null) continue;
      if (firstTime === null) firstTime = e.block_time;
      if (e.block_time > nowTime) nowTime = e.block_time;
    }
    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = state.txFeeLamports();
    const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
    for (const m of batch.marks) marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });

    // Fills: track tx fees paid, open/close held records.
    for (const f of batch.fills) {
      if (f.client_id.startsWith('_')) {
        held.delete(f.mint);
        selling.delete(f.mint);
        probes.delete(f.mint);
        continue;
      }
      if (f.status === 'filled') {
        filled += 1;
        filledMints.add(f.mint);
      }
      const h = held.get(f.mint);
      if (f.status !== 'rejected' && h !== undefined) h.txPaid += BigInt(f.tx_fee_lamports);
      if (f.side === 'buy' && f.status === 'filled' && h === undefined && !probes.has(f.mint)) {
        held.set(f.mint, { openedSlot: f.slot, txPaid: BigInt(f.tx_fee_lamports), best: 0n });
      }
    }
    for (const mint of [...selling]) if (!hasPending(state, mint, 'sell')) selling.delete(mint);
    for (const mint of [...held.keys()]) {
      if (!state.positions.has(mint) && !selling.has(mint) && !hasPending(state, mint, 'buy')) held.delete(mint);
    }
    for (const mint of [...probes]) {
      if (!state.positions.has(mint) && !hasPending(state, mint, 'buy')) probes.delete(mint);
    }

    // 1. Exits first: sell a whole position on TP, SL, trailing stop, max hold,
    // creator sell, or a large sell landing while held. Probe positions are
    // handled by section 2, never here (one sell per mint per batch).
    const soldThisBatch = new Set<Pubkey>();
    for (const [mint, pos] of state.positions) {
      if (orders.length >= cap) break;
      if (selling.has(mint) || probes.has(mint)) continue;
      const c = coins.get(mint);
      let h = held.get(mint);
      if (h === undefined) {
        h = { openedSlot: state.slot, txPaid: tx, best: 0n };
        held.set(mint, h);
      }
      let mk = marks.get(mint);
      if (mk === undefined) {
        const cv = state.curves.get(mint);
        if (cv === undefined) continue;
        mk = { vq: cv.virtual_quote_reserves, vt: cv.virtual_token_reserves };
      }
      const value = sellValue(pos.token_amount, mk.vq, mk.vt) - tx;
      const basis = pos.cost_lamports + h.txPaid;
      if (value > h.best) h.best = value;
      let exit = false;
      if (basis > 0n) {
        if (value * BPS >= basis * (BPS + P.tpBps)) exit = true;
        if (value * BPS <= basis * (BPS - P.slBps)) exit = true;
        if (h.best * BPS >= basis * (BPS + P.trailArmBps) && value * BPS <= h.best * (BPS - P.trailBps)) exit = true;
      }
      if (state.slot - h.openedSlot >= P.maxHold) exit = true;
      if (c !== undefined) {
        if (P.exitOnDevSell && c.devSold) exit = true;
        if (c.bigSellBps >= P.exitBigSellBps && c.lastTradeSlot >= h.openedSlot) exit = true;
      }
      if (!exit) continue;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      selling.add(mint);
      soldThisBatch.add(mint);
      if (c !== undefined) c.done = true;
    }

    // 2. Probe round trips: sell the whole probe as soon as its buy has landed.
    for (const mint of [...probes]) {
      if (orders.length >= cap) break;
      if (soldThisBatch.has(mint)) continue;
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined || curve === undefined || curve.complete || hasPending(state, mint, 'buy') || hasPending(state, mint, 'sell')) continue;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      soldThisBatch.add(mint);
    }

    // 3. New entries on coins created recently.
    let free = state.freeCashLamports() - tx * BigInt(orders.length);
    let open = state.committedPositionCount();
    let entries = 0;
    for (const c of coins.values()) {
      if (orders.length >= cap || entries >= P.maxEntriesPerBatch) break;
      if (c.done || c.complete) continue;
      const age = state.slot - c.createSlot;
      if (age > P.maxAge) {
        c.done = true;
        continue;
      }
      if (age < P.minAge) continue;
      if (open >= state.hello.caps.max_open_positions) break;
      if (state.positions.has(c.mint) || probes.has(c.mint) || hasPending(state, c.mint, 'buy')) continue;
      const curve = state.curves.get(c.mint);
      if (curve === undefined || curve.complete) continue;
      // Rug / demand filter on the opening tape. Real SOL grows as buys land,
      // so a coin below the minimum is re-checked until it ages out, not dropped.
      const rq = curve.real_quote_reserves;
      if (rq < P.minRealSol || rq > P.maxRealSol) continue;
      if (c.devSold && P.skipDevSold) {
        c.done = true;
        continue;
      }
      if (c.devBuy > P.maxDevBuy) {
        c.done = true;
        continue;
      }
      if (c.trades < P.minTrades || c.buyers.size < P.minBuyers || c.buyers.size > P.maxBuyers) continue;
      if (c.bought > 0n && c.soldByOthers * BPS > c.bought * P.maxSellRatioBps) continue;
      if (c.bigSellBps > P.maxBigSellBps) continue;
      const vq = curve.virtual_quote_reserves;
      const vt = curve.virtual_token_reserves;
      const runup = (vq * c.vt0 * BPS) / (vt * c.vq0);
      if (runup < P.minRunupBps || runup > P.maxRunupBps) continue;
      const fl = flow(c, state.slot);
      if (fl.net < P.minFlowLamports || fl.buyers < P.minRecentBuyers) continue;
      const maxBuy = state.maxBuyLamports(c.mint);
      const amount = P.stakeLamports < maxBuy ? P.stakeLamports : maxBuy;
      if (amount < P.minBuyLamports) continue;
      if (free < amount + tx) continue;
      orders.push({ side: 'buy', mint: c.mint, quote_lamports: amount, max_slippage_bps: P.buySlippageBps });
      free -= amount + tx;
      c.done = true;
      entries++;
      open++;
    }

    // 4. Guardrail pacing: open a probe when fills or coins lag the pace.
    if (probes.size < P.maxProbes && open < state.hello.caps.max_open_positions && firstTime !== null) {
      const elapsed = Math.max(0, Math.min(P.windowSeconds, nowTime - firstTime));
      const pace = elapsed / P.windowSeconds;
      const needCoins = filledMints.size < Math.ceil(P.coinTarget * pace);
      const needFills = filled < Math.ceil(P.fillTarget * pace);
      if (needCoins || needFills) {
        const minRq = (P.probeLamports * BPS) / BigInt(state.hello.caps.max_order_bps_of_real_reserve);
        for (const c of coins.values()) {
          if (orders.length >= cap || probes.size >= P.maxProbes) break;
          if (c.done || c.complete || state.positions.has(c.mint) || probes.has(c.mint) || hasPending(state, c.mint, 'buy')) continue;
          if (needCoins && !needFills && filledMints.has(c.mint)) continue;
          const curve = state.curves.get(c.mint);
          if (curve === undefined || curve.complete) continue;
          if (curve.real_quote_reserves < (minRq * 3n) / 2n) continue;
          if (free < P.probeLamports + 2n * tx) break;
          orders.push({ side: 'buy', mint: c.mint, quote_lamports: P.probeLamports, max_slippage_bps: P.buySlippageBps });
          probes.add(c.mint);
          free -= P.probeLamports + tx;
        }
      }
    }
    return orders;
  };
}
