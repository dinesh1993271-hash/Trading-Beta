'use strict';
// ═══════════════════════════════════════════════════════════════════════════════
// DineshTrade — Server
// Instruments : NIFTY · BANKNIFTY · SENSEX
// Broker      : Angel One SmartAPI
// Deploy      : Railway (Node 18+)
// ═══════════════════════════════════════════════════════════════════════════════

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const axios      = require('axios');
const path       = require('path');
const fs         = require('fs');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 1 — INSTRUMENTS
// ═══════════════════════════════════════════════════════════════════════════════
const INSTRUMENTS = {
  NIFTY: {
    name:       'Nifty 50',
    token:      '99926000',
    exchange:   'NSE',
    strikeGap:  50,
    lotSize:    25,
    expiry:     'weekly', // Thursday
  },
  BANKNIFTY: {
    name:       'Bank Nifty',
    token:      '99926009',
    exchange:   'NSE',
    strikeGap:  100,
    lotSize:    15,
    expiry:     'weekly', // Wednesday
  },
  SENSEX: {
    name:       'Sensex',
    token:      '99919000',
    exchange:   'BSE',
    strikeGap:  100,
    lotSize:    10,
    expiry:     'weekly', // Friday
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 2 — CONFIG (tunable at runtime via /api/tune)
// ═══════════════════════════════════════════════════════════════════════════════
let cfg = {
  // Signal thresholds
  bullMin:        57,    // bull score >= this → CALL direction
  bearMax:        43,    // bull score <= this → PUT direction

  // Breakout filter
  breakoutBuf:    0.10,  // % buffer around prev-5-candle high/low (0.10 = 0.1%)

  // Range / chop filter
  minBodyPct:     0.05,  // minimum avg candle body % to allow trade
  maxVwapDistPct: 0.10,  // max distance from VWAP to classify as "dead range"

  // Risk per trade
  slPct:          0.85,  // SL = entry * slPct  (0.85 → 15% loss)
  targetPct:      1.50,  // target = entry * targetPct (1.50 → 50% gain)
  minHoldMin:     3,     // minimum hold before SL/target check
  maxHoldMin:     45,    // force exit after 45 min

  // Capital allocation
  capital:        100000,
  basePct:        0.12,  // 12% of capital per trade (base)
  medPct:         0.18,  // 18% for medium confidence
  highPct:        0.24,  // 24% for high confidence

  // Timing
  sessionStart:   9*60+20,  // 9:20 AM — first 5 min avoided
  sessionEnd:     15*60,    // 3:00 PM
  cooldownMin:    15,        // minutes between trades per instrument

  // Bot mode
  paperMode:      true,
};

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 3 — STATE
// ═══════════════════════════════════════════════════════════════════════════════
let authToken  = null;
let feedToken  = null;   // for WebSocket
let tokenExp   = null;

let priceCache  = {};    // { NIFTY: { ltp, open, high, low, close, ...signals } }
let candleCache = {};    // { NIFTY: [ { open, high, low, close, vol, time } ] }
let oiCache     = {};    // { NIFTY: { pcr, maxPain, chain: [...] } }
let signalCache = {};    // { NIFTY: { bull, direction, ... } }

let activeInst  = 'NIFTY';
let tickRunning = false;   // guard: only one masterTick at a time
let tickTimer   = null;
let wsConn      = null;
let wsAlive     = false;

let botOn       = false;
let botPaused   = false;

let openTrade   = null;  // current open paper/live trade
let tradeLog    = [];    // all closed trades
let filterLog   = [];    // why trades were rejected

// Consecutive loss guard
let consecLosses = 0;
const MAX_CONSEC = 3;

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 4 — ANGEL ONE AUTH
// ═══════════════════════════════════════════════════════════════════════════════
function angelHeaders() {
  return {
    'Content-Type':       'application/json',
    'Accept':             'application/json',
    'X-UserType':         'USER',
    'X-SourceID':         'WEB',
    'X-ClientLocalIP':    '127.0.0.1',
    'X-ClientPublicIP':   '127.0.0.1',
    'X-MACAddress':       '00:00:00:00:00:00',
    'X-PrivateKey':       process.env.ANGEL_API_KEY || '',
    'Authorization':      `Bearer ${authToken || ''}`,
  };
}

async function login() {
  try {
    const res = await axios.post(
      'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
      {
        clientcode: process.env.ANGEL_CLIENT_ID,
        password:   process.env.ANGEL_PASSWORD,
        totp:       generateTOTP(process.env.ANGEL_TOTP_SECRET),
      },
      { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json',
                   'X-UserType': 'USER', 'X-SourceID': 'WEB',
                   'X-ClientLocalIP': '127.0.0.1', 'X-ClientPublicIP': '127.0.0.1',
                   'X-MACAddress': '00:00:00:00:00:00',
                   'X-PrivateKey': process.env.ANGEL_API_KEY || '' } },
    );
    if (res.data?.status && res.data.data?.jwtToken) {
      authToken = res.data.data.jwtToken;
      feedToken = res.data.data.feedToken || authToken;
      tokenExp  = Date.now() + 6 * 60 * 60 * 1000;
      console.log('✅ Angel One login OK');
      return true;
    }
    console.error('❌ Login failed:', res.data?.message);
    return false;
  } catch (e) {
    console.error('❌ Login error:', e.message);
    return false;
  }
}

async function ensureAuth() {
  if (!authToken || Date.now() > tokenExp - 5 * 60 * 1000) {
    await login();
    if (wsConn) startWebSocket(); // refresh WS with new token
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 5 — TOTP (RFC 6238)
// ═══════════════════════════════════════════════════════════════════════════════
function generateTOTP(secret) {
  if (!secret) return '000000';
  try {
    // base32 decode
    const b32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
    const s = secret.toUpperCase().replace(/=+$/, '');
    let bits = '';
    for (const c of s) {
      const v = b32.indexOf(c);
      if (v < 0) continue;
      bits += v.toString(2).padStart(5, '0');
    }
    const bytes = [];
    for (let i = 0; i + 8 <= bits.length; i += 8) bytes.push(parseInt(bits.slice(i, i+8), 2));
    const key = Buffer.from(bytes);

    // HMAC-SHA1
    const crypto = require('crypto');
    const epoch  = Math.floor(Date.now() / 1000 / 30);
    const msg    = Buffer.alloc(8);
    msg.writeBigUInt64BE(BigInt(epoch));
    const hmac   = crypto.createHmac('sha1', key).update(msg).digest();
    const offset = hmac[19] & 0xf;
    const code   = ((hmac[offset] & 0x7f) << 24 | hmac[offset+1] << 16 | hmac[offset+2] << 8 | hmac[offset+3]) % 1000000;
    return code.toString().padStart(6, '0');
  } catch (e) {
    console.error('TOTP error:', e.message);
    return '000000';
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 6 — BATCH PRICE FETCH (1–2 API calls for all 3 instruments)
// ═══════════════════════════════════════════════════════════════════════════════
async function batchFetch() {
  await ensureAuth();
  if (!authToken) return false;

  // Group by exchange: NSE (NIFTY, BANKNIFTY) + BSE (SENSEX)
  const groups = {};
  for (const [key, inst] of Object.entries(INSTRUMENTS)) {
    if (!groups[inst.exchange]) groups[inst.exchange] = [];
    groups[inst.exchange].push({ key, token: inst.token });
  }

  let anyOk = false;
  for (const [exchange, items] of Object.entries(groups)) {
    try {
      const res = await axios.post(
        'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
        { mode: 'FULL', exchangeTokens: { [exchange]: items.map(i => i.token) } },
        { headers: angelHeaders(), timeout: 8000 },
      );
      const fetched = res.data?.data?.fetched || [];
      for (const q of fetched) {
        const match = items.find(i => i.token === String(q.symbolToken));
        if (!match) continue;
        processQuote(match.key, q);
        anyOk = true;
      }
    } catch (e) {
      console.error(`[FETCH] ${exchange} error:`, e.message);
      for (const { key } of items) {
        if (priceCache[key]) priceCache[key].stale = true;
      }
    }
  }
  return anyOk;
}

// ─── Process raw quote into cache + candles ───────────────────────────────────
function processQuote(key, q) {
  const ltp   = parseFloat(q.ltp);
  const open  = parseFloat(q.open);
  const high  = parseFloat(q.high);
  const low   = parseFloat(q.low);
  const close = parseFloat(q.close);
  const vol   = parseFloat(q.tradeVolume) || 0;
  if (!ltp || ltp < 10) return;

  updateCandles(key, ltp, open, high, low, close, vol);
  const sigs = calcSignals(key, ltp, open, high, low, close, vol);

  priceCache[key] = {
    key, ltp, open, high, low, close, vol,
    change:    parseFloat((ltp - close).toFixed(2)),
    changePct: close ? ((ltp - close) / close * 100).toFixed(2) : '0.00',
    ...sigs,
    stale: false,
    ts:    Date.now(),
  };
  signalCache[key] = sigs;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 7 — CANDLE BUILDER (1-min candles from tick data)
// ═══════════════════════════════════════════════════════════════════════════════
function updateCandles(key, ltp, open, high, low, close, vol) {
  if (!candleCache[key]) candleCache[key] = [];
  const candles = candleCache[key];
  const now     = Date.now();
  const minTs   = Math.floor(now / 60000) * 60000; // floor to current minute

  const last = candles[candles.length - 1];
  if (last && last.time === minTs) {
    // Update current candle
    last.high  = Math.max(last.high, ltp);
    last.low   = Math.min(last.low, ltp);
    last.close = ltp;
    last.vol  += vol;
  } else {
    // New candle — seed from day open/high/low or last close
    candles.push({ time: minTs, open: ltp, high: ltp, low: ltp, close: ltp, vol });
    if (candles.length > 390) candles.shift(); // keep max 1 trading day
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 8 — SIGNAL CALCULATION (EMA, RSI, VWAP, candle structure)
// ═══════════════════════════════════════════════════════════════════════════════
function calcEMA(values, period) {
  if (values.length < period) return values[values.length - 1] || 0;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) ema = values[i] * k + ema * (1 - k);
  return ema;
}

function calcRSI(closes, period = 14) {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  if (losses === 0) return 100;
  const rs = (gains / period) / (losses / period);
  return parseFloat((100 - 100 / (1 + rs)).toFixed(1));
}

function calcVWAP(candles) {
  let sumPV = 0, sumV = 0;
  for (const c of candles) {
    const tp = (c.high + c.low + c.close) / 3;
    sumPV += tp * (c.vol || 1);
    sumV  += (c.vol || 1);
  }
  return sumV > 0 ? sumPV / sumV : 0;
}

function calcSignals(key, ltp, open, high, low, close, vol) {
  const candles = candleCache[key] || [];
  const closes  = candles.map(c => c.close);

  const ema9   = calcEMA(closes, 9);
  const ema21  = calcEMA(closes, 21);
  const rsi    = calcRSI(closes);
  const vwap   = calcVWAP(candles);

  const aboveEMA9  = ltp > ema9;
  const aboveEMA21 = ltp > ema21;
  const aboveVWAP  = vwap > 0 ? ltp > vwap : null;
  const vwapDist   = vwap > 0 ? parseFloat(((ltp - vwap) / vwap * 100).toFixed(2)) : 0;

  // Higher highs / lower lows over last 5 candles
  const last5 = candles.slice(-5);
  const hh = last5.length >= 3 && last5[last5.length-1].high > last5[0].high;
  const ll = last5.length >= 3 && last5[last5.length-1].low  < last5[0].low;

  // Candle body
  const body    = Math.abs(ltp - open);
  const bodyPct = ltp > 0 ? body / ltp * 100 : 0;
  const bullCandle = ltp >= open;

  // Bull score 0–100
  let bull = 50;
  if (aboveEMA9)   bull += 8;
  if (aboveEMA21)  bull += 10;
  if (aboveVWAP)   bull += 10;
  if (rsi > 55)    bull += 8;
  if (rsi > 65)    bull += 6;
  if (hh)          bull += 8;
  if (bullCandle)  bull += 6;
  if (ltp > high * 0.998) bull += 4; // near day high
  if (!aboveEMA9)  bull -= 8;
  if (!aboveEMA21) bull -= 10;
  if (aboveVWAP === false) bull -= 10;
  if (rsi < 45)    bull -= 8;
  if (rsi < 35)    bull -= 6;
  if (ll)          bull -= 8;
  if (!bullCandle) bull -= 6;
  bull = Math.max(0, Math.min(100, bull));

  // Prediction label
  let prediction = 'NEUTRAL';
  if (bull >= 70)      prediction = 'STRONG BULLISH';
  else if (bull >= 57) prediction = 'MILD BULLISH';
  else if (bull <= 30) prediction = 'STRONG BEARISH';
  else if (bull <= 43) prediction = 'MILD BEARISH';

  return {
    bull, rsi, ema9: Math.round(ema9), ema21: Math.round(ema21),
    vwap: Math.round(vwap), aboveVWAP, vwapDist,
    hh, ll, bullCandle, bodyPct: parseFloat(bodyPct.toFixed(3)),
    prediction,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 9 — OPTION CHAIN (OI, PCR, MaxPain)
// ═══════════════════════════════════════════════════════════════════════════════
async function fetchOptionChain(key) {
  await ensureAuth();
  if (!authToken) return;
  const inst = INSTRUMENTS[key];
  try {
    const res = await axios.get(
      `https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionChain?name=${key}&expirydate=${getNearestExpiry(key)}&strikePrice=${getATM(key)}&optionType=CE`,
      { headers: angelHeaders(), timeout: 10000 },
    );
    const data = res.data?.data;
    if (!data) return;

    // Parse chain: build list of { strike, callOI, putOI, callPrem, putPrem }
    const chain = [];
    let totalCallOI = 0, totalPutOI = 0;

    for (const row of (data.fetched || [])) {
      const strike   = parseFloat(row.strikePrice);
      const callOI   = parseFloat(row.CE?.openInterest || 0);
      const putOI    = parseFloat(row.PE?.openInterest || 0);
      const callPrem = parseFloat(row.CE?.lastPrice || 0);
      const putPrem  = parseFloat(row.PE?.lastPrice || 0);
      chain.push({ strike, callOI, putOI, callPrem, putPrem });
      totalCallOI += callOI;
      totalPutOI  += putOI;
    }

    const pcr      = totalCallOI > 0 ? parseFloat((totalPutOI / totalCallOI).toFixed(2)) : 1;
    const maxPain  = calcMaxPain(chain);

    oiCache[key] = { pcr, maxPain, chain, ts: Date.now() };
    io.emit('oiUpdate', { key, pcr, maxPain, chain: chain.slice(0, 20) });
  } catch (e) {
    // Non-fatal — OI is supplementary data
  }
}

function getATM(key) {
  const ltp = priceCache[key]?.ltp;
  if (!ltp) return 0;
  const gap = INSTRUMENTS[key].strikeGap;
  return Math.round(ltp / gap) * gap;
}

function getNearestExpiry(key) {
  // Returns nearest expiry date in DD-MMM-YYYY format
  const now   = new Date(Date.now() + 5.5 * 60 * 60 * 1000); // IST
  const day   = now.getDay();
  const exp   = INSTRUMENTS[key].expiry;
  // Target weekday: NIFTY/BANKNIFTY = Thu(4)/Wed(3), SENSEX = Fri(5)
  const target = key === 'SENSEX' ? 5 : key === 'BANKNIFTY' ? 3 : 4;
  let daysAhead = (target - day + 7) % 7;
  if (daysAhead === 0) daysAhead = 7; // already today → next week
  const expDate = new Date(now.getTime() + daysAhead * 86400000);
  const months  = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
  return `${String(expDate.getDate()).padStart(2,'0')}-${months[expDate.getMonth()]}-${expDate.getFullYear()}`;
}

function calcMaxPain(chain) {
  let minLoss = Infinity, maxPain = 0;
  for (const row of chain) {
    let loss = 0;
    for (const other of chain) {
      if (other.strike < row.strike) loss += other.callOI * (row.strike - other.strike);
      if (other.strike > row.strike) loss += other.putOI  * (other.strike - row.strike);
    }
    if (loss < minLoss) { minLoss = loss; maxPain = row.strike; }
  }
  return maxPain;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 10 — MASTER TICK (price engine)
// ═══════════════════════════════════════════════════════════════════════════════
async function masterTick() {
  if (tickRunning) return;
  tickRunning = true;
  try {
    // ── Phase 1: Fetch all prices (1–2 API calls) ─────────────────────────────
    const ok = await batchFetch();

    // ── Phase 2: Emit to UI ───────────────────────────────────────────────────
    const p = priceCache[activeInst];
    if (p) {
      io.emit('price', {
        ...p,
        candles:  (candleCache[activeInst] || []).slice(-60),
        oi:       oiCache[activeInst] || null,
      });
    }
    // Also emit background instruments (for tab badges)
    for (const key of Object.keys(INSTRUMENTS)) {
      if (key !== activeInst && priceCache[key]) {
        io.emit('priceBg', { key, ltp: priceCache[key].ltp, bull: priceCache[key].bull, stale: priceCache[key].stale });
      }
    }

    // ── Phase 3: Bot logic (only if fetch succeeded, no stale data) ───────────
    if (ok && botOn && !botPaused) {
      await runBotTick();
    }

  } catch (e) {
    console.error('[TICK]', e.message);
  } finally {
    tickRunning = false;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 11 — BOT LOGIC
// ═══════════════════════════════════════════════════════════════════════════════
async function runBotTick() {
  // If there's an open trade — manage it first
  if (openTrade) {
    await manageTrade();
    return;
  }

  // No open trade — scan instruments one at a time (rotating)
  if (!runBotTick._idx) runBotTick._idx = 0;
  const keys = Object.keys(INSTRUMENTS);
  const key  = keys[runBotTick._idx % keys.length];
  runBotTick._idx++;

  await scanInstrument(key);
}

async function scanInstrument(key) {
  const quote   = priceCache[key];
  const candles = candleCache[key] || [];
  if (!quote || quote.stale) return;

  const { bull } = quote;

  console.log(`🔍 [SCAN] ${key} bull=${bull} candles=${candles.length}`);

  // ── Guard: minimum candles ────────────────────────────────────────────────
  if (candles.length < 5) {
    console.log(`⏳ [SKIP] ${key} — only ${candles.length} candles`);
    return;
  }

  // ── Guard: trading hours ──────────────────────────────────────────────────
  const istNow  = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  const istMin  = istNow.getHours() * 60 + istNow.getMinutes();
  if (istMin < cfg.sessionStart || istMin > cfg.sessionEnd) {
    console.log(`⏳ [SKIP] ${key} — outside trading hours (${istMin} min)`);
    return;
  }

  // ── Guard: cooldown ───────────────────────────────────────────────────────
  const lastTrade = tradeLog.filter(t => t.key === key).slice(-1)[0];
  if (lastTrade) {
    const minsSince = (Date.now() - lastTrade.exitTs) / 60000;
    if (minsSince < cfg.cooldownMin) {
      console.log(`⏳ [COOLDOWN] ${key} — ${minsSince.toFixed(1)} min since last trade`);
      return;
    }
  }

  // ── Guard: consecutive losses ─────────────────────────────────────────────
  if (consecLosses >= MAX_CONSEC) {
    console.log(`🛑 [PAUSED] ${consecLosses} consecutive losses — bot paused`);
    botPaused = true;
    io.emit('botStatus', { on: botOn, paused: true, reason: `${MAX_CONSEC} consecutive losses` });
    return;
  }

  // ── Direction ─────────────────────────────────────────────────────────────
  let dir = null;
  if (bull >= cfg.bullMin) dir = 'CALL';
  if (bull <= cfg.bearMax) dir = 'PUT';
  if (!dir) {
    console.log(`⏳ [NEUTRAL] ${key} bull=${bull} — no direction`);
    return;
  }

  // ── Range / chop filter ───────────────────────────────────────────────────
  const last5bodies = candles.slice(-5).map(c => Math.abs(c.close - c.open) / c.close * 100);
  const avgBody     = last5bodies.reduce((a, b) => a + b, 0) / last5bodies.length;
  const vwapDistAbs = Math.abs(quote.vwapDist || 0);
  if (avgBody < cfg.minBodyPct && vwapDistAbs < cfg.maxVwapDistPct) {
    logFilter(key, dir, bull, `Dead range — avg body ${avgBody.toFixed(3)}%, VWAP dist ${vwapDistAbs.toFixed(2)}%`);
    return;
  }

  // ── Breakout filter ───────────────────────────────────────────────────────
  const prev5 = candles.slice(-6, -1);
  if (prev5.length >= 3) {
    const p5High = Math.max(...prev5.map(c => c.high));
    const p5Low  = Math.min(...prev5.map(c => c.low));
    const buf    = p5High * (cfg.breakoutBuf / 100);

    if (dir === 'CALL' && quote.ltp < p5High - buf) {
      logFilter(key, dir, bull, `No breakout — ₹${quote.ltp} vs high ₹${p5High} (need within ${cfg.breakoutBuf}%)`);
      return;
    }
    if (dir === 'PUT' && quote.ltp > p5Low + buf) {
      logFilter(key, dir, bull, `No breakdown — ₹${quote.ltp} vs low ₹${p5Low} (need within ${cfg.breakoutBuf}%)`);
      return;
    }
  }

  // ── All filters passed — place trade ─────────────────────────────────────
  await placeTrade(key, quote, dir, bull);
}

function logFilter(key, dir, bull, reason) {
  const entry = {
    time:   new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }),
    key, dir, bull, reason,
  };
  console.log(`🚫 [FILTER] ${key} ${dir}: ${reason}`);
  filterLog.unshift(entry);
  if (filterLog.length > 100) filterLog.pop();
  io.emit('filterLog', entry);
}

// ─── Place trade ──────────────────────────────────────────────────────────────
async function placeTrade(key, quote, dir, bull) {
  const inst   = INSTRUMENTS[key];
  const gap    = inst.strikeGap;
  const atm    = Math.round(quote.ltp / gap) * gap;

  // Strike selection: ATM for high confidence, slight OTM for low
  let strike = atm;
  if (bull < 63 && dir === 'CALL') strike = atm + gap;      // slight OTM call
  if (bull > 37 && dir === 'PUT')  strike = atm - gap;      // slight OTM put

  // Get option premium — try chain first, then estimate
  let entry = null;
  const chainRow = oiCache[key]?.chain?.find(r => r.strike === strike);
  if (chainRow) {
    entry = dir === 'CALL' ? chainRow.callPrem : chainRow.putPrem;
  }
  if (!entry || entry < 5) {
    // Estimate: ~0.5% of spot for ATM option as fallback
    entry = Math.round(quote.ltp * 0.005);
  }
  if (!entry || entry < 1) return;

  // Lot sizing
  const confidence = bull >= 70 || bull <= 30 ? 'HIGH' : bull >= 63 || bull <= 37 ? 'MED' : 'BASE';
  const pct        = confidence === 'HIGH' ? cfg.highPct : confidence === 'MED' ? cfg.medPct : cfg.basePct;
  const capital    = cfg.capital * pct;
  const lotCost    = entry * inst.lotSize;
  const lots       = Math.max(1, Math.floor(capital / lotCost));

  const sl     = Math.round(entry * cfg.slPct);
  const target = Math.round(entry * cfg.targetPct);

  openTrade = {
    id:         Date.now(),
    key, dir, strike,
    entry, sl, target, lots,
    entrySpot:  quote.ltp,
    entryTs:    Date.now(),
    expiry:     getNearestExpiry(key),
    confidence,
    paperMode:  cfg.paperMode,
    currentPrem: entry,
    pnl:        0,
  };

  console.log(`✅ [TRADE] ${key} ${dir} ${strike} @ ₹${entry} | SL ₹${sl} | Target ₹${target} | ${lots} lot(s) | ${cfg.paperMode ? 'PAPER' : 'LIVE'}`);
  io.emit('tradeOpen', openTrade);
}

// ─── Manage open trade (SL / target / time exit) ─────────────────────────────
async function manageTrade() {
  if (!openTrade) return;
  const { key, dir, strike, entry, sl, target, entryTs, lots } = openTrade;
  const quote    = priceCache[key];
  if (!quote || quote.stale) return;

  // Get current premium — chain cache first, then delta estimate
  let prem = null;
  const chainRow = oiCache[key]?.chain?.find(r => r.strike === strike);
  if (chainRow) {
    prem = dir === 'CALL' ? chainRow.callPrem : chainRow.putPrem;
  }
  if (!prem || prem < 1) {
    // Delta estimate from spot move
    const spotMove = (quote.ltp - openTrade.entrySpot) / openTrade.entrySpot;
    const premMove = dir === 'CALL' ? spotMove * 3 : -spotMove * 3;
    prem = Math.max(1, Math.round(entry * (1 + premMove)));
  }

  openTrade.currentPrem = prem;
  openTrade.pnl = (prem - entry) * lots * INSTRUMENTS[key].lotSize;

  const heldMin  = (Date.now() - entryTs) / 60000;
  const heldOk   = heldMin >= cfg.minHoldMin;

  let exitReason = null;
  if (heldOk && prem <= sl)     exitReason = 'SL_HIT';
  if (heldOk && prem >= target) exitReason = 'TARGET_HIT';
  if (heldMin >= cfg.maxHoldMin) exitReason = 'TIME_EXIT';

  io.emit('tradeUpdate', { ...openTrade, heldMin: Math.round(heldMin) });

  if (exitReason) await closeTrade(exitReason, prem);
}

async function closeTrade(reason, exitPrem) {
  if (!openTrade) return;
  const trade = {
    ...openTrade,
    exitPrem,
    exitTs:    Date.now(),
    exitReason: reason,
    finalPnl:  (exitPrem - openTrade.entry) * openTrade.lots * INSTRUMENTS[openTrade.key].lotSize,
  };
  const won = trade.finalPnl > 0;

  if (won) consecLosses = 0;
  else     consecLosses++;

  tradeLog.unshift(trade);
  if (tradeLog.length > 500) tradeLog.pop();

  console.log(`🏁 [EXIT] ${trade.key} ${trade.dir} — ${reason} @ ₹${exitPrem} | PnL: ₹${trade.finalPnl.toFixed(0)}`);
  io.emit('tradeClose', trade);
  openTrade = null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 12 — ANGEL ONE WEBSOCKET (1-second real-time ticks)
// ═══════════════════════════════════════════════════════════════════════════════
function startWebSocket() {
  let WS;
  try { WS = require('ws'); } catch (e) {
    console.warn('⚠️ ws package not found — run: npm install ws');
    return;
  }
  if (wsConn) { try { wsConn.close(); } catch (e) {} }

  const ws = new WS('wss://smartapisocket.angelone.in/smart-stream', {
    headers: {
      Authorization:   `Bearer ${authToken}`,
      'x-api-key':     process.env.ANGEL_API_KEY || '',
      'x-client-code': process.env.ANGEL_CLIENT_ID || '',
      'x-feed-token':  feedToken || authToken,
    },
  });

  ws.on('open', () => {
    wsAlive = true;
    wsConn  = ws;
    console.log('✅ [WS] SmartStream connected');
    subscribeWS(ws, activeInst);
    // Heartbeat every 25s
    ws._hb = setInterval(() => { if (ws.readyState === 1) ws.send('ping'); }, 25000);
  });

  ws.on('message', (raw) => {
    try {
      if (typeof raw === 'string') return; // pong / json ack — ignore
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      if (buf.length < 49) return;

      // Angel One binary tick: bytes 41-48 = LTP as int64 * 100
      const ltpRaw = buf.readBigInt64BE(41);
      const ltp    = Number(ltpRaw) / 100;
      if (!ltp || ltp < 10) return;

      // Token is padded in bytes 3-24
      const token = buf.slice(3, 25).toString('utf8').replace(/\0/g, '').trim();
      const key   = Object.keys(INSTRUMENTS).find(k => INSTRUMENTS[k].token === token);
      if (!key) return;

      // Update cache with WS tick
      const cached = priceCache[key];
      if (!cached) return;
      updateCandles(key, ltp, cached.open, Math.max(cached.high||ltp,ltp), Math.min(cached.low||ltp,ltp), cached.close, 0);
      priceCache[key] = {
        ...cached, ltp,
        high: Math.max(cached.high||ltp, ltp),
        low:  Math.min(cached.low||ltp, ltp),
        change:    parseFloat((ltp - cached.close).toFixed(2)),
        changePct: cached.close ? ((ltp - cached.close)/cached.close*100).toFixed(2) : '0.00',
        stale: false, ts: Date.now(),
      };

      // Emit immediately for active instrument
      if (key === activeInst) {
        io.emit('price', {
          ...priceCache[key],
          candles: (candleCache[key] || []).slice(-60),
          oi: oiCache[key] || null,
        });
      } else {
        io.emit('priceBg', { key, ltp, bull: priceCache[key].bull, stale: false });
      }
    } catch (e) { /* non-fatal binary parse */ }
  });

  ws.on('error', (e) => console.warn('[WS] Error:', e.message));
  ws.on('close', (code) => {
    wsAlive = false;
    wsConn  = null;
    if (ws._hb) clearInterval(ws._hb);
    console.warn(`[WS] Closed (${code}) — reconnecting in 5s`);
    setTimeout(() => { if (authToken) startWebSocket(); }, 5000);
  });
}

function subscribeWS(ws, key) {
  if (!ws || ws.readyState !== 1) return;
  const inst = INSTRUMENTS[key];
  ws.send(JSON.stringify({
    correlationID: `sub_${key}_${Date.now()}`,
    action: 1,
    params: {
      mode: 1, // LTP only — fastest
      tokenList: [{ exchangeType: inst.exchange === 'BSE' ? 2 : 1, tokens: [inst.token] }],
    },
  }));
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 13 — API ROUTES
// ═══════════════════════════════════════════════════════════════════════════════
// ─── Switch active instrument ─────────────────────────────────────────────────
app.post('/api/instrument', (req, res) => {
  const { key } = req.body;
  if (!INSTRUMENTS[key]) return res.json({ ok: false, error: 'Unknown instrument' });
  activeInst = key;
  if (wsConn && wsConn.readyState === 1) subscribeWS(wsConn, key);
  // Emit cached data immediately
  const p = priceCache[key];
  if (p) io.emit('price', { ...p, candles: (candleCache[key]||[]).slice(-60), oi: oiCache[key]||null, stale: true });
  res.json({ ok: true, key });
});

// ─── Bot control ──────────────────────────────────────────────────────────────
app.post('/api/bot/start', (req, res) => {
  botOn     = true;
  botPaused = false;
  consecLosses = 0;
  console.log('🤖 Bot STARTED');
  io.emit('botStatus', { on: true, paused: false });
  res.json({ ok: true });
});

app.post('/api/bot/stop', (req, res) => {
  botOn = false;
  console.log('🤖 Bot STOPPED');
  io.emit('botStatus', { on: false, paused: false });
  res.json({ ok: true });
});

app.post('/api/bot/resume', (req, res) => {
  botPaused    = false;
  consecLosses = 0;
  console.log('🤖 Bot RESUMED');
  io.emit('botStatus', { on: botOn, paused: false });
  res.json({ ok: true });
});

// ─── Manual trade exit ────────────────────────────────────────────────────────
app.post('/api/trade/exit', async (req, res) => {
  if (!openTrade) return res.json({ ok: false, error: 'No open trade' });
  const p = priceCache[openTrade.key];
  await closeTrade('MANUAL_EXIT', openTrade.currentPrem || openTrade.entry);
  res.json({ ok: true });
});

// ─── Config tuning ────────────────────────────────────────────────────────────
app.post('/api/tune', (req, res) => {
  const allowed = ['bullMin','bearMax','breakoutBuf','slPct','targetPct','minHoldMin',
                   'maxHoldMin','cooldownMin','basePct','medPct','highPct','capital',
                   'sessionStart','sessionEnd','minBodyPct','maxVwapDistPct','paperMode'];
  for (const [k, v] of Object.entries(req.body)) {
    if (allowed.includes(k)) cfg[k] = typeof cfg[k] === 'boolean' ? !!v : parseFloat(v);
  }
  io.emit('cfg', cfg);
  res.json({ ok: true, cfg });
});

// ─── State snapshots ──────────────────────────────────────────────────────────
app.get('/api/state', (req, res) => {
  res.json({
    prices:      priceCache,
    openTrade,
    tradeLog:    tradeLog.slice(0, 50),
    filterLog:   filterLog.slice(0, 50),
    cfg,
    botOn, botPaused, consecLosses,
    activeInst,
    wsAlive,
  });
});

app.get('/api/oi/:key', async (req, res) => {
  const key = req.params.key.toUpperCase();
  if (!INSTRUMENTS[key]) return res.json({ ok: false });
  await fetchOptionChain(key);
  res.json({ ok: true, oi: oiCache[key] || null });
});

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/',       (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/health', (req, res) => res.json({ ok: true, uptime: process.uptime() }));

// ─── Global error handler ─────────────────────────────────────────────────────
process.on('uncaughtException',  (e) => console.error('💥 Uncaught:', e));
process.on('unhandledRejection', (e) => console.error('💥 Unhandled:', e));

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 14 — SOCKET.IO (real-time client sync)
// ═══════════════════════════════════════════════════════════════════════════════
io.on('connection', (socket) => {
  console.log(`[WS] Client connected: ${socket.id}`);

  // Send full state on connect
  socket.emit('init', {
    prices:    priceCache,
    openTrade,
    tradeLog:  tradeLog.slice(0, 50),
    filterLog: filterLog.slice(0, 50),
    cfg,
    botOn, botPaused, consecLosses,
    activeInst, wsAlive,
    instruments: Object.entries(INSTRUMENTS).map(([k,v]) => ({ key: k, name: v.name })),
  });

  socket.on('disconnect', () => console.log(`[WS] Client disconnected: ${socket.id}`));
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 15 — STARTUP
// ═══════════════════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 8080;

// Listen FIRST — so Railway healthcheck passes immediately
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 DineshTrade live on port ${PORT}`);
});

// Then init in background — login failure won't crash the server
(async () => {
  try {
    const ok = await login();
    if (!ok) {
      console.error('❌ Angel One login failed — check env vars. Retrying in 30s...');
      setTimeout(async () => {
        const retry = await login();
        if (retry) await initSystems();
      }, 30000);
      return;
    }
    await initSystems();
  } catch (e) {
    console.error('💥 Startup error:', e);
  }
})();

async function initSystems() {
  // Initial price fetch
  await batchFetch();

  // Option chain for active instrument
  setTimeout(() => fetchOptionChain(activeInst), 3000);

  // Master tick every 5 seconds
  tickTimer = setInterval(masterTick, 5000);
  masterTick();

  // WebSocket for 1-second ticks
  startWebSocket();

  // Option chain refresh every 3 minutes
  setInterval(() => {
    for (const key of Object.keys(INSTRUMENTS)) fetchOptionChain(key);
  }, 3 * 60 * 1000);

  // Re-login every 6 hours
  setInterval(async () => {
    await login();
    startWebSocket();
  }, 6 * 60 * 60 * 1000);

  console.log('✅ All systems go. Tick: 5s | WS: active | OI: 3min');
}
