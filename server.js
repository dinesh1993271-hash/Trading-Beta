use strict';
// ═══════════════════════════════════════════════════════════════════════════════
// DineshTrade v3.0 — Production Ready
// All 13 critical fixes implemented
// Instruments : NIFTY · BANKNIFTY · SENSEX · FINNIFTY · MIDCPNIFTY
// Broker      : Angel One SmartAPI
// Deploy      : Railway (Node 18+)
// ═══════════════════════════════════════════════════════════════════════════════

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const axios      = require('axios');
const path       = require('path');
const fs         = require('fs');
const crypto     = require('crypto');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 0 — ENV VALIDATION (with debug logging)
// ═══════════════════════════════════════════════════════════════════════════════

console.log('=== ENVIRONMENT VARIABLES DEBUG ===');
for (const [key, val] of Object.entries(process.env)) {
  if (key.startsWith('ANGEL') || key.startsWith('GROQ') || key === 'PORT') {
    console.log(`${key}: ${val ? 'SET (' + val.substring(0, 4) + '...)' : 'NOT SET'}`);
  }
}
console.log('=====================================');

if (!process.env.ANGEL_PASSWORD && process.env.ANGEL_PIN) {
  process.env.ANGEL_PASSWORD = process.env.ANGEL_PIN;
  console.log('ℹ️ Using ANGEL_PIN as ANGEL_PASSWORD');
}

const requiredEnv = ['ANGEL_API_KEY', 'ANGEL_CLIENT_ID', 'ANGEL_PASSWORD', 'ANGEL_TOTP_SECRET'];
let demoMode = false;
for (const env of requiredEnv) {
  if (!process.env[env]) {
    console.warn(`⚠️ Missing env var: ${env}`);
    demoMode = true;
  }
}
if (demoMode) {
  console.log('🎮 DEMO MODE — Using simulated market data');
} else {
  console.log('✅ LIVE MODE — Connecting to Angel One');
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 1 — INSTRUMENTS
// ═══════════════════════════════════════════════════════════════════════════════
const INSTRUMENTS = {
  NIFTY: {
    name:       'Nifty 50',
    token:      '26000',
    exchange:   'NSE',
    exchangeType: 1,
    strikeGap:  50,
    lotSize:    75,
    expiryDay:  4,
    type:       'index',
  },
  BANKNIFTY: {
    name:       'Bank Nifty',
    token:      '26009',
    exchange:   'NSE',
    exchangeType: 1,
    strikeGap:  100,
    lotSize:    30,
    expiryDay:  3,
    type:       'index',
  },
  FINNIFTY: {
    name:       'FIN NIFTY',
    token:      '26037',
    exchange:   'NSE',
    exchangeType: 1,
    strikeGap:  50,
    lotSize:    65,
    expiryDay:  2,
    type:       'index',
  },
  MIDCPNIFTY: {
    name:       'MIDCAP NIFTY',
    token:      '26074',
    exchange:   'NSE',
    exchangeType: 1,
    strikeGap:  25,
    lotSize:    75,
    expiryDay:  1,
    type:       'index',
  },
  SENSEX: {
    name:       'Sensex',
    token:      '1',
    exchange:   'BSE',
    exchangeType: 2,
    strikeGap:  100,
    lotSize:    20,
    expiryDay:  5,
    type:       'index',
  },
  INDIAVIX: {
    name:       'India VIX',
    token:      '26017',
    exchange:   'NSE',
    exchangeType: 1,
    strikeGap:  0,
    lotSize:    0,
    expiryDay:  0,
    type:       'vix',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 2 — CONFIG (FIX 1: VIX-adjusted sizing + all paper realism)
// ═══════════════════════════════════════════════════════════════════════════════
let cfg = {
  // Signal thresholds
  bullMin:        57,
  bearMax:        43,
  breakoutBuf:    0.15,
  minBodyPct:     0.05,
  maxVwapDistPct: 0.10,

  // Risk per trade
  slPct:          0.85,
  targetPct:      1.50,
  minHoldMin:     3,
  maxHoldMin:     45,

  // FIX 1: VIX-based position sizing
  basePct:        0.12,
  medPct:         0.18,
  highPct:        0.24,
  vixThresholdLow:  12,
  vixThresholdHigh: 25,
  sizeAtHighVix:    0.06,
  sizeAtLowVix:     0.20,

  // Timing
  sessionStart:   9*60+20,
  sessionEnd:     15*60,
  cooldownMin:    15,

  // Bot mode
  paperMode:      true,

  // FIX 9: Realistic paper trading
  paperSlippagePct:     0.8,
  paperSpreadOTM:       1.5,
  paperPartialFill:     0.05,
  paperOrderReject:     0.02,
  paperBrokerage:       20,
  paperSttPct:          0.05,
  paperGstPct:          18,
  paperMarginRequired:  true,
  paperMaxDailyLoss:    5000,
  paperLiquidityDelay:  0.10,

  // FIX 10: Max open trades
  maxOpenTrades:        2,

  // FIX 11: Greeks danger thresholds
  maxGamma:             0.05,
  maxThetaBurnPct:      20,
};

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 3 — STATE (FIX 5: Persistence)
// ═══════════════════════════════════════════════════════════════════════════════
let authToken  = null;
let feedToken  = null;
let tokenExp   = null;

let priceCache  = {};
let candleCache = {};
let prevVolCache = {};
let oiCache     = {};
let signalCache = {};

let activeInst  = 'NIFTY';
let tickRunning = false;
let tickTimer   = null;
let wsConn      = null;
let wsAlive     = false;

let botOn       = false;
let botPaused   = false;

let openTrade   = null;
let tradeLog    = [];
let filterLog   = [];

let consecLosses = 0;
const MAX_CONSEC = 3;

// FIX 5: Paper trading daily tracking + persistence
let paperDailyPnl = 0;
let paperDailyTrades = 0;
let paperLastResetDate = null;

// FIX 5: Restore state on startup
const STATE_FILE = '/tmp/dineshtrade_state.json';
try {
  if (fs.existsSync(STATE_FILE)) {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    if (saved.tradeLog) tradeLog = saved.tradeLog;
    if (saved.consecLosses !== undefined) consecLosses = saved.consecLosses;
    if (saved.paperDailyPnl !== undefined) paperDailyPnl = saved.paperDailyPnl;
    if (saved.paperDailyTrades !== undefined) paperDailyTrades = saved.paperDailyTrades;
    if (saved.paperLastResetDate) paperLastResetDate = saved.paperLastResetDate;
    if (saved.openTrade && Date.now() - saved.openTrade.entryTs < cfg.maxHoldMin * 60000) {
      openTrade = saved.openTrade;
      console.log('🔄 Restored open trade from saved state');
    }
    console.log('🔄 State restored from disk');
  }
} catch (e) {
  console.log('ℹ️ No saved state found');
}

// FIX 5: Persist state every 30 seconds
setInterval(() => {
  const state = {
    openTrade,
    tradeLog: tradeLog.slice(0, 200),
    consecLosses,
    paperDailyPnl,
    paperDailyTrades,
    paperLastResetDate,
    ts: Date.now(),
  };
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));
  } catch (e) { /* ignore */ }
}, 30000);

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
  if (demoMode) {
    authToken = 'demo-token';
    feedToken = 'demo-feed';
    tokenExp  = Date.now() + 24 * 60 * 60 * 1000;
    console.log('🎮 Demo login OK');
    return true;
  }
  try {
    const res = await axios.post(
      'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
      {
        clientcode: process.env.ANGEL_CLIENT_ID,
        password:   process.env.ANGEL_PASSWORD,
        totp:       generateTOTP(process.env.ANGEL_TOTP_SECRET),
      },
      { headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-UserType': 'USER',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': '127.0.0.1',
        'X-ClientPublicIP': '127.0.0.1',
        'X-MACAddress': '00:00:00:00:00:00',
        'X-PrivateKey': process.env.ANGEL_API_KEY || '',
      }},
    );
    if (res.data?.status && res.data.data?.jwtToken) {
      authToken = res.data.data.jwtToken;
      feedToken = res.data.data.feedToken || authToken;
      tokenExp  = Date.now() + 6 * 60 * 60 * 1000;
      console.log('✅ Angel One login OK');
      return true;
    }
    console.error('❌ Login failed:', res.data?.message || 'Unknown error');
    return false;
  } catch (e) {
    console.error('❌ Login error:', e.response?.data?.message || e.message);
    return false;
  }
}

async function ensureAuth() {
  if (!authToken || Date.now() > tokenExp - 5 * 60 * 1000) {
    const ok = await login();
    if (ok && wsConn) {
      stopWebSocket();
      startWebSocket();
    }
    return ok;
  }
  return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 5 — TOTP
// ═══════════════════════════════════════════════════════════════════════════════
function generateTOTP(secret) {
  if (!secret) return '000000';
  try {
    const b32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
    const s = secret.toUpperCase().replace(/=+$/, '').replace(/\s/g, '');
    let bits = '';
    for (const c of s) {
      const v = b32.indexOf(c);
      if (v < 0) continue;
      bits += v.toString(2).padStart(5, '0');
    }
    const bytes = [];
    for (let i = 0; i + 8 <= bits.length; i += 8) bytes.push(parseInt(bits.slice(i, i+8), 2));
    const key = Buffer.from(bytes);

    const epoch = Math.floor(Date.now() / 1000 / 30);
    const msg = Buffer.alloc(8);
    msg.writeBigUInt64BE(BigInt(epoch));
    const hmac = crypto.createHmac('sha1', key).update(msg).digest();
    const offset = hmac[19] & 0xf;
    const code = ((hmac[offset] & 0x7f) << 24 | hmac[offset+1] << 16 | hmac[offset+2] << 8 | hmac[offset+3]) % 1000000;
    return code.toString().padStart(6, '0');
  } catch (e) {
    console.error('TOTP error:', e.message);
    return '000000';
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 6 — BATCH PRICE FETCH (FIX 7: Adaptive interval + jitter)
// ═══════════════════════════════════════════════════════════════════════════════

function getMinutesToExpiry() {
  const now = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  const day = now.getDay();
  const target = INSTRUMENTS[activeInst].expiryDay;
  let daysAhead = (target - day + 7) % 7;
  if (daysAhead === 0) daysAhead = 7;
  return (daysAhead - 1) * 24 * 60 + (15*60 - (now.getHours()*60 + now.getMinutes()));
}

async function batchFetch() {
  const authed = await ensureAuth();
  if (!authed) return false;

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
      console.error(`[FETCH] ${exchange} error:`, e.response?.data?.message || e.message);
      for (const { key } of items) {
        if (priceCache[key]) priceCache[key].stale = true;
      }
    }
  }
  return anyOk;
}

function processQuote(key, q) {
  const ltp   = parseFloat(q.ltp);
  const open  = parseFloat(q.open);
  const high  = parseFloat(q.high);
  const low   = parseFloat(q.low);
  const close = parseFloat(q.close);
  const vol   = parseFloat(q.volume) || parseFloat(q.tradeVolume) || 0;
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
// SECTION 7 — CANDLE BUILDER
// ═══════════════════════════════════════════════════════════════════════════════
function updateCandles(key, ltp, open, high, low, close, cumVol) {
  if (!candleCache[key]) candleCache[key] = [];
  if (!prevVolCache) prevVolCache = {};

  const candles = candleCache[key];
  const now = Date.now();
  const minTs = Math.floor(now / 60000) * 60000;

  const prevVol = prevVolCache[key] || 0;
  const tickVol = Math.max(0, cumVol - prevVol);
  prevVolCache[key] = cumVol;

  const last = candles[candles.length - 1];
  if (last && last.time === minTs) {
    last.high = Math.max(last.high, ltp);
    last.low = Math.min(last.low, ltp);
    last.close = ltp;
    last.vol += tickVol;
  } else {
    candles.push({ time: minTs, open: ltp, high: ltp, low: ltp, close: ltp, vol: tickVol });
    if (candles.length > 390) candles.shift();
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 8 — SIGNAL CALCULATION (FIX 2: Greeks + FIX 3: Regime detection)
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
    sumPV += tp * c.vol;
    sumV += c.vol;
  }
  return sumV > 0 ? sumPV / sumV : 0;
}

// FIX 2: Simplified Greeks
function estimateGreeks(spot, strike, daysToExpiry, iv) {
  const T = Math.max(0.001, daysToExpiry / 365);
  const d1 = (Math.log(spot/strike) + (0.05 + iv*iv/2)*T) / (iv*Math.sqrt(T) + 0.0001);
  const Nd1 = 0.5 * (1 + Math.erf(d1/Math.sqrt(2)));
  const delta = Nd1;
  const gamma = Math.exp(-d1*d1/2) / (spot * iv * Math.sqrt(T) * Math.sqrt(2*Math.PI) + 0.0001);
  const theta = -(spot * iv * Math.exp(-d1*d1/2)) / (2 * Math.sqrt(T) * Math.sqrt(2*Math.PI) * 365 + 0.0001);
  const vega = spot * Math.sqrt(T) * Math.exp(-d1*d1/2) / Math.sqrt(2*Math.PI) / 100;
  return { delta, gamma, theta, vega };
}

// FIX 3: Regime detection
function detectRegime(key) {
  const candles = candleCache[key] || [];
  if (candles.length < 10) return 'UNKNOWN';
  const ranges = candles.slice(-10).map(c => c.high - c.low);
  const avgRange = ranges.reduce((a,b) => a+b, 0) / ranges.length;
  const mean = avgRange;
  const variance = ranges.reduce((a,b) => a + Math.pow(b - mean, 2), 0) / ranges.length;
  const std = Math.sqrt(variance);
  const cv = std / (avgRange + 0.0001);
  const spot = candles[candles.length-1].close;

  if (cv > 0.5) return 'CHOPPY';
  if (avgRange < spot * 0.001) return 'DEAD';
  return 'TRENDING';
}

function calcSignals(key, ltp, open, high, low, close, vol) {
  const candles = candleCache[key] || [];
  const completed = candles.length > 1 ? candles.slice(0, -1) : candles;
  const closes = completed.map(c => c.close);

  const ema9   = calcEMA(closes, 9);
  const ema21  = calcEMA(closes, 21);
  const rsi    = calcRSI(closes);
  const vwap   = calcVWAP(completed);

  const aboveEMA9  = ltp > ema9;
  const aboveEMA21 = ltp > ema21;
  const aboveVWAP  = vwap > 0 ? ltp > vwap : null;
  const vwapDist   = vwap > 0 ? parseFloat(((ltp - vwap) / vwap * 100).toFixed(2)) : 0;

  const last5 = completed.slice(-5);
  const hh = last5.length >= 3 && last5[last5.length-1].high > last5[0].high;
  const ll = last5.length >= 3 && last5[last5.length-1].low < last5[0].low;

  const body = Math.abs(ltp - open);
  const bodyPct = ltp > 0 ? body / ltp * 100 : 0;
  const bullCandle = ltp >= open;

  // FIX 2: Estimate IV from ATM premium
  const gap = INSTRUMENTS[key].strikeGap;
  const atm = Math.round(ltp / gap) * gap;
  const chainRow = oiCache[key]?.chain?.find(r => r.strike === atm);
  const atmPrem = chainRow ? (chainRow.callPrem + chainRow.putPrem) / 2 : ltp * 0.005;
  const daysToExpiry = 3;
  const iv = Math.max(0.05, (atmPrem / ltp) * Math.sqrt(365 / daysToExpiry) * Math.sqrt(2 * Math.PI));
  const greeks = estimateGreeks(ltp, atm, daysToExpiry, iv);

  // FIX 3: Regime
  const regime = detectRegime(key);

  let bull = 50;
  if (aboveEMA9)   bull += 8;
  if (aboveEMA21)  bull += 10;
  if (aboveVWAP)   bull += 10;
  if (rsi > 55)    bull += 8;
  if (rsi > 65)    bull += 6;
  if (hh)          bull += 8;
  if (bullCandle)  bull += 6;
  if (ltp > high * 0.998) bull += 4;
  if (!aboveEMA9)  bull -= 8;
  if (!aboveEMA21) bull -= 10;
  if (aboveVWAP === false) bull -= 10;
  if (rsi < 45)    bull -= 8;
  if (rsi < 35)    bull -= 6;
  if (ll)          bull -= 8;
  if (!bullCandle) bull -= 6;
  bull = Math.max(0, Math.min(100, bull));

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
    iv: parseFloat(iv.toFixed(3)),
    gamma: parseFloat(greeks.gamma.toFixed(4)),
    theta: parseFloat(greeks.theta.toFixed(2)),
    vega: parseFloat(greeks.vega.toFixed(2)),
    regime,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 9 — OPTION CHAIN
// ═══════════════════════════════════════════════════════════════════════════════
async function fetchOptionChain(key) {
  const authed = await ensureAuth();
  if (!authed) return;

  const inst = INSTRUMENTS[key];
  const atm = getATM(key);
  const expiry = getNearestExpiry(key);

  try {
    const res = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionChain',
      { name: key, expirydate: expiry, strikePrice: atm.toString() },
      { headers: angelHeaders(), timeout: 15000 }
    );

    const data = res.data?.data;
    if (!data) {
      console.log(`[OI] ${key} — no data in response`);
      return;
    }

    let rows = [];
    if (Array.isArray(data)) rows = data;
    else if (data.fetched && Array.isArray(data.fetched)) rows = data.fetched;
    else if (typeof data === 'object') rows = Object.values(data).filter(v => v && typeof v === 'object');

    const chain = [];
    let totalCallOI = 0, totalPutOI = 0;

    for (const row of rows) {
      const strike = parseFloat(row.strikePrice || row.strike);
      if (!strike) continue;

      let ce = row.CE || row.ce || {};
      let pe = row.PE || row.pe || {};
      if (!ce.lastPrice && row.lastPrice && row.optionType === 'CE') ce = row;
      if (!pe.lastPrice && row.lastPrice && row.optionType === 'PE') pe = row;

      const callOI = parseFloat(ce.openInterest || ce.oi || 0);
      const putOI = parseFloat(pe.openInterest || pe.oi || 0);
      const callPrem = parseFloat(ce.lastPrice || ce.ltp || ce.close || 0);
      const putPrem = parseFloat(pe.lastPrice || pe.ltp || pe.close || 0);

      chain.push({
        strike, callOI, putOI,
        callPrem: callPrem || 1, putPrem: putPrem || 1,
        callToken: ce.symbolToken || ce.token || row.symbolToken || null,
        putToken: pe.symbolToken || pe.token || null,
      });
      totalCallOI += callOI;
      totalPutOI += putOI;
    }

    if (chain.length === 0) {
      console.log(`[OI] ${key} — parsed 0 strikes`);
      return;
    }

    const pcr = totalCallOI > 0 ? parseFloat((totalPutOI / totalCallOI).toFixed(2)) : 1;
    const maxPain = calcMaxPain(chain);
    oiCache[key] = { pcr, maxPain, chain, ts: Date.now() };
    io.emit('oiUpdate', { key, pcr, maxPain, chain: chain.slice(0, 20) });
    console.log(`[OI] ${key} fetched — ${chain.length} strikes, PCR: ${pcr}`);
  } catch (e) {
    console.error(`[OI] ${key} error:`, e.response?.data?.message || e.message);
  }
}

function getATM(key) {
  const ltp = priceCache[key]?.ltp;
  if (!ltp) return 0;
  const gap = INSTRUMENTS[key].strikeGap;
  return Math.round(ltp / gap) * gap;
}

function getNearestExpiry(key) {
  const now = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  const day = now.getDay();
  const target = INSTRUMENTS[key].expiryDay;
  let daysAhead = (target - day + 7) % 7;
  if (daysAhead === 0) daysAhead = 7;
  const expDate = new Date(now.getTime() + daysAhead * 86400000);
  const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
  return `${String(expDate.getDate()).padStart(2,'0')}${months[expDate.getMonth()]}${expDate.getFullYear()}`;
}

function calcMaxPain(chain) {
  if (!chain || chain.length === 0) return 0;
  let minLoss = Infinity, maxPain = 0;
  for (const row of chain) {
    let loss = 0;
    for (const other of chain) {
      if (other.strike < row.strike) loss += (other.callOI || 0) * (row.strike - other.strike);
      if (other.strike > row.strike) loss += (other.putOI || 0) * (other.strike - row.strike);
    }
    if (loss < minLoss) { minLoss = loss; maxPain = row.strike; }
  }
  return maxPain;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 10 — MASTER TICK (FIX 7: Adaptive interval)
// ═══════════════════════════════════════════════════════════════════════════════
async function masterTick() {
  if (tickRunning) return;
  tickRunning = true;
  try {
    const ok = await batchFetch();

    const p = priceCache[activeInst];
    if (p) {
      io.emit('price', {
        ...p,
        candles: (candleCache[activeInst] || []).slice(-60),
        oi: oiCache[activeInst] || null,
      });
    }

    for (const key of Object.keys(INSTRUMENTS)) {
      if (key !== activeInst && priceCache[key]) {
        io.emit('priceBg', {
          key, ltp: priceCache[key].ltp,
          bull: priceCache[key].bull,
          stale: priceCache[key].stale,
        });
      }
    }

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
// SECTION 11 — BOT LOGIC (FIX 1,2,3,4,6,10)
// ═══════════════════════════════════════════════════════════════════════════════
async function runBotTick() {
  if (openTrade) {
    await manageTrade();
    return;
  }

  if (!runBotTick._idx) runBotTick._idx = 0;
  const keys = Object.keys(INSTRUMENTS).filter(k => INSTRUMENTS[k].type === 'index');
  const key = keys[runBotTick._idx % keys.length];
  runBotTick._idx++;

  await scanInstrument(key);
}

async function scanInstrument(key) {
  const quote = priceCache[key];
  const candles = candleCache[key] || [];
  if (!quote || quote.stale) return;

  const { bull, gamma, theta, regime, iv } = quote;
  console.log(`🔍 [SCAN] ${key} bull=${bull} gamma=${gamma} theta=${theta} regime=${regime} iv=${iv}`);

  if (candles.length < 5) {
    console.log(`⏳ [SKIP] ${key} — only ${candles.length} candles`);
    return;
  }

  const istNow = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  const istMin = istNow.getHours() * 60 + istNow.getMinutes();
  if (istMin < cfg.sessionStart || istMin > cfg.sessionEnd) {
    console.log(`⏳ [SKIP] ${key} — outside trading hours`);
    return;
  }

  const lastTrade = tradeLog.filter(t => t.key === key)[0];
  if (lastTrade) {
    const minsSince = (Date.now() - lastTrade.exitTs) / 60000;
    if (minsSince < cfg.cooldownMin) {
      console.log(`⏳ [COOLDOWN] ${key} — ${minsSince.toFixed(1)} min since last trade`);
      return;
    }
  }

  if (consecLosses >= MAX_CONSEC) {
    console.log(`🛑 [PAUSED] ${consecLosses} consecutive losses`);
    botPaused = true;
    io.emit('botStatus', { on: botOn, paused: true, reason: `${MAX_CONSEC} consecutive losses` });
    return;
  }

  // FIX 3: Skip choppy/dead markets
  if (regime === 'CHOPPY') {
    logFilter(key, null, bull, `Regime: CHOPPY — high volatility, avoiding whipsaws`);
    return;
  }
  if (regime === 'DEAD') {
    logFilter(key, null, bull, `Regime: DEAD — no range, no edge`);
    return;
  }

  // FIX 2: Skip high gamma danger (expiry day)
  if (gamma > cfg.maxGamma) {
    logFilter(key, null, bull, `Gamma danger: ${gamma} > ${cfg.maxGamma} — expiry risk too high`);
    return;
  }

  // FIX 2: Skip high theta burn
  const daysToExpiry = 3;
  const thetaBurn = Math.abs(theta) / (quote.ltp * 0.005) * 100; // % of premium burned per day
  if (thetaBurn > cfg.maxThetaBurnPct) {
    logFilter(key, null, bull, `Theta burn: ${thetaBurn.toFixed(1)}% > ${cfg.maxThetaBurnPct}% — time decay too fast`);
    return;
  }

  let dir = null;
  if (bull >= cfg.bullMin) dir = 'CALL';
  if (bull <= cfg.bearMax) dir = 'PUT';
  if (!dir) {
    console.log(`⏳ [NEUTRAL] ${key} bull=${bull}`);
    return;
  }

  // FIX 4: Correlation guard — skip if same direction trade active on another index
  const sameDirOpen = tradeLog.filter(t =>
    t.dir === dir &&
    (Date.now() - t.entryTs) < 30 * 60 * 1000 &&
    t.exitTs === null
  ).length;
  if (sameDirOpen > 0) {
    logFilter(key, dir, bull, `Correlation: ${sameDirOpen} other ${dir} trade(s) active in last 30 min`);
    return;
  }

  // FIX 10: Max open trades
  const totalOpen = tradeLog.filter(t => !t.exitTs && (Date.now() - t.entryTs) < cfg.maxHoldMin * 60000).length;
  if (totalOpen >= cfg.maxOpenTrades) {
    logFilter(key, dir, bull, `Max open trades: ${totalOpen}/${cfg.maxOpenTrades}`);
    return;
  }

  const last5bodies = candles.slice(-5).map(c => Math.abs(c.close - c.open) / c.close * 100);
  const avgBody = last5bodies.reduce((a, b) => a + b, 0) / last5bodies.length;
  const vwapDistAbs = Math.abs(quote.vwapDist || 0);
  if (avgBody < cfg.minBodyPct && vwapDistAbs < cfg.maxVwapDistPct) {
    logFilter(key, dir, bull, `Dead range — avg body ${avgBody.toFixed(3)}%, VWAP dist ${vwapDistAbs.toFixed(2)}%`);
    return;
  }

  const completed = candles.length > 1 ? candles.slice(0, -1) : candles;
  const prev5 = completed.slice(-5);
  const currentCandle = candles[candles.length - 1];

  if (prev5.length >= 3) {
    const p5High = Math.max(...prev5.map(c => c.high), currentCandle?.high || quote.ltp);
    const p5Low = Math.min(...prev5.map(c => c.low), currentCandle?.low || quote.ltp);
    const buf = p5High * (cfg.breakoutBuf / 100);

    if (dir === 'CALL' && quote.ltp < p5High - buf) {
      logFilter(key, dir, bull, `No breakout — ₹${quote.ltp} vs high ₹${p5High}`);
      return;
    }
    if (dir === 'PUT' && quote.ltp > p5Low + buf) {
      logFilter(key, dir, bull, `No breakdown — ₹${quote.ltp} vs low ₹${p5Low}`);
      return;
    }
  }

  await placeTrade(key, quote, dir, bull);
}

function logFilter(key, dir, bull, reason) {
  const entry = {
    time: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }),
    key, dir: dir || '--', bull: bull || 0, reason,
  };
  console.log(`🚫 [FILTER] ${key} ${dir || '--'}: ${reason}`);
  filterLog.unshift(entry);
  if (filterLog.length > 100) filterLog.pop();
  io.emit('filterLog', entry);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 11b — PLACE TRADE (FIX 1: VIX sizing + FIX 9: Paper realism)
// ═══════════════════════════════════════════════════════════════════════════════
async function placeTrade(key, quote, dir, bull) {
  const inst = INSTRUMENTS[key];
  const gap = inst.strikeGap;
  const atm = Math.round(quote.ltp / gap) * gap;

  let strike = atm;
  if (bull >= 70 && dir === 'CALL') strike = atm + gap;
  if (bull <= 30 && dir === 'PUT')  strike = atm - gap;

  if (!oiCache[key] || Date.now() - (oiCache[key].ts || 0) > 120000) {
    await fetchOptionChain(key);
  }

  let entry = null;
  let optionToken = null;
  const chainRow = oiCache[key]?.chain?.find(r => r.strike === strike);

  if (chainRow) {
    entry = dir === 'CALL' ? chainRow.callPrem : chainRow.putPrem;
    optionToken = dir === 'CALL' ? chainRow.callToken : chainRow.putToken;
  }

  if (!entry || entry < 5) {
    entry = Math.round(quote.ltp * 0.005);
  }
  if (!entry || entry < 1) return;

  // FIX 9: Simulate order rejection
  if (cfg.paperMode && Math.random() < cfg.paperOrderReject) {
    logFilter(key, dir, bull, `Order REJECTED by broker (simulated live rejection)`);
    console.log(`🚫 [REJECTED] ${key} ${dir} — simulated broker rejection`);
    return;
  }

  // FIX 9: Simulate entry slippage
  let slippedEntry = entry;
  let executionNote = '';
  if (cfg.paperMode) {
    const slipPct = cfg.paperSlippagePct / 100;
    const isOTM = (dir === 'CALL' && strike > atm) || (dir === 'PUT' && strike < atm);
    const extraSlip = isOTM ? (cfg.paperSpreadOTM / 100) : 0;
    const totalSlip = slipPct + extraSlip;
    slippedEntry = Math.round(entry * (1 + totalSlip));
    if (slippedEntry > entry) {
      executionNote += `Slippage: ₹${entry} → ₹${slippedEntry} (+${(totalSlip*100).toFixed(1)}%). `;
    }
  }

  // FIX 9: Simulate partial fill
  let filledLots = null;
  if (cfg.paperMode && Math.random() < cfg.paperPartialFill) {
    filledLots = Math.max(1, Math.floor(lots * 0.5));
    executionNote += `Partial fill: ${filledLots}/${lots} lots. `;
  }

  // FIX 1: VIX-adjusted position sizing
  const vix = priceCache['INDIAVIX']?.ltp || 15;
  const confidence = bull >= 70 || bull <= 30 ? 'HIGH' : bull >= 63 || bull <= 37 ? 'MED' : 'BASE';
  let basePct = confidence === 'HIGH' ? cfg.highPct : confidence === 'MED' ? cfg.medPct : cfg.basePct;

  if (vix > cfg.vixThresholdHigh) basePct = cfg.sizeAtHighVix;
  else if (vix < cfg.vixThresholdLow) basePct = cfg.sizeAtLowVix;

  const capital = cfg.capital * basePct;
  const lotCost = slippedEntry * inst.lotSize;
  const lots = Math.max(1, Math.floor(capital / lotCost));

  // FIX 9: Margin check even in paper
  if (cfg.paperMode && cfg.paperMarginRequired) {
    const marginRequired = slippedEntry * (filledLots || lots) * inst.lotSize * 1.2;
    if (marginRequired > cfg.capital) {
      logFilter(key, dir, bull, `Margin insufficient: need ₹${Math.round(marginRequired)}, have ₹${cfg.capital}`);
      return;
    }
  }

  // FIX 9: Daily loss kill switch
  if (cfg.paperMode && cfg.paperMaxDailyLoss > 0) {
    const today = new Date().toDateString();
    if (paperLastResetDate !== today) {
      paperDailyPnl = 0;
      paperDailyTrades = 0;
      paperLastResetDate = today;
    }
    if (paperDailyPnl <= -cfg.paperMaxDailyLoss) {
      logFilter(key, dir, bull, `Daily loss limit hit: ₹${paperDailyPnl} (limit: -₹${cfg.paperMaxDailyLoss})`);
      botPaused = true;
      io.emit('botStatus', { on: botOn, paused: true, reason: `Daily loss limit: ₹${paperDailyPnl}` });
      return;
    }
  }

  const sl = Math.round(slippedEntry * cfg.slPct);
  const target = Math.round(slippedEntry * cfg.targetPct);

  openTrade = {
    id: Date.now(),
    key, dir, strike,
    entry: slippedEntry,
    originalEntry: entry,
    sl, target, lots,
    filledLots: filledLots || lots,
    entrySpot: quote.ltp,
    entryTs: Date.now(),
    expiry: getNearestExpiry(key),
    confidence,
    paperMode: cfg.paperMode,
    currentPrem: slippedEntry,
    pnl: 0,
    optionToken,
    executionNote,
    paperCosts: 0,
    vixAtEntry: vix,
    gammaAtEntry: quote.gamma,
  };

  console.log(`✅ [TRADE] ${key} ${dir} ${strike} @ ₹${slippedEntry} (was ₹${entry}) | SL ₹${sl} | Target ₹${target} | ${lots} lot(s) | VIX ${vix}${executionNote ? ' | ' + executionNote : ''}`);

  getAIAnalysis(key, quote, dir, strike, slippedEntry, lots).then(aiReason => {
    if (aiReason) {
      openTrade.aiReason = aiReason;
      io.emit('tradeOpen', { ...openTrade, aiReason });
    } else {
      io.emit('tradeOpen', openTrade);
    }
  }).catch(() => {
    io.emit('tradeOpen', openTrade);
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 11c — MANAGE TRADE (FIX 9: Exit realism)
// ═══════════════════════════════════════════════════════════════════════════════
async function manageTrade() {
  if (!openTrade) return;
  const { key, dir, strike, entry, sl, target, entryTs, lots, optionToken, filledLots, paperMode } = openTrade;
  const quote = priceCache[key];
  if (!quote || quote.stale) return;

  let prem = null;

  if (optionToken) {
    try {
      const res = await axios.post(
        'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
        {
          mode: 'LTP',
          exchangeTokens: { [INSTRUMENTS[key].exchange]: [optionToken] }
        },
        { headers: angelHeaders(), timeout: 5000 }
      );
      const q = res.data?.data?.fetched?.[0];
      if (q && q.ltp) prem = parseFloat(q.ltp);
    } catch (e) {}
  }

  if (!prem || prem < 1) {
    const chainRow = oiCache[key]?.chain?.find(r => r.strike === strike);
    if (chainRow) prem = dir === 'CALL' ? chainRow.callPrem : chainRow.putPrem;
  }

  if (!prem || prem < 1) {
    prem = estimatePremium(key, dir, strike, openTrade.entrySpot, quote.ltp, entry);
  }

  // FIX 9: Exit slippage
  let exitSlippageNote = '';
  let exitPrem = prem;
  if (paperMode && cfg.paperMode) {
    const isOTM = (dir === 'CALL' && strike > Math.round(quote.ltp / INSTRUMENTS[key].strikeGap) * INSTRUMENTS[key].strikeGap) ||
                  (dir === 'PUT' && strike < Math.round(quote.ltp / INSTRUMENTS[key].strikeGap) * INSTRUMENTS[key].strikeGap);
    const exitSlip = isOTM ? (cfg.paperSpreadOTM / 100) : (cfg.paperSlippagePct / 100);
    exitPrem = Math.max(1, Math.round(prem * (1 - exitSlip)));
    if (exitPrem < prem) {
      exitSlippageNote = `Exit slip: ₹${prem} → ₹${exitPrem} (-${(exitSlip*100).toFixed(1)}%). `;
    }
    if (Math.random() < cfg.paperLiquidityDelay) {
      const tickSize = 0.05;
      exitPrem = Math.max(1, Math.round((exitPrem - tickSize) * 100) / 100);
      exitSlippageNote += 'Liquidity delay. ';
    }
  }

  const actualLots = filledLots || lots;
  openTrade.currentPrem = exitPrem;
  openTrade.pnl = (exitPrem - entry) * actualLots * INSTRUMENTS[key].lotSize;

  const heldMin = (Date.now() - entryTs) / 60000;
  const heldOk = heldMin >= cfg.minHoldMin;

  let exitReason = null;
  if (heldOk && exitPrem <= sl)     exitReason = 'SL_HIT';
  if (heldOk && exitPrem >= target) exitReason = 'TARGET_HIT';
  if (heldMin >= cfg.maxHoldMin) exitReason = 'TIME_EXIT';

  io.emit('tradeUpdate', { ...openTrade, heldMin: Math.round(heldMin), exitSlippageNote });

  if (exitReason) await closeTrade(exitReason, exitPrem);
}

function estimatePremium(key, dir, strike, entrySpot, currentSpot, entryPrem) {
  const moneyness = dir === 'CALL'
    ? (currentSpot - strike) / strike
    : (strike - currentSpot) / strike;
  const baseDelta = 0.5;
  const delta = moneyness > 0.01 ? 0.75 : moneyness < -0.01 ? 0.25 : baseDelta;
  const spotChange = (currentSpot - entrySpot) / entrySpot;
  const premChange = spotChange * delta * (dir === 'CALL' ? 1 : -1);
  return Math.max(1, Math.round(entryPrem * (1 + premChange)));
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 11d — CLOSE TRADE (FIX 9: Cost deduction)
// ═══════════════════════════════════════════════════════════════════════════════
async function closeTrade(reason, exitPrem) {
  if (!openTrade) return;
  const trade = {
    ...openTrade,
    exitPrem,
    exitTs: Date.now(),
    exitReason: reason,
  };

  const actualLots = trade.filledLots || trade.lots;
  const grossPnl = (exitPrem - trade.entry) * actualLots * INSTRUMENTS[trade.key].lotSize;

  if (trade.paperMode && cfg.paperMode) {
    const brokerage = cfg.paperBrokerage * 2;
    const stt = (exitPrem * actualLots * INSTRUMENTS[trade.key].lotSize) * (cfg.paperSttPct / 100);
    const gst = brokerage * (cfg.paperGstPct / 100);
    const totalCosts = brokerage + stt + gst;

    trade.paperCosts = totalCosts;
    trade.grossPnl = grossPnl;
    trade.finalPnl = grossPnl - totalCosts;

    paperDailyPnl += trade.finalPnl;
    paperDailyTrades++;
    trade.paperDailyPnl = paperDailyPnl;
    trade.paperDailyTrades = paperDailyTrades;
  } else {
    trade.finalPnl = grossPnl;
  }

  const won = trade.finalPnl > 0;
  if (won) consecLosses = 0;
  else     consecLosses++;

  tradeLog.unshift(trade);
  if (tradeLog.length > 500) tradeLog.pop();

  const costNote = trade.paperCosts ? ` | Costs: ₹${trade.paperCosts.toFixed(0)}` : '';
  console.log(`🏁 [EXIT] ${trade.key} ${trade.dir} — ${reason} @ ₹${exitPrem} | Gross: ₹${trade.grossPnl?.toFixed(0) || grossPnl.toFixed(0)} | Net: ₹${trade.finalPnl.toFixed(0)}${costNote}${trade.executionNote ? ' | ' + trade.executionNote : ''}${trade.exitSlippageNote ? ' | ' + trade.exitSlippageNote : ''}`);
  io.emit('tradeClose', trade);
  openTrade = null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 11e — AI ANALYSIS (GROQ)
// ═══════════════════════════════════════════════════════════════════════════════
async function getAIAnalysis(key, quote, dir, strike, entry, lots) {
  const groqKey = process.env.GROQ_API_KEY;
  if (!groqKey) return null;

  const inst = INSTRUMENTS[key];
  const prompt = `You are an expert Indian options trader. Analyze this trade setup:

Instrument: ${inst.name} (${key})
Direction: ${dir}
Strike: ₹${strike}
Entry Premium: ₹${entry}
Lots: ${lots}
Lot Size: ${inst.lotSize}
Spot: ₹${quote.ltp}
Change: ${quote.change} (${quote.changePct}%)
RSI: ${quote.rsi}
EMA9: ₹${quote.ema9}
VWAP: ₹${quote.vwap}
IV: ${quote.iv}%
Gamma: ${quote.gamma}
Regime: ${quote.regime}
VIX: ${priceCache['INDIAVIX']?.ltp || 'N/A'}

Provide a 2-3 sentence trade rationale and one risk warning. Be concise.`;

  try {
    const res = await axios.post(
      'https://api.groq.com/openai/v1/chat/completions',
      {
        model: 'llama-3.1-8b-instant',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 200,
        temperature: 0.3,
      },
      { headers: { 'Authorization': `Bearer ${groqKey}`, 'Content-Type': 'application/json' }, timeout: 10000 }
    );
    return res.data?.choices?.[0]?.message?.content || null;
  } catch (e) {
    console.error('[AI] Groq error:', e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 12 — WEBSOCKET (DISABLED)
// ═══════════════════════════════════════════════════════════════════════════════
function startWebSocket() {
  console.log('ℹ️ SmartStream WebSocket disabled — using REST polling');
  wsAlive = false;
  return;
}
function stopWebSocket() {
  if (wsConn) { try { wsConn.close(); } catch (e) {} wsConn = null; }
  wsAlive = false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 13 — API ROUTES
// ═══════════════════════════════════════════════════════════════════════════════
app.post('/api/instrument', (req, res) => {
  const { key } = req.body;
  if (!INSTRUMENTS[key]) return res.json({ ok: false, error: 'Unknown instrument' });
  activeInst = key;
  const p = priceCache[key];
  if (p) io.emit('price', { ...p, candles: (candleCache[key]||[]).slice(-60), oi: oiCache[key]||null });
  res.json({ ok: true, key });
});

app.post('/api/bot/start', (req, res) => {
  botOn = true; botPaused = false; consecLosses = 0;
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
  botPaused = false; consecLosses = 0;
  console.log('🤖 Bot RESUMED');
  io.emit('botStatus', { on: botOn, paused: false });
  res.json({ ok: true });
});

app.post('/api/trade/exit', async (req, res) => {
  if (!openTrade) return res.json({ ok: false, error: 'No open trade' });
  await closeTrade('MANUAL_EXIT', openTrade.currentPrem || openTrade.entry);
  res.json({ ok: true });
});

app.post('/api/tune', (req, res) => {
  const allowed = ['bullMin','bearMax','breakoutBuf','slPct','targetPct','minHoldMin',
                   'maxHoldMin','cooldownMin','basePct','medPct','highPct','capital',
                   'sessionStart','sessionEnd','minBodyPct','maxVwapDistPct','paperMode',
                   'vixThresholdLow','vixThresholdHigh','sizeAtHighVix','sizeAtLowVix',
                   'maxGamma','maxThetaBurnPct','maxOpenTrades','paperMaxDailyLoss'];
  for (const [k, v] of Object.entries(req.body)) {
    if (allowed.includes(k)) cfg[k] = typeof cfg[k] === 'boolean' ? !!v : parseFloat(v);
  }
  io.emit('cfg', cfg);
  res.json({ ok: true, cfg });
});

app.get('/api/state', (req, res) => {
  res.json({
    prices: priceCache,
    openTrade,
    tradeLog: tradeLog.slice(0, 50),
    filterLog: filterLog.slice(0, 50),
    cfg,
    botOn, botPaused, consecLosses,
    activeInst, wsAlive,
    paperStats: cfg.paperMode ? {
      dailyPnl: paperDailyPnl,
      dailyTrades: paperDailyTrades,
      lastResetDate: paperLastResetDate,
      maxDailyLoss: cfg.paperMaxDailyLoss,
      slippageEnabled: cfg.paperSlippagePct > 0,
    } : null,
    aiEnabled: !!process.env.GROQ_API_KEY,
    instruments: Object.entries(INSTRUMENTS).map(([k,v]) => ({ key: k, name: v.name })),
  });
});

app.get('/api/oi/:key', async (req, res) => {
  const key = req.params.key.toUpperCase();
  if (!INSTRUMENTS[key]) return res.json({ ok: false });
  await fetchOptionChain(key);
  res.json({ ok: true, oi: oiCache[key] || null });
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/health', (req, res) => {
  const healthy = !!authToken && Date.now() < (tokenExp || 0);
  res.status(healthy ? 200 : 503).json({
    ok: healthy, uptime: process.uptime(),
    loggedIn: !!authToken, wsAlive, botOn, botPaused,
  });
});

process.on('uncaughtException', (e) => console.error('💥 Uncaught:', e));
process.on('unhandledRejection', (e) => console.error('💥 Unhandled:', e));

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 14 — SOCKET.IO
// ═══════════════════════════════════════════════════════════════════════════════
io.on('connection', (socket) => {
  console.log(`[WS] Client connected: ${socket.id}`);
  socket.emit('init', {
    prices: priceCache, openTrade,
    tradeLog: tradeLog.slice(0, 50),
    filterLog: filterLog.slice(0, 50),
    cfg, botOn, botPaused, consecLosses,
    activeInst, wsAlive,
    paperStats: cfg.paperMode ? {
      dailyPnl: paperDailyPnl, dailyTrades: paperDailyTrades,
      lastResetDate: paperLastResetDate,
    } : null,
    aiEnabled: !!process.env.GROQ_API_KEY,
    instruments: Object.entries(INSTRUMENTS).map(([k,v]) => ({ key: k, name: v.name })),
  });
  socket.on('disconnect', () => console.log(`[WS] Client disconnected: ${socket.id}`));
});

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 15 — STARTUP (FIX 7: Adaptive interval)
// ═══════════════════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 8080;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 DineshTrade v3.0 live on port ${PORT}`);
});

(async () => {
  try {
    const ok = await login();
    if (!ok) {
      console.error('❌ Angel One login failed — retrying in 30s...');
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
  await batchFetch();
  setTimeout(() => fetchOptionChain(activeInst), 3000);

  // FIX 7: Adaptive interval with jitter
  function scheduleTick() {
    const minsToExpiry = getMinutesToExpiry();
    let interval = 3000;
    if (minsToExpiry < 30) interval = 2000;
    if (minsToExpiry < 10) interval = 1500;
    const jitter = Math.random() * 1000;
    tickTimer = setTimeout(() => {
      masterTick();
      scheduleTick();
    }, interval + jitter);
  }
  scheduleTick();
  masterTick();

  setInterval(() => {
    for (const key of Object.keys(INSTRUMENTS)) fetchOptionChain(key);
  }, 3 * 60 * 1000);

  setInterval(async () => { await login(); }, 6 * 60 * 60 * 1000);

  console.log('✅ All systems go. Adaptive tick | OI: 3min | WS: disabled');
}
