# DineshTrade v2

Nifty · BankNifty · Sensex live options trading dashboard.
Built on Angel One SmartAPI. Deploys to Railway in 5 minutes.

---

## Files
```
server.js     — backend (Node.js + Express + Socket.IO)
index.html    — frontend (single file, no build step)
package.json  — dependencies
```

---

## Setup (one time)

### Step 1 — GitHub repo
1. Go to github.com → New repository → name it `dineshtrade`
2. Upload all 4 files (server.js, index.html, package.json, .gitignore)

### Step 2 — Railway deployment
1. Go to railway.app → New Project → Deploy from GitHub repo
2. Select your `dineshtrade` repo
3. Railway auto-detects Node.js and runs `npm start`

### Step 3 — Environment variables (Railway → Variables tab)
Add exactly these 4 variables:

```
ANGEL_API_KEY      = your SmartAPI key (from smartapi.angelone.in)
ANGEL_CLIENT_ID    = your Angel One client ID (e.g. D123456)
ANGEL_PASSWORD     = your Angel One login password
ANGEL_TOTP_SECRET  = your TOTP secret key (shown when setting up 2FA in Angel One)
```

### Step 4 — Done
Railway gives you a URL like `https://dineshtrade-production.up.railway.app`
Open it on your phone — it works as a mobile app.

---

## How it works

**Price updates**
- REST batch fetch: all 3 instruments in 1–2 API calls every 5 seconds
- WebSocket on top: 1-second ticks from Angel One SmartStream (additive)
- REST always updates UI — WebSocket never blocks it

**Bot logic**
- Scans one instrument per tick (rotating) — no parallel API storm
- Only trades with fresh data — skips if last fetch failed
- Guards: 5 candles minimum, trading hours, cooldown, consecutive loss limit

**Signal filters (in order)**
1. Bull score 43–57 = neutral → skip
2. Dead range (flat candles near VWAP) → skip
3. No breakout within 0.1% of prev 5-candle high/low → skip
4. validateSignal (confidence, RSI, EMA alignment) → skip if confidence too low
5. All pass → place trade

**Trade management**
- SL: 15% of entry premium (configurable)
- Target: 50% gain (configurable)
- Min hold: 3 minutes before SL/target check
- Max hold: 45 minutes → forced exit
- After 3 consecutive losses → bot pauses, requires manual resume

---

## Settings (in-app)
All settings adjustable from the Settings tab without redeployment.
Changes take effect immediately on next tick.

---

## Paper vs Live
- Default: Paper mode (virtual ₹1,00,000 capital)
- Toggle in Settings tab
- For live: Angel One SmartAPI must have trading permissions enabled
