# BANKNIFTY Paper Trade Bot

This bot uses a Telegram user session to read `INSTITUTIONAL DUAL MATCH` alerts from `Marketmenia_news` and posts live-price paper trade updates into `bnf trade` through a Telegram bot token.

## Input and Output

- Input source: `Marketmenia_news` via `Telethon`
- Output destination: your private channel like `bnf trade` via Bot API
- Market data: Angel One option LTP

## Accepted Source Alerts

This bot will open/monitor a paper trade only for `INSTITUTIONAL DUAL MATCH` alerts that include an `ACTION: BUY ...` line.

Examples:

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY BANKNIFTY <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY NIFTY <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY MIDCPNIFTY <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY SENSEX <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY HDFCBANK <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY ICICIBANK <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY RELIANCE <strike> CE/PE
```

Notes:
- `2 MIN OPTION FLOW ALERT` messages are treated as informational and are ignored by this bot (no `ACTION: BUY ...` line).

## Output Messages

Entry:

```text
🔥 BANKNIFTY 57200 PE

📍 Entry: 583.60
🛡️ SL: 553.60
🎯 T1: 613.60
🎯 T2: 643.60
🎯 T3: 673.60
🎯 T4: 703.60
```

Price update:

```text
📈 Price Update: 587.75
```

Stop loss:

```text
❌ SL HIT @ 553.20
```

## Environment

```text
TG_API_ID=
TG_API_HASH=
TG_SESSION_STR=
SOURCE_CHAT=Marketmenia_news

PAPER_TRADE_BOT_TOKEN=
PAPER_TRADE_CHANNEL_ID=-100...

ANGEL_API_KEY=
ANGEL_CLIENT_ID=
ANGEL_PASSWORD=
ANGEL_TOTP_SECRET=

REAL_TRADE_ENABLED=false
REAL_PRODUCT_TYPE=INTRADAY
REAL_ORDER_TYPE=MARKET
MAX_TRADES_PER_DAY=5
ALLOW_REAL_TRADING_AFTER=09:20
STOP_REAL_TRADING_AFTER=15:10
TRADE_UNDERLYINGS=NIFTY,BANKNIFTY
REAL_ALLOWED_UNDERLYINGS=NIFTY,BANKNIFTY
NIFTY_LOT_SIZE=65
BANKNIFTY_LOT_SIZE=30
KEEPALIVE_ENABLED=true
KEEPALIVE_INTERVAL_SECONDS=300
KEEPALIVE_START=09:00
KEEPALIVE_END=15:30
KEEPALIVE_LOG_ENABLED=false
STARTUP_CONFIRMATION_ENABLED=true
```

## Run

```powershell
cd "C:\Users\kalpe\gdfl data\banknifty-paper-trade-bot"
pip install -r requirements.txt
python paper_trade_bot.py
```

## Logic

- Reads only `INSTITUTIONAL DUAL MATCH` style alerts from the source chat.
- Extracts `ACTION: BUY <symbol> <strike> CE/PE`.
- Opens one active trade per allowed underlying.
- Duplicate same signal is blocked for 10 minutes.
- Reverse signal exits the old trade and opens the new one.
- Entry is live Angel One LTP.
- Index SL is `entry - 30`.
- Index targets are `entry + 30`, `+60`, `+90`, `+120`.
- Stock SL is `entry - 3`.
- Stock targets are `entry + 3`, `+6`, `+9`, `+12`.
- Monitor loop runs every 3 seconds.
- By default real trading is OFF. Set `REAL_TRADE_ENABLED=true` only when you want live Angel One orders.
- Current real-trade scope is only `NIFTY` and `BANKNIFTY`.
- Real quantity is one lot: `NIFTY=65`, `BANKNIFTY=30`.
- Real entries are blocked before `09:20`, after `15:10`, and after 5 real entries per day.
- If a real trade is open at `15:10`, the bot sends a market SELL exit.
- During `09:00-15:30`, keepalive calls Telegram `getMe` every 5 minutes so Railway Serverless does not see the worker as idle.
- Telegram startup confirmation is kept short: `SCANNER START`.
- On startup, the bot sends one Telegram confirmation showing mode, symbols, quantity, entry window, and max trades.
