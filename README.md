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
ACTION: BUY HDFCBANK <strike> CE/PE
```

```text
INSTITUTIONAL DUAL MATCH
ACTION: BUY ICICIBANK <strike> CE/PE
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
```

## Run

```powershell
cd "C:\Users\kalpe\gdfl data\banknifty-paper-trade-bot"
pip install -r requirements.txt
python paper_trade_bot.py
```

## Logic

- Reads only `INSTITUTIONAL DUAL MATCH` style alerts from the source chat.
- Extracts `BANKNIFTY <strike> CE/PE`.
- Opens only one active trade at a time.
- Duplicate same signal is blocked for 10 minutes.
- Reverse signal exits the old trade and opens the new one.
- Entry is live Angel One LTP.
- SL is `entry - 30`.
- Targets are `entry + 30`, `+60`, `+90`, `+120`.
- Monitor loop runs every 3 seconds.
