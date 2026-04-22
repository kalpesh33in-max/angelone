# BANKNIFTY Paper Trade Bot

This bot reads Telegram channel alerts like:

```text
BUY BANKNIFTY 56500 CE
Entry: 200
SL: 170
T1: 230
T2: 260
T3: 290
```

It creates a paper entry from the alert price when provided, otherwise using Angel One live option LTP. It does not place live orders.

## Telegram Setup

1. Open Telegram and create a channel.
2. Add your existing alert bot/channel source so the channel receives alerts.
3. Create a new bot with `@BotFather`.
4. Add this new paper bot to the channel as admin.
5. Give it permission to read/post messages.
6. Send one test message in the channel and get the channel id.

For a private channel, the id usually looks like:

```text
-1001234567890
```

## Environment

Set these variables. You can reuse your existing AngelOne/MCX Telegram bot, so a new Telegram bot is not required:

```text
TELE_TOKEN_MCX=telegram bot token
CHAT_ID_MCX=-100...
ANGEL_API_KEY=...
ANGEL_CLIENT_ID=...
ANGEL_PASSWORD=...
ANGEL_TOTP_SECRET=...
```

The bot also supports these optional dedicated names:

```text
PAPER_TRADE_BOT_TOKEN=telegram bot token
PAPER_TRADE_CHANNEL_ID=-100...
```

On Railway, add the same values in:

```text
Project -> Variables
```

Do not commit `.env` to GitHub.

## Run

```powershell
cd "C:\Users\kalpe\gdfl data\banknifty-paper-trade-bot"
pip install -r requirements.txt
python paper_trade_bot.py
```

## Railway Deploy

Push only this folder to GitHub or make it the repo root. Railway will use:

```text
Procfile
runtime.txt
requirements.txt
```

Worker command:

```text
python paper_trade_bot.py
```

## Trade Logic

- Reads `BUY BANKNIFTY <strike> CE/PE`.
- `ACTION:` is optional for backward compatibility.
- If `Entry`, `SL`, `T1`, `T2`, and `T3` are included, the bot uses those alert prices.
- If prices are not included, the bot uses Angel One live option LTP and calculates levels.
- Only one open paper trade.
- Quantity: 1 BANKNIFTY lot.
- Initial SL: entry - 30 points.
- First target: entry + 30 points.
- Sends live updates only when price moves above the last alerted price.
- Down moves do not send alerts unless SL is hit.
- If T1 hits, alert says exit or move SL cost to cost.
- If T2 hits, alert says exit or move SL to T1.
- If T3 hits, final target is hit and the paper trade closes.
- If active SL is hit before the next target, the paper trade exits at SL.
