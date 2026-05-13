import os
import re
import asyncio
import datetime
import pytz
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ---------------- CONFIG ---------------- #

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STR = os.getenv("TG_SESSION_STR")

SOURCE_IDS = [int(i.strip()) for i in os.getenv("SOURCE_BOT").split(",")]
TARGET_BOT_RAW = os.getenv("TARGET_BOT", "").strip()

IST = pytz.timezone("Asia/Kolkata")

# ---------------- INSTRUMENT SPECS ---------------- #

INDEX_SYMBOLS = ["BANKNIFTY", "NIFTY", "SENSEX", "MIDCPNIFTY"]
STOCK_SYMBOLS = ["HDFCBANK", "ICICIBANK", "RELIANCE"]

WATCH_SYMBOLS = INDEX_SYMBOLS + STOCK_SYMBOLS

# Strike steps
STRIKE_STEPS = {
    "BANKNIFTY": int(os.getenv("BANKNIFTY_STRIKE_STEP", "100")),
    "NIFTY": int(os.getenv("NIFTY_STRIKE_STEP", "50")),
    "SENSEX": int(os.getenv("SENSEX_STRIKE_STEP", "100")),
    "MIDCPNIFTY": int(os.getenv("MIDCPNIFTY_STRIKE_STEP", "25")),
    "HDFCBANK": 5,
    "ICICIBANK": 10,
    "RELIANCE": 10,
}

# Thresholds
FUT_LOT_THRESHOLD = int(os.getenv("FUT_LOT_THRESHOLD", "2000"))

ITM_WRITER_THRESHOLD_CR = float(
    os.getenv("ITM_WRITER_THRESHOLD_CR", "12")
)

ITM_WRITER_CONFLICT_CR = float(
    os.getenv("ITM_WRITER_CONFLICT_CR", "10")
)

# ---------------- STATE ---------------- #

last_fut_signals = {}
last_signals_by_symbol = {}
instant_itm_alerts = {}

# ---------------- UTILITY ---------------- #

def parse_target_ref(value):
    if not value:
        raise RuntimeError("TARGET_BOT env var missing")

    value = value.strip()
    value = re.sub(
        r"^https?://t\.me/",
        "",
        value,
        flags=re.IGNORECASE
    ).strip("/")

    return int(value) if re.fullmatch(r"-?\d+", value) else value


TARGET_BOT_REF = parse_target_ref(TARGET_BOT_RAW)


def _entity_key(value):
    return re.sub(r"[\s_@]+", "", str(value or "").lower())


async def resolve_target_entity(client, target_ref):

    candidates = [target_ref]

    if isinstance(target_ref, str) and not target_ref.startswith("@"):
        candidates.append(f"@{target_ref}")

    last_error = None

    for candidate in candidates:
        try:
            return await client.get_entity(candidate)
        except Exception as e:
            last_error = e

    target_key = _entity_key(target_ref)

    async for dialog in client.iter_dialogs():

        entity = dialog.entity

        entity_id = str(getattr(entity, "id", ""))

        full_channel_id = (
            f"-100{entity_id}"
            if entity_id and not entity_id.startswith("-")
            else entity_id
        )

        values = [
            getattr(entity, "username", None),
            getattr(entity, "title", None),
            getattr(entity, "first_name", None),
            dialog.name,
            entity_id,
            full_channel_id,
        ]

        if any(_entity_key(v) == target_key for v in values):
            return entity

    raise RuntimeError(
        f"Could not resolve TARGET_BOT={target_ref!r}. "
        f"Last error: {last_error}"
    )


def get_atm(price, symbol):

    step = STRIKE_STEPS.get(symbol.upper(), 100)

    return int(round(float(price) / step) * step)


def risk_points_for(symbol):

    return (3, 6) if symbol.upper() in STOCK_SYMBOLS else (30, 60)


def _normalize_cr(value, unit):

    try:
        val = float(value)

        if unit == "Cr":
            return val

        elif unit == "L":
            return val / 100

        return 0.0

    except:
        return 0.0


def get_writing_values(label, text):

    pattern = (
        rf"{label}\s+\d+\(([\d.]+)(Cr|L|)\)"
        rf"\s+\d+\(([\d.]+)(Cr|L|)\)"
    )

    matches = re.findall(pattern, text, re.IGNORECASE)

    if not matches:
        return 0.0, 0.0

    itm_val, itm_unit, otm_val, otm_unit = matches[0]

    return (
        _normalize_cr(itm_val, itm_unit),
        _normalize_cr(otm_val, otm_unit),
    )


def get_value(label, text):

    pattern = rf"{label}\s*:\s*([\d.]+)(Cr|L|)"

    matches = re.findall(pattern, text, re.IGNORECASE)

    if not matches:
        return 0.0

    val_str, unit = matches[-1]

    return _normalize_cr(val_str, unit)


# ---------------- STRONG FUTURE PRICE PARSER ---------------- #

def get_future_price(text, symbol):

    if not text:
        return None

    patterns = [

        rf"{re.escape(symbol)}\s*\(FUT:\s*([\d.]+)\)",

        rf"{re.escape(symbol)}\(FUT:\s*([\d.]+)\)",

        rf"{re.escape(symbol)}\s*FUT:\s*([\d.]+)",

        rf"{re.escape(symbol)}.*?FUT:\s*([\d.]+)",

    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            re.IGNORECASE | re.DOTALL
        )

        if match:

            try:
                return float(match.group(1))

            except:
                pass

    return None


def extract_instrument_section(text, symbol):

    sym_pat = rf"(?<![A-Z0-9_]){re.escape(symbol)}"

    m = re.search(sym_pat, text, re.IGNORECASE)

    if not m:
        return None

    start = m.start()

    next_pos = [len(text)]

    for sym in WATCH_SYMBOLS:

        if sym == symbol:
            continue

        m2 = re.search(
            rf"(?<![A-Z0-9_]){re.escape(sym)}",
            text[m.end():],
            re.IGNORECASE
        )

        if m2:
            next_pos.append(m.end() + m2.start())

    return text[start:min(next_pos)]


def parse_flow_metrics(section):

    if not section:
        return None

    opt_part = section.split("---- FUTURES FLOW ----")[0]

    c_itm, c_otm = get_writing_values("CALL_WR", opt_part)

    p_itm, p_otm = get_writing_values("PUT_WR", opt_part)

    return {

        "bull_t": get_value("Bullish Turn", opt_part),

        "bear_t": get_value("Bearish Turn", opt_part),

        "call_itm": c_itm,
        "call_otm": c_otm,

        "put_itm": p_itm,
        "put_otm": p_otm,
    }


async def safe_send(client, target_id, message):

    try:
        await client.send_message(target_id, message)

    except Exception as e:
        print(f"❌ DELIVERY ERROR: {e}", flush=True)


# ---------------- MAIN ---------------- #

async def main():

    client = TelegramClient(
        StringSession(SESSION_STR),
        API_ID,
        API_HASH
    )

    await client.start()

    try:

        target_entity = await resolve_target_entity(
            client,
            TARGET_BOT_REF
        )

        print(
            f"✅ TARGET_BOT resolved: "
            f"{getattr(target_entity, 'id', TARGET_BOT_REF)}",
            flush=True
        )

    except Exception as e:

        target_entity = TARGET_BOT_REF

        print(f"❌ TARGET_BOT resolve failed: {e}", flush=True)

    print(
        "🚀 SCANNER ACTIVE WITH DEBUG LOGS",
        flush=True
    )

    # ---------------- TELEGRAM HANDLER ---------------- #

    @client.on(events.NewMessage(chats=SOURCE_IDS))
    async def handler(event):

        try:

            text = event.message.text

            if not text:
                return

            print("\n==============================", flush=True)
            print("📩 NEW MESSAGE RECEIVED", flush=True)
            print("==============================", flush=True)

            print(text[:3000], flush=True)

            now = datetime.datetime.now(IST)

            # Detect timeframe

            if "2 MIN" in text.upper():

                lbl = "2 MIN FLOW"
                short_lbl = "2MIN"

            elif "5 MIN" in text.upper():

                lbl = "5 MIN FLOW"
                short_lbl = "5MIN"

            else:
                return

            # ====================================================
            # LOOP SYMBOLS
            # ====================================================

            for symbol in WATCH_SYMBOLS:

                print("\n--------------------------------", flush=True)
                print(f"🔍 SYMBOL => {symbol}", flush=True)

                section = extract_instrument_section(text, symbol)

                print(
                    f"SECTION FOUND => {bool(section)}",
                    flush=True
                )

                if not section:
                    continue

                print("\n📌 EXTRACTED SECTION:", flush=True)
                print(section[:1000], flush=True)

                # ---------------- FUT PRICE ---------------- #

                price = get_future_price(section, symbol)

                print(f"FUT PRICE => {price}", flush=True)

                # SAFETY CHECK

                if not price:

                    print(
                        f"⚠️ SKIPPED {symbol} => FUT PRICE NOT FOUND",
                        flush=True
                    )

                    continue

                strike = get_atm(price, symbol)

                print(f"ATM STRIKE => {strike}", flush=True)

                sl, tg = risk_points_for(symbol)

                # ---------------- FLOW METRICS ---------------- #

                metrics = parse_flow_metrics(section)

                print(f"METRICS => {metrics}", flush=True)

                if not metrics:
                    continue

                # ====================================================
                # FUTURES LOT LOGIC
                # ====================================================

                fut_match = re.search(
                    r"(FUT_BUY|FUT_SELL)\s*:\s*(\d+)\s+lots",
                    section,
                    re.IGNORECASE
                )

                if fut_match:

                    fut_type = fut_match.group(1).upper()
                    fut_lots = int(fut_match.group(2))

                    print(
                        f"FUT TYPE => {fut_type} | LOTS => {fut_lots}",
                        flush=True
                    )

                    if fut_lots >= FUT_LOT_THRESHOLD:

                        sig_fut = (
                            "CALL"
                            if fut_type == "FUT_BUY"
                            else "PUT"
                        )

                        print(
                            f"✅ FUT SIGNAL DETECTED => {sig_fut}",
                            flush=True
                        )

                        if symbol not in last_fut_signals:

                            last_fut_signals[symbol] = {
                                "2 MIN FLOW": None,
                                "5 MIN FLOW": None
                            }

                        last_fut_signals[symbol][lbl] = {
                            "type": sig_fut,
                            "time": now
                        }

                        other_lbl = (
                            "5 MIN FLOW"
                            if short_lbl == "2MIN"
                            else "2 MIN FLOW"
                        )

                        other = last_fut_signals[symbol].get(other_lbl)

                        if (
                            other
                            and other["type"] == sig_fut
                            and abs(
                                (
                                    now - other["time"]
                                ).total_seconds()
                            ) <= 30
                        ):

                            emoji = (
                                "🟢"
                                if sig_fut == "CALL"
                                else "🔴"
                            )

                            msg = (
                                f"{emoji} "
                                f"**INSTITUTIONAL DUAL MATCH** "
                                f"{emoji}\n\n"

                                f"**ACTION: BUY "
                                f"{symbol} {strike} "
                                f"{'CE' if sig_fut == 'CALL' else 'PE'}**\n"

                                f"**SIGNAL: {sig_fut} "
                                f"(FUT lots >= {FUT_LOT_THRESHOLD})**\n"

                                f"🛡️ **SL: {sl} pts | "
                                f"🎯 TARGET: {tg} pts**"
                            )

                            print(
                                f"🚨 ALERT GENERATED => "
                                f"{symbol} {strike} {sig_fut}",
                                flush=True
                            )

                            await safe_send(
                                client,
                                target_entity,
                                msg
                            )

                            last_fut_signals[symbol] = {
                                "2 MIN FLOW": None,
                                "5 MIN FLOW": None
                            }

                # ====================================================
                # ITM WRITER ALERT
                # ====================================================

                if short_lbl == "2MIN":

                    alert_side = None

                    both_sides_high = (

                        metrics["put_itm"]
                        >= ITM_WRITER_CONFLICT_CR

                        and

                        metrics["call_itm"]
                        >= ITM_WRITER_CONFLICT_CR
                    )

                    if not both_sides_high:

                        if (
                            metrics["put_itm"]
                            >= ITM_WRITER_THRESHOLD_CR
                        ):

                            alert_side = "CALL"

                        elif (
                            metrics["call_itm"]
                            >= ITM_WRITER_THRESHOLD_CR
                        ):

                            alert_side = "PUT"

                    if alert_side:

                        akey = (
                            f"{symbol}_"
                            f"{alert_side}_"
                            f"{now.strftime('%H:%M')}"
                        )

                        if akey not in instant_itm_alerts:

                            instant_itm_alerts[akey] = now

                            emoji = (
                                "🟢"
                                if alert_side == "CALL"
                                else "🔴"
                            )

                            msg = (
                                f"{emoji} "
                                f"**INSTITUTIONAL DUAL MATCH** "
                                f"{emoji}\n\n"

                                f"**ACTION: BUY "
                                f"{symbol} {strike} "
                                f"{'CE' if alert_side == 'CALL' else 'PE'}**\n"

                                f"**SIGNAL: {alert_side} "
                                f"(2MIN ITM WRITER >= "
                                f"{ITM_WRITER_THRESHOLD_CR:g}Cr)**\n"

                                f"🛡️ **SL: {sl} pts | "
                                f"🎯 TARGET: {tg} pts**"
                            )

                            print(
                                f"🚨 ITM WRITER ALERT => "
                                f"{symbol} {strike} {alert_side}",
                                flush=True
                            )

                            await safe_send(
                                client,
                                target_entity,
                                msg
                            )

                # ====================================================
                # DUAL FLOW LOGIC
                # ====================================================

                sig_type = None

                # INDEX LOGIC

                if symbol in ("BANKNIFTY", "NIFTY"):

                    m_turn, m_itm = (
                        (10.0, 6.5)
                        if short_lbl == "2MIN"
                        else (2.0, 1.0)
                    )

                    if (
                        metrics["bull_t"] >= m_turn
                        and metrics["put_itm"] >= m_itm
                        and metrics["bear_t"] < 1.0
                    ):

                        sig_type = "CALL"

                    elif (
                        metrics["bear_t"] >= m_turn
                        and metrics["call_itm"] >= m_itm
                        and metrics["bull_t"] < 1.0
                    ):

                        sig_type = "PUT"

                else:

                    # STOCK LOGIC

                    if short_lbl == "2MIN":

                        if (
                            metrics["bull_t"] >= 6.0
                            and metrics["put_itm"] >= 3.5
                            and metrics["bear_t"] < 1.0
                        ):

                            sig_type = "CALL"

                        elif (
                            metrics["bear_t"] >= 6.0
                            and metrics["call_itm"] >= 3.5
                            and metrics["bull_t"] < 1.0
                        ):

                            sig_type = "PUT"

                    else:

                        if (
                            metrics["bull_t"] >= 1.0
                            and metrics["put_itm"] < 1.0
                            and metrics["bear_t"] < 1.0
                        ):

                            sig_type = "CALL"

                        elif (
                            metrics["bear_t"] >= 1.0
                            and metrics["call_itm"] < 1.0
                            and metrics["bull_t"] < 1.0
                        ):

                            sig_type = "PUT"

                # ====================================================
                # FINAL DUAL CONFIRMATION
                # ====================================================

                if sig_type:

                    print(
                        f"✅ SIGNAL DETECTED => {sig_type}",
                        flush=True
                    )

                    if symbol not in last_signals_by_symbol:

                        last_signals_by_symbol[symbol] = {
                            "2 MIN FLOW": None,
                            "5 MIN FLOW": None
                        }

                    last_signals_by_symbol[symbol][lbl] = {
                        "type": sig_type,
                        "time": now
                    }

                    other_lbl = (
                        "5 MIN FLOW"
                        if short_lbl == "2MIN"
                        else "2 MIN FLOW"
                    )

                    other = last_signals_by_symbol[symbol].get(other_lbl)

                    if (
                        other
                        and other["type"] == sig_type
                        and abs(
                            (
                                now - other["time"]
                            ).total_seconds()
                        ) <= 30
                    ):

                        emoji = (
                            "🟢"
                            if sig_type == "CALL"
                            else "🔴"
                        )

                        msg = (
                            f"{emoji} "
                            f"**INSTITUTIONAL DUAL MATCH** "
                            f"{emoji}\n\n"

                            f"**ACTION: BUY "
                            f"{symbol} {strike} "
                            f"{'CE' if sig_type == 'CALL' else 'PE'}**\n"

                            f"**SIGNAL: {sig_type} "
                            f"(Matched in "
                            f"{abs((now-other['time']).total_seconds()):.1f}s)**\n"

                            f"🛡️ **SL: {sl} pts | "
                            f"🎯 TARGET: {tg} pts**"
                        )

                        print(
                            f"🚨 FINAL ALERT => "
                            f"{symbol} {strike} {sig_type}",
                            flush=True
                        )

                        await safe_send(
                            client,
                            target_entity,
                            msg
                        )

                        last_signals_by_symbol[symbol] = {
                            "2 MIN FLOW": None,
                            "5 MIN FLOW": None
                        }

        except Exception as e:

            print(
                f"\n❌ HANDLER ERROR => {e}",
                flush=True
            )

    print("\n🚀 BOT RUNNING...\n", flush=True)

    await client.run_until_disconnected()


# ---------------- START ---------------- #

if __name__ == "__main__":

    asyncio.run(main())
