from datetime import datetime

# State Tracking
instrument_history = {}  # {token: [{'time': t, 'oi': oi, 'price': p}]}
pending_confirmations = {}  # {token: {'time': t, 'baseline_oi': x, 'baseline_price': y}}
TRIGGER_THRESHOLD_LOTS = 20
CONFIRMATION_SECONDS = 30
TRIGGER_THRESHOLDS_BY_SYMBOL = {
    "CRUDEOIL": 170,
    "NATURALGAS": 140,
}
BASE_TURNOVER_BY_SYMBOL = {
    "CRUDEOIL": 300000,
    "NATURALGAS": 72000,
    "SILVER": 200000,
    "GOLD": 150000,
}
def get_strength_label(lots):
    if lots >= 50:
        return "BLAST"
    if lots >= 25:
        return "AWESOME"
    if lots >= 10:
        return "VERY GOOD"
    return "GOOD"


def classify_option_moneyness(option_type, strike, future_price):
    if strike is None or future_price is None or future_price <= 0:
        return "NA"

    if option_type == "CE":
        return "ITM" if strike < future_price else "OTM"
    if option_type == "PE":
        return "ITM" if strike > future_price else "OTM"
    return "NA"


def format_moneyness_suffix(option_type, strike, future_price):
    moneyness = classify_option_moneyness(option_type, strike, future_price)
    if moneyness == "NA" or strike is None or future_price is None or future_price <= 0:
        return ""

    diff = round(strike - future_price, 2)
    return f" ({moneyness})({diff:g})"


def classify_action(inst, oi_change, price_change, future_price):
    if inst["type"] == "FUT":
        if oi_change > 0:
            return "FUTURE BUY (LONG)" if price_change >= 0 else "FUTURE SELL (SHORT)"
        return "FUTURE S_C" if price_change >= 0 else "FUTURE UNW"

    option_side = "CALL" if inst.get("option_type") == "CE" else "PUT"
    suffix = format_moneyness_suffix(inst.get("option_type"), inst.get("strike"), future_price)

    if oi_change > 0:
        return f"{option_side} BUY{suffix}" if price_change >= 0 else f"{option_side} WRITER{suffix}"
    return f"{option_side} S_C{suffix}" if price_change >= 0 else f"{option_side} UNW{suffix}"


def calculate_turnover(inst, oi_change, option_price, action):
    abs_oi_change = abs(int(oi_change))
    confirmed_lots = int(abs_oi_change / inst["lot_size"]) if inst["lot_size"] else 0
    base_turnover = BASE_TURNOVER_BY_SYMBOL.get(inst["name"], 300000)
    if inst["type"] == "FUT" or "WRITER" in action or "S_C" in action:
        return confirmed_lots * base_turnover
    return abs_oi_change * option_price


def format_turnover(value):
    if value >= 10000000:
        return f"{value / 10000000:.2f}Cr"
    if value >= 100000:
        return f"{value / 100000:.2f}L"
    return f"{value:,.2f}"


def process_mcx_tick(token, inst, ltp, oi, future_price, alerts_list):
    now = datetime.now()

    if token not in instrument_history:
        instrument_history[token] = []
    history = instrument_history[token]

    if not history:
        history.append({"time": now, "oi": oi, "price": ltp})
        return

    prev_oi = history[-1]["oi"]
    prev_price = history[-1]["price"]
    tick_oi_chg = oi - prev_oi
    trigger_threshold = TRIGGER_THRESHOLDS_BY_SYMBOL.get(inst["name"], TRIGGER_THRESHOLD_LOTS)
    tick_lots = int(abs(tick_oi_chg) / inst["lot_size"])

    pending = pending_confirmations.get(token)

    if pending is None and tick_lots >= trigger_threshold:
        pending_confirmations[token] = {
            "time": now,
            "baseline_oi": prev_oi,
            "baseline_price": prev_price,
        }
    elif pending is not None:
        confirmed_oi_chg = oi - pending["baseline_oi"]
        confirmed_lots = int(abs(confirmed_oi_chg) / inst["lot_size"])

        if confirmed_lots < trigger_threshold:
            pending_confirmations.pop(token, None)
        elif (now - pending["time"]).total_seconds() >= CONFIRMATION_SECONDS:
            price_change = ltp - pending["baseline_price"]
            strength = get_strength_label(confirmed_lots)
            action = classify_action(inst, confirmed_oi_chg, price_change, future_price)
            turnover = calculate_turnover(inst, confirmed_oi_chg, ltp, action)

            if inst["type"] == "OPT":
                moneyness = classify_option_moneyness(inst.get("option_type"), inst.get("strike"), future_price)
                if moneyness != "ITM":
                    pending_confirmations.pop(token, None)
                    history.append({"time": now, "oi": oi, "price": ltp})
                    if len(history) > 20:
                        history.pop(0)
                    return

            p_icon = "▲" if price_change >= 0 else "▼"

            alert_lines = [
                strength,
                f"ALERT: {action}",
                f"Symbol: {inst['symbol']}",
                "---------------",
                f"Lots: {confirmed_lots}",
                f"Turnover: {format_turnover(turnover)}",
            ]

            if inst["type"] == "OPT":
                alert_lines.append(f"Option Price: {ltp:.2f} ({p_icon})")
                if future_price is not None and future_price > 0:
                    alert_lines.append(f"Future Price: {future_price:.2f}")
                else:
                    alert_lines.append("Future Price: N/A")
            else:
                alert_lines.append(f"Price: {ltp:.2f} ({p_icon})")

            alert_lines.extend(
                [
                    "---------------",
                    f"Prev OI   : {pending['baseline_oi']:,}",
                    f"OI Change : {confirmed_oi_chg:+,d}",
                    f"New OI    : {oi:,}",
                    f"Time: {now.strftime('%H:%M:%S')}",
                ]
            )
            alerts_list.append("\n".join(alert_lines))
            pending_confirmations.pop(token, None)

    history.append({"time": now, "oi": oi, "price": ltp})
    if len(history) > 20:
        history.pop(0)
