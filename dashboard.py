import threading
import time
from datetime import datetime

import pandas as pd
import streamlit as st

from angel_mcx_scanner import AngelMCXScanner


st.set_page_config(page_title="MCX Excel Board", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    .stApp, .main {
        background: #f4f6f8;
        color: #111827;
    }
    .board-title {
        font-size: 1.2rem;
        font-weight: 700;
        margin-bottom: 4px;
    }
    .board-sub {
        font-size: 0.82rem;
        color: #4b5563;
        margin-bottom: 10px;
    }
    .panel {
        background: #ffffff;
        border: 1px solid #cfd8e3;
        border-radius: 4px;
        padding: 0;
        overflow: hidden;
        margin-bottom: 10px;
    }
    .panel-head {
        padding: 6px 8px;
        font-size: 0.78rem;
        font-weight: 700;
        color: #ffffff;
        text-transform: uppercase;
    }
    .panel-body {
        padding: 8px;
        font-size: 0.82rem;
    }
    .grid-3 {
        display: grid;
        grid-template-columns: 1fr 1fr 1fr;
        gap: 8px;
    }
    .metric-box {
        border: 1px solid #d7dee7;
        border-radius: 3px;
        background: #f9fbfc;
        padding: 6px 8px;
    }
    .metric-label {
        font-size: 0.68rem;
        color: #6b7280;
        text-transform: uppercase;
    }
    .metric-value {
        font-size: 1rem;
        font-weight: 700;
        color: #111827;
    }
    .put-head { background: #dc2626; }
    .call-head { background: #2563eb; }
    .sum-head { background: #111827; }
    .ladder-head { background: #f59e0b; color: #111827; }
    .small-note {
        font-size: 0.74rem;
        color: #4b5563;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_engine():
    scanner = AngelMCXScanner()
    if not scanner.initialize():
        st.error("Failed to initialize Angel MCX scanner. Check environment variables and login details.")
        st.stop()
    scanner.start_background_stream()
    return scanner


def ensure_worker(scanner):
    if st.session_state.get("dashboard_worker_started"):
        return

    def worker():
        while True:
            try:
                scanner.process_once(send_alerts=False)
                time.sleep(5)
            except Exception:
                time.sleep(5)

    threading.Thread(target=worker, daemon=True).start()
    st.session_state.dashboard_worker_started = True


def collect_option_rows(scanner, commodity):
    future_price = scanner.latest_future_price.get(commodity, 0.0)
    rows = []

    for token, inst in scanner.instruments.items():
        if inst["name"] != commodity or inst["type"] != "OPT":
            continue

        tick = scanner.latest_data.get(token, {})
        price = round(tick.get("ltp", 0.0), 2)
        oi = int(tick.get("oi", 0))
        strike = inst.get("strike")
        option_type = inst.get("option_type")

        zone = ""
        diff = None
        if strike is not None and future_price:
            diff = round(strike - future_price, 2)
            if option_type == "CE":
                zone = "ITM" if strike < future_price else "OTM"
            else:
                zone = "ITM" if strike > future_price else "OTM"

        rows.append(
            {
                "token": token,
                "symbol": inst["symbol"],
                "strike": strike or 0.0,
                "type": option_type,
                "price": price,
                "oi": oi,
                "lots": int(oi / inst["lot_size"]) if inst["lot_size"] else 0,
                "zone": zone,
                "diff": diff,
            }
        )

    return pd.DataFrame(rows)


def build_ladder(df, future_price):
    if df.empty:
        return pd.DataFrame()

    strikes = sorted(df["strike"].dropna().unique())
    if not strikes:
        return pd.DataFrame()

    center = min(strikes, key=lambda x: abs(x - future_price)) if future_price else strikes[len(strikes) // 2]
    center_idx = strikes.index(center)
    selected = strikes[max(0, center_idx - 4): center_idx + 5]

    ce = df[df["type"] == "CE"].set_index("strike")
    pe = df[df["type"] == "PE"].set_index("strike")

    rows = []
    for strike in selected:
        ce_row = ce.loc[strike] if strike in ce.index else None
        pe_row = pe.loc[strike] if strike in pe.index else None
        rows.append(
            {
                "CE Price": getattr(ce_row, "price", ""),
                "CE Lots": getattr(ce_row, "lots", ""),
                "CE Zone": getattr(ce_row, "zone", ""),
                "Strike": strike,
                "PE Zone": getattr(pe_row, "zone", ""),
                "PE Lots": getattr(pe_row, "lots", ""),
                "PE Price": getattr(pe_row, "price", ""),
            }
        )
    return pd.DataFrame(rows)


def latest_side_alert(scanner, commodity, side_keyword):
    for item in list(scanner.recent_alerts):
        msg = item["message"]
        if commodity in msg and side_keyword in msg:
            lines = msg.splitlines()
            return {
                "time": item["time"],
                "title": lines[1].replace("ALERT: ", "") if len(lines) > 1 else msg,
                "turnover": next((line.split(": ", 1)[1] for line in lines if line.startswith("Turnover:")), "-"),
                "price": next((line.split(": ", 1)[1] for line in lines if "Price:" in line), "-"),
                "oi_change": next((line.split(": ", 1)[1] for line in lines if line.startswith("OI Change")), "-"),
            }
    return {"time": "-", "title": "No recent signal", "turnover": "-", "price": "-", "oi_change": "-"}


def build_signal_matrix(scanner, commodity):
    put_buy = latest_side_alert(scanner, commodity, "PUT BUY")
    put_writer = latest_side_alert(scanner, commodity, "PUT WRITER")
    put_sc = latest_side_alert(scanner, commodity, "PUT S_C")
    put_unw = latest_side_alert(scanner, commodity, "PUT UNW")

    call_buy = latest_side_alert(scanner, commodity, "CALL BUY")
    call_writer = latest_side_alert(scanner, commodity, "CALL WRITER")
    call_sc = latest_side_alert(scanner, commodity, "CALL S_C")
    call_unw = latest_side_alert(scanner, commodity, "CALL UNW")

    return {
        "put": [put_buy, put_writer, put_sc, put_unw],
        "call": [call_buy, call_writer, call_sc, call_unw],
    }


def get_future_symbol(scanner, commodity):
    return next(
        (meta["symbol"] for meta in scanner.instruments.values() if meta["name"] == commodity and meta["type"] == "FUT"),
        "N/A",
    )


scanner = get_engine()
ensure_worker(scanner)

st.sidebar.title("MCX Excel Board")
commodity = st.sidebar.selectbox("Commodity", ["CRUDEOIL", "GOLD", "SILVER", "NATURALGAS"])
auto_refresh = st.sidebar.slider("Refresh (sec)", 3, 30, 5)

future_price = scanner.latest_future_price.get(commodity, 0.0)
future_symbol = get_future_symbol(scanner, commodity)
option_df = collect_option_rows(scanner, commodity)
ladder_df = build_ladder(option_df, future_price)
signals = build_signal_matrix(scanner, commodity)

st.markdown('<div class="board-title">MCX Signal Board</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="board-sub">Excel-style live board for {commodity} based on the same MCX alert engine</div>',
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <div class="panel">
      <div class="panel-head sum-head">Market Summary</div>
      <div class="panel-body">
        <div class="grid-3">
          <div class="metric-box">
            <div class="metric-label">Commodity</div>
            <div class="metric-value">{commodity}</div>
          </div>
          <div class="metric-box">
            <div class="metric-label">Near Future</div>
            <div class="metric-value">{future_price:,.2f}</div>
            <div class="small-note">{future_symbol}</div>
          </div>
          <div class="metric-box">
            <div class="metric-label">Updated</div>
            <div class="metric-value">{datetime.now().strftime("%H:%M:%S")}</div>
            <div class="small-note">Near expiry contracts only</div>
          </div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns([1.05, 1.05])

with left:
    st.markdown('<div class="panel"><div class="panel-head put-head">PUT Side Signals</div><div class="panel-body">', unsafe_allow_html=True)
    put_rows = pd.DataFrame(
        [
            {"State": "BUY", "Signal": signals["put"][0]["title"], "Turnover": signals["put"][0]["turnover"], "OI Chg": signals["put"][0]["oi_change"]},
            {"State": "WRITER", "Signal": signals["put"][1]["title"], "Turnover": signals["put"][1]["turnover"], "OI Chg": signals["put"][1]["oi_change"]},
            {"State": "S_C", "Signal": signals["put"][2]["title"], "Turnover": signals["put"][2]["turnover"], "OI Chg": signals["put"][2]["oi_change"]},
            {"State": "UNW", "Signal": signals["put"][3]["title"], "Turnover": signals["put"][3]["turnover"], "OI Chg": signals["put"][3]["oi_change"]},
        ]
    )
    st.dataframe(put_rows, use_container_width=True, height=210)
    st.markdown("</div></div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="panel"><div class="panel-head call-head">CALL Side Signals</div><div class="panel-body">', unsafe_allow_html=True)
    call_rows = pd.DataFrame(
        [
            {"State": "BUY", "Signal": signals["call"][0]["title"], "Turnover": signals["call"][0]["turnover"], "OI Chg": signals["call"][0]["oi_change"]},
            {"State": "WRITER", "Signal": signals["call"][1]["title"], "Turnover": signals["call"][1]["turnover"], "OI Chg": signals["call"][1]["oi_change"]},
            {"State": "S_C", "Signal": signals["call"][2]["title"], "Turnover": signals["call"][2]["turnover"], "OI Chg": signals["call"][2]["oi_change"]},
            {"State": "UNW", "Signal": signals["call"][3]["title"], "Turnover": signals["call"][3]["turnover"], "OI Chg": signals["call"][3]["oi_change"]},
        ]
    )
    st.dataframe(call_rows, use_container_width=True, height=210)
    st.markdown("</div></div>", unsafe_allow_html=True)

bottom_left, bottom_right = st.columns([1.45, 0.9])

with bottom_left:
    st.markdown('<div class="panel"><div class="panel-head ladder-head">Strike Ladder</div><div class="panel-body">', unsafe_allow_html=True)
    if ladder_df.empty:
        st.info("Waiting for live option ticks...")
    else:
        st.dataframe(ladder_df, use_container_width=True, height=330)
    st.markdown("</div></div>", unsafe_allow_html=True)

with bottom_right:
    st.markdown('<div class="panel"><div class="panel-head sum-head">Latest Alerts</div><div class="panel-body">', unsafe_allow_html=True)
    alert_rows = []
    for item in list(scanner.recent_alerts):
        if commodity in item["message"]:
            lines = item["message"].splitlines()
            alert_rows.append(
                {
                    "Time": item["time"],
                    "Alert": lines[1].replace("ALERT: ", "") if len(lines) > 1 else item["message"],
                    "Turnover": next((line.split(": ", 1)[1] for line in lines if line.startswith("Turnover:")), "-"),
                }
            )
    if alert_rows:
        st.dataframe(pd.DataFrame(alert_rows[:10]), use_container_width=True, height=330)
    else:
        st.info("No recent alerts for this commodity.")
    st.markdown("</div></div>", unsafe_allow_html=True)

time.sleep(auto_refresh)
st.rerun()
