import os


BASE_DIR = os.path.dirname(__file__)
ENV_FILE = os.path.join(BASE_DIR, ".env")


def _load_local_env():
    if not os.path.exists(ENV_FILE):
        return

    try:
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()

                if not key:
                    continue

                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]

                os.environ.setdefault(key, value)
    except Exception as e:
        print(f"Warning: failed to load .env file: {e}")


def _clean_env(name):
    value = os.getenv(name, "").strip()
    return value or None


_load_local_env()


# Angel One credentials must come from environment variables.
ANGEL_API_KEY = _clean_env("ANGEL_API_KEY")
ANGEL_CLIENT_ID = _clean_env("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = _clean_env("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = _clean_env("ANGEL_TOTP_SECRET")

# Telegram credentials are optional.
TELE_TOKEN = _clean_env("TELE_TOKEN_MCX")
TELE_CHAT_ID = _clean_env("CHAT_ID_MCX")
FUTURE_SCANNER_TELE_TOKEN = _clean_env("FUTURE_SCANNER_TELE_TOKEN") or TELE_TOKEN
FUTURE_SCANNER_CHAT_ID = _clean_env("FUTURE_SCANNER_CHAT_ID") or TELE_CHAT_ID
PAPER_TRADE_TELE_TOKEN = _clean_env("PAPER_TRADE_BOT_TOKEN") or TELE_TOKEN
PAPER_TRADE_CHAT_ID = _clean_env("PAPER_TRADE_CHANNEL_ID") or TELE_CHAT_ID
SOURCE_CHAT = _clean_env("SOURCE_CHAT") or "Marketmenia_news"
TG_API_ID = _clean_env("TG_API_ID")
TG_API_HASH = _clean_env("TG_API_HASH")
TG_SESSION_STR = _clean_env("TG_SESSION_STR")

# NFO 1H futures scanner settings.
GAP_THRESHOLD_PERCENT = 1.5
FLAT_VOLUME_THRESHOLD_PERCENT = 26.0
VWAP_REFRESH_SECONDS = 60
LOGIC_START_TIME = "10:15"
