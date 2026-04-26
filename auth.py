import pyotp
from SmartApi.smartConnect import SmartConnect

from env_config import ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET


def _validate_credentials():
    missing = []
    for name, value in {
        "ANGEL_API_KEY": ANGEL_API_KEY,
        "ANGEL_CLIENT_ID": ANGEL_CLIENT_ID,
        "ANGEL_PASSWORD": ANGEL_PASSWORD,
        "ANGEL_TOTP_SECRET": ANGEL_TOTP_SECRET,
    }.items():
        if not value:
            missing.append(name)
    return missing


def get_angel_session():
    missing = _validate_credentials()
    if missing:
        print(f"Missing Angel credentials: {', '.join(missing)}")
        return None, None

    try:
        print("Generating TOTP for Angel One...")
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()

        print(f"Logging in as {ANGEL_CLIENT_ID}...")
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

        if session["status"]:
            print("Login Successful!")
            return obj, session["data"]

        print(f"Login Failed: {session['message']}")
        return None, None

    except Exception as e:
        print(f"Auth Error: {e}")
        return None, None
