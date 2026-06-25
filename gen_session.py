from telethon.sync import TelegramClient
from telethon.sessions import StringSession


def main() -> None:
    print("--- TELEGRAM SESSION GENERATOR ---")
    api_id = int(input("Enter TG_API_ID: ").strip())
    api_hash = input("Enter TG_API_HASH: ").strip()

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\n--- TG_SESSION_STR ---")
        print(client.session.save())
        print("--- COPY THE VALUE ABOVE ---")


if __name__ == "__main__":
    main()
