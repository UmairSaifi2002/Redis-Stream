"""
Producer script. Orders generate karta hai aur stream mein daalta hai.
"""
import time
import random
import json
from stream_utils import r, STREAM_NAME, ensure_stream_and_group


def publish_order(order: dict) -> str:
    """
    Stream mein naya message add karo (XADD).
    Return: Redis-generated message ID (jaise '1704067200000-0')
    """
    # XADD: stream name, auto-ID (*), aur fields (dict)
    # MAXLEN ~ 10000 se stream bounded rehti hai — memory blow nahi hoga
    message_id = r.xadd(
        name=STREAM_NAME,
        fields=order,
        id="*",               # * = auto-generate timestamp-based ID
        maxlen=10000,         # keep at most ~10000 messages
        approximate=True      # ~ → faster trimming
    )
    return message_id


def make_order(order_id: int) -> dict:
    """Ek random order ka data."""
    return {
        "order_id": str(order_id),      # Streams fields must be strings
        "user_id": str(random.randint(1, 100)),
        "items": str(random.randint(1, 5)),
        "total": str(round(random.uniform(100, 2000), 2)),
    }


if __name__ == "__main__":
    try:
        r.ping()
    except Exception:
        raise SystemExit("Start Redis first: sudo systemctl start redis-server")

    ensure_stream_and_group()

    print(f"\n🏭 Producer started. Adding to stream '{STREAM_NAME}'...")
    print(f"   (Ctrl+C to stop)\n")

    order_id = 10000

    try:
        while True:
            order = make_order(order_id)
            msg_id = publish_order(order)

            # Stream ki total length print karo
            length = r.xlen(STREAM_NAME)

            print(f"📤 [{msg_id}]  order={order['order_id']}  "
                  f"user={order['user_id']}  total=${order['total']}  "
                  f"(stream length: {length})")

            order_id += 1
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n👋 Producer stopped.")



        