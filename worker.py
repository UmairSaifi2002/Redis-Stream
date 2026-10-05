"""
Consumer script using consumer group.
Message padhta hai, process karta hai, aur XACK se acknowledge karta hai.
"""
import time
import json
import sys
import random
import redis
from stream_utils import r, STREAM_NAME, GROUP_NAME, CONSUMER_PREFIX


def process_order(order: dict, worker_id: str):

    print(f"  [{worker_id}] Processing order #{order['order_id']} "
          f"(user={order['user_id']}, ${order['total']})")

    # Simulate kaam ka time
    time.sleep(random.uniform(0.5, 1.5))

    # 10% chance failure — crash simulate karne ke liye
    if random.random() < 0.1:
        raise RuntimeError(f"Simulated failure on order {order['order_id']}")

    print(f"  [{worker_id}] ✅ Order #{order['order_id']} done")


def process_batch(worker_id: str):
    """
    Ek batch messages ko read karo group se, aur process karo.
    """
    # XREADGROUP:
    # - GROUP <group> <consumer>       → kaun sa group aur kaun consumer
    # - COUNT <n>                       → ek baar mein max n messages
    # - BLOCK <ms>                      → naye messages ke liye kitni der wait
    # - STREAMS <stream> >              → > ka matlab: naye messages
    response = r.xreadgroup(
        groupname=GROUP_NAME,
        consumername=worker_id,
        streams={STREAM_NAME: ">"},
        count=5,                # 5 messages ek baar mein
        block=5000              # 5 second block (0 = infinite)
    )

    if not response:
        return   # timeout ho gaya, koi message nahi

    # response format: [[stream_name, [(msg_id, fields), ...]]]
    for stream_name, messages in response:
        for msg_id, fields in messages:
            try:
                process_order(fields, worker_id)

                # ✅ SUCCESS → acknowledge
                acked = r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                print(f"  [{worker_id}] 🔔 ACKed {msg_id} ({acked})")

            except Exception as e:
                # ❌ FAILURE → acknowledge nahi karo
                # Message PEL mein rahega, koi doosra worker claim kar sakta hai
                print(f"  [{worker_id}] ❌ FAILED {msg_id}: {e}  "
                      f"(left pending)")


def main():
    worker_id = sys.argv[1] if len(sys.argv) > 1 else f"{CONSUMER_PREFIX}-1"

    print(f"👷 [{worker_id}] Worker started.")
    print(f"   Stream: {STREAM_NAME}   Group: {GROUP_NAME}")
    print(f"   (Ctrl+C to stop)\n")

    try:
        while True:
            process_batch(worker_id)
    except KeyboardInterrupt:
        print(f"\n👋 [{worker_id}] Stopped.")


if __name__ == "__main__":
    try:
        r.ping()
    except Exception:
        raise SystemExit("Start Redis first: sudo systemctl start redis-server")
    main()

