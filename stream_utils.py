"""
Shared constants and helpers for the Streams demo.
Sab scripts iss module se common setup import karenge.
"""
import redis

STREAM_NAME = "orders"
GROUP_NAME = "order_workers"
CONSUMER_PREFIX = "worker"

# Redis connection — decode_responses=True se strings milte hain, bytes nahi
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def ensure_stream_and_group():
    """
    Idempotent setup:
    - Agar stream exist nahi karta, toh create ho jaayega (pehla XADD pe).
    - Consumer group banao agar pehle se nahi hai.
    """
    try:
        # mkstream=True → stream exists nahi karta toh bana do
        r.xgroup_create(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            id="$",         # $ = ab se aage ke messages se shuru karo
            mkstream=True
        )
        print(f"✅ Group '{GROUP_NAME}' created on stream '{STREAM_NAME}'")
    except redis.exceptions.ResponseError as e:
        # BUSYGROUP error aata hai agar group already exist karta hai
        if "BUSYGROUP" in str(e):
            print(f"ℹ️  Group '{GROUP_NAME}' already exists — reusing")
        else:
            raise



        