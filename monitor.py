"""
Monitor script. Stream, groups, aur pending messages ki info dikhata hai.
"""
import sys
import time
from stream_utils import r, STREAM_NAME, GROUP_NAME


def show_stream_info():
    """Stream ki basic info."""
    try:
        info = r.xinfo_stream(STREAM_NAME)
        print("=" * 60)
        print("STREAM INFO")
        print("=" * 60)
        print(f"  Length:            {info['length']}")
        print(f"  Radix tree keys:   {info.get('radix-tree-keys', 'N/A')}")
        print(f"  Last generated ID: {info.get('last-generated-id', 'N/A')}")
        print(f"  First entry:       {info.get('first-entry', 'N/A')}")
        print(f"  Last entry:        {info.get('last-entry', 'N/A')}")
    except Exception as e:
        print(f"Stream not found yet: {e}")


def show_groups():
    """Consumer groups ki info."""
    try:
        groups = r.xinfo_groups(STREAM_NAME)
        print("\n" + "=" * 60)
        print("CONSUMER GROUPS")
        print("=" * 60)
        for g in groups:
            print(f"  Group: {g['name']}")
            print(f"    Consumers:       {g['consumers']}")
            print(f"    Pending:         {g['pending']}")
            print(f"    Last delivered:  {g.get('last-delivered-id', 'N/A')}")
    except Exception as e:
        print(f"No groups: {e}")


def show_pending():
    """Pending messages ki summary."""
    print("\n" + "=" * 60)
    print("PENDING MESSAGES")
    print("=" * 60)

    try:
        # Basic summary
        summary = r.xpending(STREAM_NAME, GROUP_NAME)
        count = summary["pending"]
        print(f"  Total pending: {count}")

        if count == 0:
            return

        print(f"  Min ID: {summary['min']}")
        print(f"  Max ID: {summary['max']}")
        print(f"  Consumers:")
        for consumer, c_count in summary["consumers"]:
            print(f"    - {consumer}: {c_count} pending")

        # Detailed pending — full list
        print(f"\n  Detailed (first 10):")
        details = r.xpending_range(
            STREAM_NAME, GROUP_NAME,
            min="-", max="+",
            count=10
        )
        for d in details:
            print(f"    ID={d['message_id']}  consumer={d['consumer']}  "
                  f"idle={d['time_since_delivered']}ms  deliveries={d['times_delivered']}")

    except Exception as e:
        print(f"  Error: {e}")


if __name__ == "__main__":
    try:
        r.ping()
    except Exception:
        raise SystemExit("Start Redis first")

    while True:
        show_stream_info()
        show_groups()
        show_pending()
        print("\n" + "-" * 60)
        time.sleep(5)



        