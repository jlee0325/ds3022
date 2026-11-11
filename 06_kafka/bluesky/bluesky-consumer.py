from quixstreams import Application
from collections import Counter
import json
import time
import redis

"""
Consumes Bluesky events from the `bluesky-events` topic and counts them by commit
operation (create / update / delete). Counts are buffered in memory and flushed to a
single Redis hash (`bluesky:operations`) in batches; running totals print on Ctrl-C.
"""

REDIS_KEY = "bluesky:operations"   # One hash for this project; fields are operation names
FLUSH_EVERY_MSGS = 1000            # Flush to Redis after this many messages...
FLUSH_EVERY_SECS = 5               # ...or after this many seconds, whichever comes first

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)


def flush(consumer, pending, last_msgs):
    """Write buffered counts to Redis in one round-trip, then store each partition's last offset.
    Offsets are stored only after Redis succeeds, so a crash re-counts a batch rather than losing it."""
    if pending:
        pipe = redis_client.pipeline()
        for op, n in pending.items():
            pipe.hincrby(REDIS_KEY, op, n)
        pipe.execute()
        pending.clear()
    for msg in last_msgs.values():
        consumer.store_offsets(msg)
    last_msgs.clear()


def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="INFO",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    pending = Counter()      # Counts not yet written to Redis
    last_msgs = {}           # Latest message per partition in the current batch (for offset storage)
    batch_size = 0
    last_flush = time.time()

    with app.get_consumer() as consumer:
        consumer.subscribe(["bluesky-events"])
        try:
            while True:
                msg = consumer.poll(1)

                if msg is None:
                    print("Waiting...")
                elif msg.error() is not None:
                    raise Exception(msg.error())
                else:
                    # Count the commit operation; skip (but still advance past) malformed messages
                    try:
                        value = json.loads(msg.value())
                        pending[value['commit']['operation']] += 1
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        print(f"Skipping bad message at offset {msg.offset()} ({type(e).__name__}: {e})")
                    last_msgs[msg.partition()] = msg
                    batch_size += 1

                # Flush on batch size or elapsed time (the time check also covers quiet periods)
                if batch_size >= FLUSH_EVERY_MSGS or time.time() - last_flush >= FLUSH_EVERY_SECS:
                    flush(consumer, pending, last_msgs)
                    batch_size, last_flush = 0, time.time()
        finally:
            # Ctrl-C or error: write whatever is still buffered before the consumer closes and commits
            flush(consumer, pending, last_msgs)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Print running totals from this project's hash only
        for op, count in sorted(redis_client.hgetall(REDIS_KEY).items()):
            print(f"Operation: {op.decode('utf8')} | Count: {count.decode('utf8')}")
        redis_client.close()
