#!/usr/bin/env python3

"""
<<<<<<< HEAD
Reads puzzle messages from the "puzzle" topic, groups the letters by cycle,
and once a cycle has all `total` letters, sorts them by `order_no` and prints
the reassembled phrase.
=======
Reads puzzle messages from the "puzzle" topic and reassembles the quote three
ways for comparison: (1) slotted Piece objects sorted by "order_no",
(2) a plain order_no -> letter dict sorted by key, and (3) a DuckDB table
(saved to puzzle.duckdb in this directory) queried with SQL. All three should print the same.
>>>>>>> upstream
"""

import os
import json
<<<<<<< HEAD
from datetime import datetime, timezone
=======
import duckdb
>>>>>>> upstream
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "puzzle"
MAX_EMPTY_POLLS = 5
<<<<<<< HEAD
=======
DUCKDB_FILE = "puzzle.duckdb"
>>>>>>> upstream

# Kafka app; start from the oldest message. Uncomment extra_config to force IPv4 if using "localhost"
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="puzzle-consumer",
    auto_offset_reset="earliest",
    # consumer_extra_config={"broker.address.family": "v4"},
)

<<<<<<< HEAD
# Sort one cycle's letters by order_no and join them (spaces included)
def assemble(letters):
    return "".join(letters[n] for n in sorted(letters))

# Collect messages until one cycle is complete or the topic goes quiet for MAX_EMPTY_POLLS seconds
def consume_messages():
    # cycle_id -> {order_no: letter}; a dict also drops any duplicate deliveries
    cycles = {}
    totals = {}
=======
# One letter of the quote and its position.
# __slots__ declares the only attributes a Piece can have. Python then stores
# them in fixed slots instead of a per-object __dict__, which uses less memory
# per object (useful when a stream delivers many small records) and raises an
# AttributeError if you mistype an attribute name (e.g. p.ordr_no = 3).
class Piece:
    __slots__ = ("order_no", "letter")

    def __init__(self, order_no, letter):
        self.order_no = order_no
        self.letter = letter

# Collect every message until the topic goes quiet, then sort and reassemble
def consume_messages():
    # Method 1 (slots): order_no -> Piece object.
    # Keyed by order_no so a duplicate message overwrites rather than repeats a letter
    pieces = {}
    # Method 2 (dict+sort): plain order_no -> letter mapping, no class needed
    letters = {}
    # Method 3 (DuckDB): raw (order_no, letter) rows, duplicates included; SQL handles them
    rows = []
>>>>>>> upstream
    try:
        # No offset commits, so every run re-reads the topic from the start
        with app.get_consumer(auto_commit_enable=False) as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            empty_polls = 0
            while empty_polls < MAX_EMPTY_POLLS:
                msg = consumer.poll(1)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                empty_polls = 0
<<<<<<< HEAD
                # here is the real message data for each log entry:
                # each are part of the `msg`: msg.key, msg.timestamp(), msg.value()

                # key / timestamp / value
                #key = msg.key().decode("utf8")

                # Kafka timestamps are (type, epoch milliseconds)
                #ts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc).isoformat()

                # value is the actual json payload of the message content
                value = json.loads(msg.value())

                #print(f"key={key} timestamp={ts} value={json.dumps(value)}")

                # Store the letter under its cycle and position
                cycle_id = value["cycle_id"]
                cycles.setdefault(cycle_id, {})[value["order_no"]] = value["letter"]
                totals[cycle_id] = value["total"]

                # Use `total` to tell when this cycle has every letter
                if len(cycles[cycle_id]) == totals[cycle_id]:
                    print(f"Cycle {cycle_id} complete ({totals[cycle_id]} letters):")
                    print(repr(assemble(cycles[cycle_id])))
                    return

            # Topic went quiet before any cycle finished; show what we have
            if not cycles:
                print("No messages found. Did the producer run?")
            for cycle_id, letters in cycles.items():
                print(f"Cycle {cycle_id} incomplete: {len(letters)}/{totals[cycle_id]} letters")
                print(repr(assemble(letters)))
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

=======

                # value is the JSON payload; wrap the two fields we need in a Piece
                value = json.loads(msg.value())
                piece = Piece(int(value["order_no"]), value["letter"])
                pieces[piece.order_no] = piece

                # Same data stored the simple way: the dict key is the position
                letters[int(value["order_no"])] = value["letter"]

                # Same data again as a row tuple for loading into DuckDB
                rows.append((int(value["order_no"]), value["letter"]))
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

    # Method 1 (slots): messages arrive out of order; sort the Pieces by their order_no slot
    ordered = sorted(pieces.values(), key=lambda p: p.order_no)
    quote = "".join(p.letter for p in ordered)
    print(f"Received {len(ordered)} pieces")
    print(f"Quote (slots):     {quote}")

    # Method 2 (dict+sort): sorted() on a dict returns its keys in order,
    # so look up each letter by its sorted order_no and join them
    quote2 = "".join(letters[n] for n in sorted(letters))
    print(f"Quote (dict+sort): {quote2}")

    # Method 3 (DuckDB): load rows into a table in a local DuckDB file and let SQL do the work.
    # DISTINCT drops duplicate messages; string_agg(... ORDER BY order_no)
    # concatenates the letters in position order into a single string.
    try:
        con = duckdb.connect(DUCKDB_FILE)  # creates the file on first run
        # OR REPLACE so each run starts from a fresh table instead of failing or appending
        con.execute("CREATE OR REPLACE TABLE pieces (order_no INTEGER, letter VARCHAR)")
        con.executemany("INSERT INTO pieces VALUES (?, ?)", rows)
        quote3 = con.execute("""
            SELECT string_agg(letter, '' ORDER BY order_no)
            FROM (SELECT DISTINCT order_no, letter FROM pieces)
        """).fetchone()[0]
        print(f"Quote (DuckDB):    {quote3}")
        con.close()
    except Exception as e:
        print(f"DuckDB error: {e}")

>>>>>>> upstream
if __name__ == "__main__":
    consume_messages()
