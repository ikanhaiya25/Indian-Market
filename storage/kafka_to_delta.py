import json
import pandas as pd
from kafka import KafkaConsumer
from deltalake.writer import write_deltalake

TOPIC = "market-bars"
DELTA_PATH = "data/delta/bronze_bars"
BATCH_SIZE = 20

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="delta_writer",
)

buffer = []
for msg in consumer:
    buffer.append(msg.value)
    if len(buffer) >= BATCH_SIZE:
        df = pd.DataFrame(buffer)
        write_deltalake(DELTA_PATH, df, mode="append")
        print(f"Wrote {len(buffer)} rows to delta")
        buffer = []