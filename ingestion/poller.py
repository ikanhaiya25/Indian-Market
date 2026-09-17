import json
import time
import logging
import yfinance as yf
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("poller")

SYMBOLS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS"]
TOPIC = "market-bars"
POLL_INTERVAL_SEC = 60  # don't go lower — avoid rate limiting

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

def poll_and_publish():
    try:
        data = yf.download(
            tickers=SYMBOLS, period="1d", interval="1m",
            group_by="ticker", progress=False, threads=True,
        )
    except Exception as e:
        log.error(f"yfinance download failed: {e}")
        return
    for symbol in SYMBOLS:
        try:
            sym_data = data[symbol].dropna(subset=["Volume"])  # drop unfinished/incomplete bars
            if sym_data.empty:
                log.warning(f"No complete bars available yet for {symbol}")
                continue
            latest = sym_data.iloc[-1]
            ts = sym_data.index[-1]
            msg = {
                "symbol": symbol,
                "interval": "1m",
                "timestamp": str(ts),
                "open": float(latest["Open"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"]),
                "close": float(latest["Close"]),
                "volume": int(latest["Volume"]),
            }
            producer.send(TOPIC, value=msg)
            log.info(f"Published {symbol} @ {ts}")
        except Exception as e:
            log.warning(f"Skipping {symbol}: {e}")

    producer.flush()

if __name__ == "__main__":
    while True:
        poll_and_publish()
        time.sleep(POLL_INTERVAL_SEC)