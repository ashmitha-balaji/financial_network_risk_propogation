import os, json, time, yfinance as yf
from datetime import datetime

OUT_DIR = "/tmp/streams/market"
os.makedirs(OUT_DIR, exist_ok=True)

TICKERS = ["AAPL","MSFT","GOOG","AMZN"]

def write_json(record):
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    fname = f"{OUT_DIR}/market_{ts}.json"
    with open(fname, "w") as f:
        json.dump(record, f)
    print("📨 Market →", fname)

while True:
    data = yf.download(TICKERS, period="1d", interval="1m")
    last = data.tail(1)
    ts = str(last.index[0])

    for t in TICKERS:
        rec = {
            "stream_type": "market",
            "timestamp": ts,
            "payload": {
                "symbol": t,
                "price": float(last["Close"][t])
            }
        }
        write_json(rec)

    time.sleep(60)
