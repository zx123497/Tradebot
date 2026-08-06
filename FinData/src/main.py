import io
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import websocket
import wikipedia as wp
from dotenv import load_dotenv

from kafka_producer import TradeKafkaProducer

wp.set_user_agent(
    "sp500-updater (https://github.com/fja05680/sp500; contact: fja0568@gmail.com)"
)

pd.options.mode.chained_assignment = None
pd.set_option("display.max_rows", 600)

FIN_DATA_DIR = Path(__file__).resolve().parent.parent
SP500_CSV = FIN_DATA_DIR / "sp500.csv"
FINNHUB_WS_URL = "wss://ws.finnhub.io?token={token}"


def get_table(title: str, filename: Path, match: str, use_cache: bool = False) -> pd.DataFrame:
    if not (use_cache and filename.is_file()):
        html = wp.page(title).html()
        df = pd.read_html(io.StringIO(html), header=0, match=match)[0]
        df.to_csv(filename, header=True, index=False, encoding="utf-8")

    return pd.read_csv(filename)


def get_sp500_symbols(gics_sector: str | None = None) -> list[str]:
    sp500 = get_table("List of S&P 500 companies", SP500_CSV, match="Symbol", use_cache=True)
    if gics_sector:
        sp500 = sp500[sp500["GICS Sector"] == gics_sector]
    return sp500["Symbol"].tolist()


def run_producer() -> None:
    load_dotenv(FIN_DATA_DIR / ".env")

    api_key = os.environ.get("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is required in FinData/.env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "sp500.trades")
    gics_sector = os.getenv("GICS_SECTOR", "Information Technology")

    symbol_limit = os.getenv("SYMBOL_LIMIT")

    symbols = get_sp500_symbols(gics_sector=gics_sector)
    print(f"Found {len(symbols)} symbols in {gics_sector}")
    if symbol_limit:
        symbols = symbols[: int(symbol_limit)]

    producer = TradeKafkaProducer(bootstrap_servers=bootstrap_servers, topic=topic)

    def on_open(ws: websocket.WebSocketApp) -> None:
        for symbol in symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(
            f"Subscribed to {len(symbols)} symbols "
            f"({gics_sector}) on topic '{topic}'"
        )

    def on_message(_ws: websocket.WebSocketApp, message: str) -> None:
        msg = json.loads(message)
        if msg.get("type") != "trade":
            return

        for trade in msg.get("data", []):
            producer.publish_trade(trade)
            print(
                f"{trade['s']} @ {trade['p']} "
                f"(vol={trade['v']}, ts={datetime.fromtimestamp(trade['t'] / 1000)})"
            )

    def on_error(_ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"WebSocket error: {error}")

    def on_close(_ws: websocket.WebSocketApp, *_args) -> None:
        print("WebSocket closed, flushing Kafka producer")
        producer.close()

    ws = websocket.WebSocketApp(
        FINNHUB_WS_URL.format(token=api_key),
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()


if __name__ == "__main__":
    run_producer()
