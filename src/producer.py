
import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Published {msg.key().decode()} to "
            f"{msg.topic()} [partition {msg.partition()}, "
            f"offset {msg.offset()}]"
        )


def fetch_prices():
    response = requests.get(
        API_URL,
        params={
            "ids": ",".join(COINS),
            "vs_currencies": "usd",
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Streaming crypto prices. Press Ctrl+C to stop.")

    try:
        while True:
            try:
                prices = fetch_prices()
                timestamp = datetime.now(timezone.utc).isoformat()

                for coin_id, symbol in COINS.items():
                    price = prices.get(coin_id, {}).get("usd")
                    if price is None:
                        print(f"No USD price returned for {symbol}")
                        continue

                    event = {
                        "symbol": symbol,
                        "price_usd": price,
                        "timestamp": timestamp,
                        "source": "coingecko",
                    }

                    producer.produce(
                        topic="crypto-prices",
                        key=symbol,
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_report,
                    )

                producer.poll(0)
                producer.flush(10)

            except (requests.RequestException, ValueError) as exc:
                print(f"Price fetch failed: {exc}")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
