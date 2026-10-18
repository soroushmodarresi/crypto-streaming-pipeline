
import json
import os
from validation import validate_event

import pyodbc
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException

load_dotenv()

TOPIC = "crypto-prices"


def connect_to_database():
    password = os.environ["MSSQL_SA_PASSWORD"]

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('MSSQL_HOST', 'localhost')},"
        f"{os.getenv('MSSQL_PORT', '1433')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE', 'CryptoDB')};"
        f"UID={os.getenv('MSSQL_USER', 'sa')};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(connection_string)


def main():
    db = connect_to_database()
    cursor = db.cursor()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "crypto-price-sql-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])
    print("Consuming crypto prices into SQL Server. Press Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                event = json.loads(message.value().decode("utf-8"))

                                # Support older sample events using "price".
                if (
                    isinstance(event, dict)
                    and "price_usd" not in event
                    and "price" in event
                ):
                    event["price_usd"] = event["price"]

                is_valid, reason = validate_event(event)

                if not is_valid:
                    print(f"Skipping invalid event: {reason}")
                    consumer.commit(message=message, asynchronous=False)
                    continue

                price = float(event["price_usd"])

                # Validate price.
                try:
                    price = float(event["price_usd"])
                except (TypeError, ValueError):
                    print(f"Skipping invalid price: {event['price_usd']}")
                    consumer.commit(message=message, asynchronous=False)
                    continue

                if not (0 < price < 1000000000):
                    print(f"Skipping out-of-range price: {price}")
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if not isinstance(event, dict) or not required_fields.issubset(event):
                    print(f"Skipping invalid event: {event}")
                    consumer.commit(message=message, asynchronous=False)
                    continue

                cursor.execute(
                    """
                    INSERT INTO dbo.CryptoPrices
                        (Symbol, PriceUsd, EventTimestamp, Source)
                    VALUES (?, ?, ?, ?)
                    """,
                    event["symbol"],
                    price,
                    event["timestamp"],
                    event.get("source", "unknown"),
                )
                db.commit()

                consumer.commit(message=message, asynchronous=False)

                print(
                    f"Saved {event['symbol']} at "
                    f"${event['price_usd']} to SQL Server"
                )

            except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
                print(f"Skipping malformed event: {exc}")
                consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        cursor.close()
        db.close()


if __name__ == "__main__":
    main()
