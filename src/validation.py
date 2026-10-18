
ALLOWED_SYMBOLS = {"BTC", "ETH", "SOL", "BTC-USD"}


def validate_event(event):
    """Validate a crypto price event. Return (is_valid, reason)."""

    if not isinstance(event, dict):
        return False, "Event must be a JSON object"

    required_fields = {"symbol", "price_usd", "timestamp"}

    if not required_fields.issubset(event):
        return False, "Missing required fields"

    if event["symbol"] not in ALLOWED_SYMBOLS:
        return False, "Unsupported symbol"

    try:
        price = float(event["price_usd"])
    except (TypeError, ValueError):
        return False, "Invalid price"

    if not (0 < price < 1_000_000_000):
        return False, "Price outside allowed range"

    if not isinstance(event["timestamp"], str) or not event["timestamp"].strip():
        return False, "Invalid timestamp"

    return True, "Valid event"
