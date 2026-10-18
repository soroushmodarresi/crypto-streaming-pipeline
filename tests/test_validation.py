
import unittest

from src.validation import validate_event


class TestValidateEvent(unittest.TestCase):

    def setUp(self):
        self.event = {
            "symbol": "BTC",
            "price_usd": 65000.0,
            "timestamp": "2026-10-19T10:00:00+00:00",
            "source": "coingecko",
        }

    def test_valid_event(self):
        valid, reason = validate_event(self.event)
        self.assertTrue(valid, reason)

    def test_rejects_unknown_symbol(self):
        self.event["symbol"] = "DOGE"
        valid, _ = validate_event(self.event)
        self.assertFalse(valid)

    def test_rejects_zero_price(self):
        self.event["price_usd"] = 0
        valid, _ = validate_event(self.event)
        self.assertFalse(valid)

    def test_rejects_negative_price(self):
        self.event["price_usd"] = -100
        valid, _ = validate_event(self.event)
        self.assertFalse(valid)

    def test_rejects_missing_timestamp(self):
        del self.event["timestamp"]
        valid, _ = validate_event(self.event)
        self.assertFalse(valid)

    def test_rejects_invalid_price(self):
        self.event["price_usd"] = "not-a-price"
        valid, _ = validate_event(self.event)
        self.assertFalse(valid)

    def test_rejects_non_object(self):
        valid, _ = validate_event(["BTC", 65000])
        self.assertFalse(valid)


if __name__ == "__main__":
    unittest.main()
