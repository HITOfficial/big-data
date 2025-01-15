import datetime
import json

from alpaca.data.live import CryptoDataStream
from alpaca.data.models.orderbooks import Orderbook, OrderbookQuote
from config import API_KEY, KAFKA_BOOTSTRAP_SERVER, SECRET_KEY
from handler import kafka_produce_message
from kafka import KafkaProducer

usd_pairs = [
    "AAVE/USD",
    "AVAX/USD",
    "BAT/USD",
    "BCH/USD",
    "BTC/USD",
    "CRV/USD",
    "DOGE/USD",
    "DOT/USD",
    "ETH/USD",
    "GRT/USD",
    # "LINK/USD",  limit exceeded (405) if use more pairs
    # "LTC/USD",
    # "MKR/USD",
    # "SHIB/USD",
    # "SUSHI/USD",
    # "UNI/USD",
    # "USDT/USD",
    # "XTZ/USD",
    # "YFI/USD",
]


def custom_serializer(obj):
    def inner_serializer(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Convert datetime to string
        if isinstance(obj, OrderbookQuote):
            return obj.__dict__

        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    if isinstance(obj, Orderbook):
        return json.dumps(obj.__dict__, default=inner_serializer)

    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def subscribe_orderbooks_stream(crypto_data_stream, producer, topic, usd_pairs):
    async def async_handler(data):
        await kafka_produce_message(producer, topic, data)

    crypto_data_stream.subscribe_orderbooks(async_handler, *usd_pairs)
    crypto_data_stream.run()


def main():
    producer = None
    topic = "orderbooks-topic"
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            acks=0,
            value_serializer=lambda v: json.dumps(
                v, default=custom_serializer
            ).encode(),
        )
    except Exception as e:
        print(f"KafkaProducer could not be initialized. {e}")
        return

    crypto_data_stream = CryptoDataStream(API_KEY, SECRET_KEY)
    subscribe_orderbooks_stream(crypto_data_stream, producer, topic, usd_pairs)


if __name__ == "__main__":
    main()
