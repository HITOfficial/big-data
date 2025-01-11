import json
import os

from alpaca.data.live import CryptoDataStream
from config import API_KEY, KAFKA_BOOTSTRAP_SERVER, SECRET_KEY
from handler import kafka_produce_message
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

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
            security_protocol="SSL",
            api_version=(
                0,
                11,
                5,
            ),  # TODO: update api_version, added OOD https://stackoverflow.com/questions/38854957/nobrokersavailable-nobrokersavailable-kafka-error
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        print(f"KafkaProducer could not be initialized. {e}")
        return

    crypto_data_stream = CryptoDataStream(API_KEY, SECRET_KEY)
    subscribe_orderbooks_stream(crypto_data_stream, producer, topic, usd_pairs)


if __name__ == "__main__":
    main()
