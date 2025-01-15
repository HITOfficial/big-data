import datetime
import json
import os
from pathlib import Path

from kafka import KafkaConsumer
from pyflink.common import Row, Types
from pyflink.common.serialization import DeserializationSchema, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer


def orderbook_deserializer(message):
    if message is None:
        return Row()

    def inner_deserializer(dct):
        if "timestamp" in dct:
            dct["timestamp"] = datetime.datetime.fromisoformat(dct["timestamp"])

        if "bids" in dct:
            dct["bids"] = [Row(bid["price"], bid["size"]) for bid in dct["bids"]]

        if "asks" in dct:
            dct["asks"] = [Row(ask["price"], ask["size"]) for ask in dct["asks"]]
        dct["reset"] = bool(dct.get("reset", False))
        return dct

    try:
        # TODO: Fix on Producer double json.dumps() e.g '"{\\"symbol\\": \\"AVAX/USD\\", \\"timestamp\\": \\"2025-01-15T20:56:14.634276+00:00\\", \\"bids\\": [], \\"asks\\": [{\\"price\\": 39.3249, \\"size\\": 0.0}], \\"reset\\": false}"'
        outer_data = json.loads(message)
        inner_data = json.loads(outer_data)
        inner_data = inner_deserializer(inner_data)

        row = Row(
            inner_data.get("symbol"),
            inner_data.get("timestamp"),
            inner_data.get("bids", []),
            inner_data.get("asks", []),
            inner_data.get("reset", False),
        )

        return row
    except Exception as e:
        print(f"Error parsing message: {e}")
        return Row()


orderbook_schema = Types.ROW_NAMED(
    ["symbol", "timestamp", "bids", "asks", "reset"],
    [
        Types.STRING(),
        Types.SQL_TIMESTAMP(),
        Types.LIST(Types.ROW_NAMED(["price", "size"], [Types.FLOAT(), Types.FLOAT()])),
        Types.LIST(Types.ROW_NAMED(["price", "size"], [Types.FLOAT(), Types.FLOAT()])),
        Types.BOOLEAN(),
    ],
)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    jar_path = Path(__file__).parent / "flink-sql-connector-kafka-3.4.0-1.20.jar"
    if not jar_path.exists():
        raise FileNotFoundError(f"JAR file not found: {jar_path}")

    env.add_jars(f"file://{jar_path.resolve()}")

    topic = "orderbooks-topic"
    kafka_bootstrap_servers = "localhost:39092"

    kafka_consumer = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": kafka_bootstrap_servers,
        },
    )

    stream = env.add_source(kafka_consumer)

    # Prevent None error (Caused by: java.lang.NullPointerException: Cannot invoke "Object.toString()" because "record" is null)
    processed_stream = stream.map(
        lambda message: orderbook_deserializer(message),
        output_type=Types.ROW(
            [
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.LIST(Types.ROW([Types.FLOAT(), Types.FLOAT()])),
                Types.LIST(Types.ROW([Types.FLOAT(), Types.FLOAT()])),
                Types.BOOLEAN(),
            ]
        ),
    )

    processed_stream.print()
    env.execute("Stream message")


if __name__ == "__main__":
    main()
