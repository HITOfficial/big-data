import datetime
import json
import os
from pathlib import Path

from kafka import KafkaConsumer
from pyflink.common import Row, Types
from pyflink.common.serialization import DeserializationSchema, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream import TimeCharacteristic, Time, WindowAssigner, TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.typeinfo import Types
import numpy as np

class ComputeRSI(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        prices = [element.price for element in elements]
        gains = [prices[i] - prices[i - 1] for i in range(1, len(prices)) if prices[i] > prices[i - 1]]
        losses = [prices[i - 1] - prices[i] for i in range(1, len(prices)) if prices[i] < prices[i - 1]]
        avg_gain = np.mean(gains) if gains else 0
        avg_loss = np.mean(losses) if losses else 0
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs))
        out.collect((key, rsi))

class ComputeMedianPrice(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        prices = [element.price for element in elements]
        median_price = np.median(prices)
        out.collect((key, median_price))

class ComputeMaxGainLoss(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        prices = [element.price for element in elements]
        if len(prices) < 2:
            return

        gains = [(prices[i] - prices[i - 1], elements[i].symbol) for i in range(1, len(prices))]
        losses = [(prices[i - 1] - prices[i], elements[i].symbol) for i in range(1, len(prices))]

        max_gain = max(gains, key=lambda x: x[0], default=(0, None))
        max_loss = max(losses, key=lambda x: x[0], default=(0, None))

        out.collect((key, max_gain[1], max_gain[0], max_loss[1], max_loss[0]))

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
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

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

    windowed_stream = processed_stream \
        .key_by(lambda row: row.symbol) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))

    rsi_stream = windowed_stream.apply(ComputeRSI(), Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    median_price_stream = windowed_stream.apply(ComputeMedianPrice(), Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    max_gain_loss_stream = windowed_stream.apply(ComputeMaxGainLoss(), Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT()]))

    rsi_stream.print()
    median_price_stream.print()
    max_gain_loss_stream.print()

    windowed_stream_5min = processed_stream \
        .key_by(lambda row: row.symbol) \
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))

    windowed_stream_10min = processed_stream \
        .key_by(lambda row: row.symbol) \
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))

    windowed_stream_30sec = processed_stream \
        .key_by(lambda row: row.symbol) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))

    # Apply computations to the new windowed streams
    rsi_stream_5min = windowed_stream_5min.apply(ComputeRSI(), Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    median_price_stream_5min = windowed_stream_5min.apply(ComputeMedianPrice(),
                                                          Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    max_gain_loss_stream_5min = windowed_stream_5min.apply(ComputeMaxGainLoss(), Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT()]))

    rsi_stream_10min = windowed_stream_10min.apply(ComputeRSI(), Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    median_price_stream_10min = windowed_stream_10min.apply(ComputeMedianPrice(),
                                                            Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    max_gain_loss_stream_10min = windowed_stream_10min.apply(ComputeMaxGainLoss(), Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT()]))

    rsi_stream_30sec = windowed_stream_30sec.apply(ComputeRSI(), Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    median_price_stream_30sec = windowed_stream_30sec.apply(ComputeMedianPrice(),
                                                            Types.TUPLE([Types.STRING(), Types.FLOAT()]))
    max_gain_loss_stream_30sec = windowed_stream_30sec.apply(ComputeMaxGainLoss(), Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT()]))

    # Print the results of the new windowed streams
    rsi_stream_5min.print()
    median_price_stream_5min.print()
    max_gain_loss_stream_5min.print()

    rsi_stream_10min.print()
    median_price_stream_10min.print()
    max_gain_loss_stream_10min.print()

    rsi_stream_30sec.print()
    median_price_stream_30sec.print()
    max_gain_loss_stream_30sec.print()

    processed_stream.print()
    env.execute("Stream message")


if __name__ == "__main__":
    main()
