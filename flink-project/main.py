# setup client
import os

from alpaca.data.live import CryptoDataStream
from alpaca.trading.client import TradingClient

# load .env variables
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

api_key = os.getenv("APCA_API_KEY_ID")
secret_key = os.getenv("APCA_API_SECRET_KEY")
paper = True  # Please do not modify this. This example is for paper trading only.

client = TradingClient(
    api_key=api_key,
    secret_key=secret_key,
    paper=paper,
)

crypto_client = CryptoDataStream(api_key, secret_key)


async def bars_data_handler(data):
    print(data)


# https://alpaca.markets/support/what-cryptocurrencies-does-alpaca-currently-support
crypto_usd_pairs = [  # 19 pairs
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
    "LINK/USD",
    "LTC/USD",
    "MKR/USD",
    "SHIB/USD",
    "SUSHI/USD",
    "UNI/USD",
    "USDT/USD",
    "XTZ/USD",
    "YFI/USD",
]

for pair in crypto_usd_pairs:
    crypto_client.subscribe_bars(bars_data_handler, pair)

crypto_client.run()
