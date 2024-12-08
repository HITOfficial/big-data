import os

from alpaca.data.live import CryptoDataStream
from config import API_KEY, SECRET_KEY
from handler import orderbooks_handler

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


def main():
    crypto_data_stream = CryptoDataStream(API_KEY, SECRET_KEY)
    crypto_data_stream.subscribe_orderbooks(orderbooks_handler, *usd_pairs)
    crypto_data_stream.run()


if __name__ == "__main__":
    main()
