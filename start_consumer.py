
from src.consumer.main import receive_data

receive_data('fut_tickers', partition=1, offset=0)
