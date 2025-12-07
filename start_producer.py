
from src.producer.main import send_data

for i in range(1, 27):
    ch = chr(ord('a') + i - 1)
    ch = ch * i
    r = send_data(ch, topic='fut_tickers')
    print(f'>>ack_offset: {r}')