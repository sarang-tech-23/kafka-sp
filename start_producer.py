
from src.producer.main import send_data

cnt = 1
while True:
    for i in range(1, 27):
        ch = chr(ord('a') + i - 1)
        ch = str(cnt) +  ch * i
        cnt += 1
        r = send_data(ch, topic='random_alphabets')
        print(f'>>ack_offset: {r}')