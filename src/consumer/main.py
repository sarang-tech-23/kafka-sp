# Consumer
# makes a tcp connection to kafka broker and sends data
# data should contain -> Topic, Partion, offset

import socket
import struct
from ..utils.encoders import encode_consumer_to_broker_message

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf += chunk
    return buf


def receive_data(
        topic, 
        partition=None,
        host="0.0.0.0", 
        port=8001, 
        offset=0
    ):
    """
    host: broker host
    port: broker port
    """
    # AF_INET -> IPv4, SOCK_STREAM -> TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    msg_bytes = encode_consumer_to_broker_message(msg_type=2, topic=topic, partition=partition, offset=offset)
    print(f'>>consu_message_sent: {msg_bytes}')
    sock.send(msg_bytes)
    consumer_msg = sock.recv(1024)

    print(f'>>cons_msg_{len(consumer_msg[12:13])}: {consumer_msg}')
    # First 12 bytes = header
    header = consumer_msg[:12]
    next_offset, block_len = struct.unpack("!QI", header)
    print(f'>>next_offset_{next_offset}__block_len_{block_len}')

    # Remaining bytes = msg_block
    msg_block = consumer_msg[12:12 + block_len]
    print(f'>>first_message_block: {msg_block}')

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        msg_bytes = encode_consumer_to_broker_message(msg_type=2, topic=topic, partition=partition, offset=next_offset)
        # print(f'>>message_sent_2nd: {next_offset}__{type(next_offset)}')
        sock.send(msg_bytes)

        consumer_msg = sock.recv(1024)
        # print(f'>>len_consumer_msg: {len(consumer_msg)}')
        # First 12 bytes = header
        header = consumer_msg[:12]
        next_offset, block_len = struct.unpack("!QI", header)

        # Remaining bytes = msg_block
        msg_block = consumer_msg[12:12 + block_len].decode()
        print(f'{msg_block}')

