# Poducer
# makes a tcp connection to kafka broker and sends data
# data should contain -> Topic, Partion, and actual data

import socket
from ..utils.encoders import encode_message_producer_to_broker

def send_data(
        host="0.0.0.0", 
        port=8001, 
        topic='default', 
        partition=1, 
        data: str = 'Welcome to kafka-sp'
    ):
    """
    host: broker host
    port: broker port
    """
    # AF_INET -> IPv4, SOCK_STREAM -> TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    msg_bytes = encode_message_producer_to_broker(msg_type=1, topic=topic, partition=partition, data=data.encode('utf-8'))
    sock.send(msg_bytes)
    print(f'message_sent: {msg_bytes}')
    print(sock.recv(1024).decode())
