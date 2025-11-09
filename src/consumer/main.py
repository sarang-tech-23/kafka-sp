# Consumer
# makes a tcp connection to kafka broker and sends data
# data should contain -> Topic, Partion, offset

import socket
from ..utils.encoders import encode_consumer_to_broker_message

def receive_data(
        host="0.0.0.0", 
        port=8001, 
        topic='default', 
        partition=1, 
    ):
    """
    host: broker host
    port: broker port
    """
    # AF_INET -> IPv4, SOCK_STREAM -> TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    msg_bytes = encode_consumer_to_broker_message(msg_type=2, topic=topic, partition=partition, offset=0)
    sock.send(msg_bytes)
    print(f'message_sent: {msg_bytes}')
    print(sock.recv(1024).decode())
