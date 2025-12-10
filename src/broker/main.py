# broker listens for connect from producer and conumser and sends the data as required
import socket
import threading
import json
from .recv_bytes import recv_bytes_of_length
from .msg_processor import msg_processor
import struct
import threading
from queue import Queue

class Broker():
    def __init__(self, host='0.0.0.0', port=8001):
        self.host = host
        self.port = port

    def start_broker(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("0.0.0.0", 8001))
        s.listen()
        print("Broker running...")

        while True:
            # .accept() blocks the loop until client tries to connect to port
            # completes the 3 way handshake and give conn obj to recv and send bytes
            # conn is tuple, which contains (broker_host, broker_port, client_host, client_port)
            # so each client connection is distiguished based on these parameters
            conn, addr = s.accept()
            t = threading.Thread(target=msg_processor, args=(conn, addr), daemon=True)
            t.start()
            # print(f'conn_handshake_succes: {conn}')
            # msg_processor(conn, addr)

        