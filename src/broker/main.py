# broker listens for connect from producer and conumser and sends the data as required
import socket
import threading
import json
from .recv_bytes import recv_bytes_of_length
from .msg_processor import msg_processor
import struct

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
            print(f'conn_handshake_succes: {conn}')
            threading.Thread(target=msg_processor, args=(conn, addr)).start()

    def handle_conn(self, conn, addr):
        print(f"Connected: {addr}")
        # receive all the bytes in tcp stream

        len_bytes = recv_bytes_of_length(conn, 4)
        (total_len,) = struct.unpack("!I", len_bytes)

        raw_msg = recv_bytes_of_length(conn, total_len)
        msg_processor(raw_msg)

        while data := conn.recv(1024):
            msg = json.loads(data.decode())
            if msg["type"] == "produce":
                '''
                - check the topic and partition it is for.
                - store it in os page cache (later flushed to disk) 
                - give and offset no to the message
                - send True conn object
                '''
                print(f"Received message: {msg['data']}")
                conn.send(b'{"status": "ok"}')
            elif msg["type"] == "consume":
                '''
                - check the topic and partition
                - check the offset no
                - send data packet back to conn object
                '''
                conn.send(b'{"messages": ["hello", "world"]}')
        conn.close()

    # creates a socket connection object, AF_INET -> IPv4, SOCK_STREAM -> TCP connection
    
