# client.py
import socket
import json

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 9092))

sock.send(json.dumps({"type": "produce", "data": "hi from producer"}).encode())
print(sock.recv(1024).decode())

sock.send(json.dumps({"type": "fetch"}).encode())
print(sock.recv(1024).decode())
