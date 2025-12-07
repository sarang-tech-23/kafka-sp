import struct

def recv_bytes_of_length(conn, length):
    """keep reading until length of bytes have been received from conn"""
    chunks = []
    bytes_recd = 0
    while bytes_recd < length:
        chunk = conn.recv(min(4096, length - bytes_recd))
        if not chunk:
            raise ConnectionError("Socket closed before receiving full data")
        chunks.append(chunk)
        bytes_recd += len(chunk)
        # print(f'length: {length}  bytes_recvd: {bytes_recd}')
    return b''.join(chunks)
