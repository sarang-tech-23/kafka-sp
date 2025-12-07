import struct, zlib

def encode_consumer_to_broker_message(
        msg_type: int,
        topic: str,
        partition: int,
        offset: int
):
    topic_bytes = topic.encode()
    topic_len = len(topic_bytes)
    body = struct.pack(
        f"!H{topic_len}sHI",
        topic_len, topic_bytes, partition, offset
    )

    crc = zlib.crc32(body) & 0xffffffff
    total_len = len(body) + 9

    return struct.pack("!I B I", total_len, msg_type, crc) + body
    

def encode_message_producer_to_broker(msg_type: int, topic: str, partition: int, data: bytes) -> bytes:
    topic_bytes = topic.encode()
    topic_len = len(topic_bytes)
    data_len = len(data)

    header = struct.pack(
        f"!H {topic_len}s H I",
        topic_len, topic_bytes, partition, data_len
    )
    body = header + data
    crc = zlib.crc32(body) & 0xffffffff
    total_len = len(body) + 4 + 4 +1  # 4 bytes total_len + 4 bytes crc + 1 byte msg_type

    # final message
    # print(f'pack_len: {len(struct.pack("!I B I", total_len, msg_type, crc))}. body_len: {len(body)}')
    # for pub/sub connection the data packets first 9 bytes should be reserved for this 3
    # attributes, total_len(the lenght of whole packet), msg_type(1/2), crc
    return struct.pack("!I B I", total_len, msg_type, crc) + body
