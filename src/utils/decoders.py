import struct, zlib

def decode_packet_metadata(raw: bytes):
    return struct.unpack_from("!I B I", raw, 0)

def decode_consumer_body(crc, raw: bytes):
    if zlib.crc32(raw) & 0xffffffff != crc:
        raise ValueError("CRC mismatch")
    
    offset = 0
    topic_len = struct.unpack_from("!H", raw, offset)[0]
    offset += 2

    topic = struct.unpack_from(f"!{topic_len}s", raw, offset)[0].decode()
    offset += topic_len

    partition = struct.unpack_from("!H", raw, offset)[0]
    offset += 2

    msg_offset = struct.unpack_from("!I", raw, offset)[0]

    return {"topic": topic, "partition": partition, "offset": msg_offset}



def decode_producer_body(crc, raw: bytes):
    if zlib.crc32(raw) & 0xffffffff != crc:
        raise ValueError("CRC mismatch")

    offset = 0
    topic_len = struct.unpack_from("!H", raw, offset)[0]
    offset += 2

    topic = struct.unpack_from(f"!{topic_len}s", raw, offset)[0].decode()
    offset += topic_len

    partition = struct.unpack_from("!H", raw, offset)[0]
    offset += 2

    data_len = struct.unpack_from("!I", raw, offset)[0]
    offset += 4

    data = struct.unpack_from(f"!{data_len}s", raw, offset)[0]

    return {"topic": topic, "partition": partition, "data": data}
