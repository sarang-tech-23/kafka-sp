from src.utils.decoders import decode_producer_body, decode_packet_metadata, decode_consumer_body
from .recv_bytes import recv_bytes_of_length
from .handle_producer import handler_msg_from_pub
from .handle_consumer import handler_msg_from_sub


def msg_processor(conn, addr):
    """unpack first 9 byte to check whether it is producer or consumer message"""
    meta_data_bytes = recv_bytes_of_length(conn, 9)
    total_len, msg_type, crc  = decode_packet_metadata(meta_data_bytes)
    # print(f'meta_data_bytes_msg_process: {meta_data_bytes}.  total_len: {total_len}')

    body_bytes = recv_bytes_of_length(conn, total_len-9)
    print(f'recv_rest_msg: {body_bytes}')

    if msg_type == 1: # producer message
        msg = decode_producer_body(crc, body_bytes)
        print(f'recd_producer_msg: {msg}')
        handler_msg_from_pub(msg)
    else: # consumer message
        msg = decode_consumer_body(crc, body_bytes)
        print(f'recd_consumer_msg: {msg}')
        handler_msg_from_sub(msg)

    conn.close()


