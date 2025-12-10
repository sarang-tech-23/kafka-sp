import os, json, bisect, struct

LOG_DIR = "./temp/log_dir/"
FILE_OFFSET_SIZE = 1000

def handler_msg_from_sub(msg, conn):
    topic = msg.get("topic")
    partition = str(msg.get("partition"))
    msg_offset = int(msg.get("offset"))
    # print(f'>> fetching_data_for_{topic}_{partition}_{msg_offset}')

    topic_path = os.path.join(LOG_DIR, topic)
    partition_path = os.path.join(topic_path, partition)

    if not os.path.exists(topic_path):
        raise FileNotFoundError("Topic does not exist")

    if not os.path.exists(partition_path):
        raise FileNotFoundError("Partition does not exist")

    log_files = [f for f in os.listdir(partition_path) if f.endswith(".log")]
    if not log_files:
        raise FileNotFoundError("No log files in partition")

    file_offsets = sorted([int(f.split(".")[0]) for f in log_files])
    idx = bisect.bisect_right(file_offsets, msg_offset) - 1
    if idx < 0:
        idx = 0

    target_file = log_files[0] # single file for each partion
    target_path = os.path.join(partition_path, target_file)

    # read the first 4 bytes from the msg_offset in this file and then 
    # unpack this msg_block = struct.pack(f"!I{data_len}s", data_len, data)
    with open(target_path, "rb") as f:
        f.seek(msg_offset)
        data_len_bytes = f.read(4)
        if not data_len_bytes:
            raise EOFError("Reached end of file, no message at this offset")

        data_len = struct.unpack("!I", data_len_bytes)[0]
        # print(f'consumer_data_len: {data_len}')
        data_bytes = f.read(data_len)

        next_offset = f.tell()
        print(f'>>cons_sending_{next_offset}_{data_bytes}')
        header = struct.pack("!QI", next_offset, len(data_bytes))
        conn.sendall(header + data_bytes)

