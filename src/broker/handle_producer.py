import os, json
import struct

LOG_DIR = "./temp/log_dir/"
FILE_OFFSET_SIZE = 1000

def handler_msg_from_pub(msg):
    topic = msg.get("topic")
    partition = str(msg.get("partition"))
    data = msg.get("data").encode() if isinstance(msg.get("data"), str) else msg.get("data")

    topic_path = os.path.join(LOG_DIR, topic)
    partition_path = os.path.join(topic_path, partition)

    if not os.path.exists(topic_path):
        os.makedirs(topic_path, exist_ok=True)

    if not os.path.exists(partition_path):
        os.makedirs(partition_path, exist_ok=True)

    data_len = len(data)
    msg_block = struct.pack(f"!I{data_len}s", data_len, data)

    log_files = [f for f in os.listdir(partition_path) if f.endswith(".log")]
    if not log_files:
        current_file = os.path.join(partition_path, "00000000.log")
        offset = 0
    else:
        log_files.sort()
        current_file = os.path.join(partition_path, log_files[-1])
        offset = os.path.getsize(current_file)

    if offset >= FILE_OFFSET_SIZE:
        base_offset = int(log_files[-1].split(".")[0]) + FILE_OFFSET_SIZE if log_files else 0
        current_file = os.path.join(partition_path, f"{base_offset:08d}.log")
        offset = 0

    with open(current_file, "ab") as f:
        f.write(msg_block)

    return current_file
