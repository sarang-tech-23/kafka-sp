import os, json
import struct
import threading
from queue import Queue

LOG_DIR = "./temp/log_dir/"
FILE_OFFSET_SIZE = 1000


partions_state = {} # keys: (topic, partition), values: { q, t_instance }

def log_writer_fn(topic, partition, partition_q):
    while True:
        msg = partition_q.get() 
        topic_path = os.path.join(LOG_DIR, topic)
        partition_path = os.path.join(topic_path, partition)

        if not os.path.exists(topic_path):
            os.makedirs(topic_path, exist_ok=True)

        if not os.path.exists(partition_path):
            os.makedirs(partition_path, exist_ok=True)

        data = msg.get("data").encode() if isinstance(msg.get("data"), str) else msg.get("data")
        data_len = len(data)
        msg_block = struct.pack(f"!I{data_len}s", data_len, data)

        current_file = os.path.join(partition_path, "00000000.log")

        with open(current_file, "ab") as f:
            f.write(msg_block)

        return True


def get_partition_q(topic, partion):
    key = (topic, partion)
    if partions_state.get(key) is not None:
        return partions_state[key]
    
    q = Queue()
    t = threading.Thread(target=log_writer_fn, args=(topic, partion, q), daemon=True).start()

    partions_state[key] = {"queue": q, "thread": t}
    return partions_state[key]


def handler_msg_from_pub(msg):
    topic = msg.get("topic")
    partition = str(msg.get("partition"))
    # we need to push data to a particular partition queue (internal)
    # and there should be a thread running for each partition
    # 
    

    partion_q = get_partition_q(topic, partition)
    partion_q.push(msg)

    # check whether the queue/thread exists for (partition, topic)
    # if not
    # create queue then thread, msg, will be pushed to queue 
    # thread process will read from that queue, and write the message to log


  