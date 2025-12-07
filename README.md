# Kafka Internal Functioning Demo

## Project Overview
This project is a **poc demonstration** attempting to replicate the fundamental internal messaging functionality of Kafka using Python's inbuilt `socket` and `threading` libraries.

The goal is to showcase the core architectural pattern involving a **Broker** (server), a **Producer** (writer), and a **Consumer** (reader).

---

## Getting Started

Follow below steps to run on your local machine.

### Prerequisites

* Required **Python 3.11.11**.
    ```bash
    python3 --version
    ```

### 1. Start the Broker

The broker acts as the central message hub and must be running first.

```bash
# Terminal 1
python3 start_broker.py

# Terminal 2
python3 start_producer.py

# Terminal 3
python3 start_consumer.py
```