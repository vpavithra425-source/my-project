# MeshWeaver – Zero-Dependency P2P Async Task Broker

## Project Overview

MeshWeaver is a Python-based Peer-to-Peer (P2P) asynchronous task broker. It allows multiple peers to communicate, discover each other, exchange tasks, execute tasks remotely, and monitor system resources.

The project uses UDP networking, asynchronous programming, a Distributed Hash Table (DHT), and a Gossip Protocol to support decentralized peer communication and task distribution.

## Key Features

1. **Task Queue:** Manages tasks using an asynchronous queue.
2. **Peer-to-Peer Networking:** Enables communication between multiple peers using UDP.
3. **PING/PONG Communication:** Checks peer connectivity and availability.
4. **Remote Task Execution:** Executes tasks on remote peers.
5. **DHT Peer Discovery:** Helps peers discover other peers in the network.
6. **Kademlia K-Buckets:** Maintains peer routing information.
7. **Iterative FIND_NODE:** Searches for peers in the DHT network.
8. **Failed Peer Replacement:** Detects inactive peers and replaces them.
9. **Gossip Protocol:** Shares information between peers.
10. **Resource Monitoring:** Monitors CPU and RAM usage.

## Technologies Used

- Python 3.12
- Asyncio
- UDP Networking
- Cloudpickle
- Psutil
- Distributed Hash Table (DHT)
- Kademlia Concepts
- Gossip Protocol

## Project Structure

```text
MeshWeaver/
│
├── broker/
│   ├── __init__.py
│   └── Queue.py
│
├── network/
│   ├── __init__.py
│   ├── Dht.py
│   ├── Dht_backup.py
│   ├── Gossip.py
│   ├── Peer.py
│   ├── Test_peer.py
│   └── monitoring/
│       └── Resources.py
│
├── Main.py
├── Test_queue_error.py
├── Test_three_peer_integration.py
└── README.md
```

## Requirements

- Python 3.12
- cloudpickle
- psutil

## Installation

Clone the repository:

```bash
git clone https://github.com/vpavithra425-source/my-project.git
```

Navigate to the project directory:

```bash
cd my-project/meshweaver
```

Install the required packages:

```bash
pip install cloudpickle psutil
```

## Running the Project

Run the main integration program:

```bash
python Main.py
```

## Testing

Run the three-peer integration test:

```bash
python Test_three_peer_integration.py
```

Run the queue error-handling test:

```bash
python Test_queue_error.py
```

## Expected Results

- Successful peer communication.
- PING/PONG responses.
- Successful DHT peer discovery.
- Iterative FIND_NODE completion.
- Successful remote task execution.
- CPU and RAM usage information.
- Gossip message exchange.
- Queue error handling.

## Project Objective

The main objective of MeshWeaver is to develop a decentralized asynchronous task broker that supports peer communication, task distribution, peer discovery, remote task execution, and resource monitoring.

## Conclusion

MeshWeaver demonstrates how asynchronous programming, Peer-to-Peer networking, Distributed Hash Tables, and Gossip Protocols can be combined to build a decentralized task execution system.

## Repository

GitHub: https://github.com/vpavithra425-source/my-project

Branch: V-Pavithra

## Author

Pavithra
