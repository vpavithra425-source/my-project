MeshWeaver – Zero-Dependency P2P Async Task Broker

Project Overview

MeshWeaver is a Python-based Peer-to-Peer (P2P) asynchronous task broker. It allows multiple peers to communicate, discover each other, exchange tasks, and monitor system resources.

The project uses UDP networking, asynchronous programming, a Distributed Hash Table (DHT), and a Gossip Protocol.

Features

- Task Queue: Manages tasks using an asynchronous queue.
- Peer-to-Peer Networking: Enables communication between multiple peers using UDP.
- PING/PONG: Checks peer connectivity.
- Remote Task Execution: Executes tasks on remote peers.
- DHT Peer Discovery: Helps peers discover other peers.
- Kademlia K-Buckets: Maintains peer routing information.
- Iterative FIND_NODE: Searches for peers in the DHT network.
- Failed Peer Replacement: Detects inactive peers and replaces them.
- Gossip Protocol: Shares information between peers.
- Resource Monitoring: Monitors CPU and RAM usage.

Technologies Used

- Python 3.12
- Asyncio
- UDP Networking
- Cloudpickle
- Psutil
- Distributed Hash Table (DHT)
- Kademlia concepts
- Gossip Protocol

Requirements

- Python 3.12
- cloudpickle
- psutil

Installation

Install the required packages:

pip install cloudpickle psutil

## Running the Project

Run the main integration program:

```bash
python Main.py

Testing

Run the three-peer integration test:

python test_three_peer_integration.py

Run the queue error-handling test:

python test_queue_error.py

Run the three-peer DHT test:

python network/test_dht_3peers.py

Expected Results

- Successful peer communication.
- PING/PONG responses.
- Successful DHT peer discovery.
- Iterative FIND_NODE completion.
- Successful remote task execution.
- CPU and RAM usage information.
- Gossip message exchange.
- Queue error handling.

Project Objective

The main objective of MeshWeaver is to develop a decentralized asynchronous task broker that supports peer communication, task distribution, peer discovery, and resource monitoring.

Conclusion

MeshWeaver demonstrates how asynchronous programming and Peer-to-Peer networking can be combined to build a distributed task execution system.
