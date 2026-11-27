import asyncio
import hashlib
import json
import uuid


class DHTProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):
        self.node = node

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        try:
            message = json.loads(data.decode())

            if message["type"] == "JOIN":

                peer = message["peer"]

                self.node.add_peer(
                    peer["host"],
                    peer["port"]
                )

                print("New peer joined:", address)

                peers = [
                    {
                        "host": self.node.host,
                        "port": self.node.port
                    }
                ]

                for host, port in self.node.get_peers():
                    peers.append({
                        "host": host,
                        "port": port
                    })

                response = {
                    "type": "PEERS",
                    "peers": peers
                }

                self.transport.sendto(
                    json.dumps(response).encode(),
                    address
                )

            elif message["type"] == "PEERS":

                print("Peer list received!")

                for peer in message["peers"]:
                    self.node.add_peer(
                        peer["host"],
                        peer["port"]
                    )

            elif message["type"] == "FIND_NODE":

                target_id = message["target_id"]
                request_id = message.get("request_id")

                print("\nFIND_NODE request received!")
                print("Target ID:", target_id)

                closest = self.node.get_closest_peers(
                    target_id,
                    count=3
                )

                peer_list = [
                    {"host": host, "port": port}
                    for host, port in closest
                ]

                response = {
                    "type": "NODES",
                    "request_id": request_id,
                    "target_id": target_id,
                    "peers": peer_list
                }

                self.transport.sendto(
                    json.dumps(response).encode(),
                    address
                )

                print("Closest peers sent!")

            elif message["type"] == "NODES":

                print("\nFIND_NODE response received!")

                request_id = message.get("request_id")

                if request_id:
                    request = self.node.pending_requests.pop(
                        request_id, None
                    )

                    if request is not None:
                        print("Request matched successfully!")
                    else:
                        print("Unknown or expired request ID!")

                for peer in message["peers"]:
                    self.node.add_peer(
                        peer["host"],
                        peer["port"]
                    )

                print(
                    "Discovered closest peers:",
                    self.node.get_peers()
                )

        except Exception as error:
            print("DHT network error:", error)


class DHTNode:

    BITS = 160
    K = 3

    def __init__(self, host, port):

        self.host = host
        self.port = port

        self.routing_table = {}
        self.k_buckets = [[] for _ in range(self.BITS)]

        self.transport = None
        self.pending_requests = {}

        self.node_id = hashlib.sha1(
            f"{host}:{port}".encode()
        ).hexdigest()

        print("DHT Node created!")
        print("Node ID:", self.node_id)

    async def start(self):

        loop = asyncio.get_running_loop()

        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: DHTProtocol(self),
            local_addr=(self.host, self.port)
        )

        print("DHT node started:", (self.host, self.port))

    def get_bucket_index(self, peer_id):

        distance = (
            int(self.node_id, 16) ^ int(peer_id, 16)
        )

        if distance == 0:
            return None

        return distance.bit_length() - 1

    def add_peer(self, host, port):

        peer_id = hashlib.sha1(
            f"{host}:{port}".encode()
        ).hexdigest()

        if peer_id == self.node_id:
            return

        bucket_index = self.get_bucket_index(peer_id)

        if bucket_index is None:
            return

        bucket = self.k_buckets[bucket_index]

        # Check whether peer already exists
        for peer in bucket:

            if peer[0] == peer_id:

                bucket.remove(peer)
                bucket.append((peer_id, (host, port)))

                self.routing_table[peer_id] = (host, port)
                return

        # Add peer if bucket has available space
        if len(bucket) < self.K:

            bucket.append((peer_id, (host, port)))
            self.routing_table[peer_id] = (host, port)

            print("Peer added to bucket:", bucket_index)
            print("Peer discovered:", (host, port))

        else:

            print("K-Bucket is full:", bucket_index)

    def get_peers(self):

        return [
            address
            for bucket in self.k_buckets
            for _, address in bucket
        ]

    def get_closest_peers(self, target_id, count=3):

        peers = [
            (peer_id, address)
            for bucket in self.k_buckets
            for peer_id, address in bucket
        ]

        peers.sort(
            key=lambda peer:
            int(peer[0], 16) ^ int(target_id, 16)
        )

        return [
            address
            for _, address in peers[:count]
        ]

    def show_k_buckets(self):

        print("\n========== K-BUCKET DETAILS ==========")

        for index, bucket in enumerate(self.k_buckets):

            if bucket:

                print(
                    "Bucket",
                    index,
                    ":",
                    [address for _, address in bucket]
                )

    def join_network(self, bootstrap_host, bootstrap_port):

        message = {
            "type": "JOIN",
            "peer": {
                "host": self.host,
                "port": self.port
            }
        }

        self.transport.sendto(
            json.dumps(message).encode(),
            (bootstrap_host, bootstrap_port)
        )

        print("Joining DHT network...")

    def find_node(self, host, port, target_id):

        request_id = str(uuid.uuid4())

        message = {
            "type": "FIND_NODE",
            "request_id": request_id,
            "target_id": target_id
        }

        self.pending_requests[request_id] = {
            "host": host,
            "port": port,
            "target_id": target_id
        }

        self.transport.sendto(
            json.dumps(message).encode(),
            (host, port)
        )

        print("\nFIND_NODE request sent!")
        print("Request ID:", request_id)


async def main():

    peer1 = DHTNode("127.0.0.1", 5000)
    peer2 = DHTNode("127.0.0.1", 5001)
    peer3 = DHTNode("127.0.0.1", 5002)

    try:

        # Start all three peers
        await peer1.start()
        await peer2.start()
        await peer3.start()

        print("\nAll three peers started!")

        # Peer 2 joins through Peer 1
        peer2.join_network("127.0.0.1", 5000)

        await asyncio.sleep(1)

        # Peer 3 joins through Peer 1
        peer3.join_network("127.0.0.1", 5000)

        await asyncio.sleep(2)

        print("\n========== PEER DETAILS ==========")

        print("Peer 1:", peer1.get_peers())
        print("Peer 2:", peer2.get_peers())
        print("Peer 3:", peer3.get_peers())

        # FIND_NODE test
        print("\n========== FIND_NODE TEST ==========")

        peer2.find_node(
            "127.0.0.1",
            5000,
            peer3.node_id
        )

        await asyncio.sleep(2)

        print("\nPeer 2 updated routing table:")
        print(peer2.get_peers())

        print("\nPending requests:", peer2.pending_requests)

        print("\nFIND_NODE test completed!")

        # Display K-Bucket details
        peer1.show_k_buckets()
        peer2.show_k_buckets()

    finally:

        for peer in (peer1, peer2, peer3):

            if peer.transport:
                peer.transport.close()


if __name__ == "__main__":
    asyncio.run(main())