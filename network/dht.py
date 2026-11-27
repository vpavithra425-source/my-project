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

            # PING message
            if message["type"] == "PING":

                response = {
                    "type": "PONG",
                    "request_id": message.get("request_id")
                }

                self.transport.sendto(
                    json.dumps(response).encode(), address
                )

                print("PING received from:", address)

            # PONG message
            elif message["type"] == "PONG":

                request_id = message.get("request_id")

                print("PONG received! Request:", request_id)

                self.node.handle_pong(request_id)

            # JOIN message
            elif message["type"] == "JOIN":

                peer = message["peer"]

                self.node.add_peer(
                    peer["host"], peer["port"]
                )

                print("New peer joined:", address)

                peers = [
                    {"host": self.node.host,
                     "port": self.node.port}
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
                    json.dumps(response).encode(), address
                )

            # PEERS message
            elif message["type"] == "PEERS":

                print("Peer list received!")

                for peer in message["peers"]:

                    self.node.add_peer(
                        peer["host"], peer["port"]
                    )

            # FIND_NODE message
            elif message["type"] == "FIND_NODE":

                target_id = message["target_id"]
                request_id = message.get("request_id")

                print("\nFIND_NODE request received!")
                print("Target ID:", target_id)

                closest = self.node.get_closest_peers(
                    target_id, count=self.node.K
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
                    json.dumps(response).encode(), address
                )

                print("Closest peers sent!")

            # NODES message
            elif message["type"] == "NODES":

                print("\nFIND_NODE response received!")

                request_id = message.get("request_id")

                request = self.node.pending_requests.pop(
                    request_id, None
                )

                if request is None:

                    print("Unknown or expired request ID!")
                    return

                print("Request matched successfully!")

                target_id = request["target_id"]
                peer_id = request["peer_id"]

                state = self.node.lookup_states.get(target_id)

                if state is None:
                    return

                state["in_flight"].discard(peer_id)

                for peer in message.get("peers", []):

                    self.node.add_peer(
                        peer["host"], peer["port"]
                    )

                print(
                    "Discovered peers:",
                    self.node.get_peers()
                )

                self.node.continue_lookup(target_id)

        except Exception as error:

            print("DHT network error:", error)


class DHTNode:

    BITS = 160
    K = 3

    def __init__(self, host, port):

        self.host = host
        self.port = port

        self.routing_table = {}

        self.k_buckets = [
            [] for _ in range(self.BITS)
        ]

        self.transport = None

        self.pending_requests = {}
        self.pending_pings = {}

        self.lookup_states = {}

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

        print(
            "DHT node started:",
            (self.host, self.port)
        )

    def get_bucket_index(self, peer_id):

        distance = (
            int(self.node_id, 16) ^
            int(peer_id, 16)
        )

        if distance == 0:
            return None

        return distance.bit_length() - 1

    # Handle PONG response

    def handle_pong(self, request_id):

        future = self.pending_pings.pop(
            request_id, None
        )

        if future is not None and not future.done():

            future.set_result(True)

            print("Peer is alive!")

    # Ping peer and check timeout

    async def ping_peer(self, host, port, timeout=2):

        request_id = str(uuid.uuid4())

        loop = asyncio.get_running_loop()

        future = loop.create_future()

        self.pending_pings[request_id] = future

        message = {
            "type": "PING",
            "request_id": request_id
        }

        self.transport.sendto(
            json.dumps(message).encode(),
            (host, port)
        )

        try:

            await asyncio.wait_for(
                future,
                timeout=timeout
            )

            return True

        except asyncio.TimeoutError:

            print(
                "Peer is not responding:",
                (host, port)
            )

            return False

        finally:

            self.pending_pings.pop(
                request_id, None
            )

    # K-Bucket replacement method

    def replace_peer(
        self, bucket_index, new_peer_id, host, port
    ):

        bucket = self.k_buckets[bucket_index]

        if not bucket:
            return

        old_peer_id, old_address = bucket.pop(0)

        self.routing_table.pop(
            old_peer_id, None
        )

        bucket.append(
            (new_peer_id, (host, port))
        )

        self.routing_table[new_peer_id] = (
            host, port
        )

        print("Old peer replaced:", old_address)

        print(
            "New peer added:",
            (host, port)
        )

    # Add peer to routing table

    def add_peer(self, host, port):

        peer_id = hashlib.sha1(
            f"{host}:{port}".encode()
        ).hexdigest()

        if peer_id == self.node_id:
            return

        bucket_index = self.get_bucket_index(
            peer_id
        )

        if bucket_index is None:
            return

        bucket = self.k_buckets[bucket_index]

        for peer in bucket:

            if peer[0] == peer_id:

                bucket.remove(peer)

                bucket.append(
                    (peer_id, (host, port))
                )

                self.routing_table[peer_id] = (
                    host, port
                )

                return

        if len(bucket) < self.K:

            bucket.append(
                (peer_id, (host, port))
            )

            self.routing_table[peer_id] = (
                host, port
            )

            print(
                "Peer added to bucket:",
                bucket_index
            )

            print(
                "Peer discovered:",
                (host, port)
            )

        else:

            print(
                "K-Bucket is full:",
                bucket_index
            )

    def get_peers(self):

        return [
            address
            for bucket in self.k_buckets
            for _, address in bucket
        ]

    def get_closest_peers(
        self, target_id, count=3
    ):

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

        print(
            "\n========== K-BUCKET DETAILS =========="
        )

        for index, bucket in enumerate(
            self.k_buckets
        ):

            if bucket:

                print(
                    "Bucket", index, ":",
                    [address for _, address in bucket]
                )

    def join_network(
        self, bootstrap_host, bootstrap_port
    ):

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

    # Iterative FIND_NODE

    def find_node(self, host, port, target_id):

        peer_id = hashlib.sha1(
            f"{host}:{port}".encode()
        ).hexdigest()

        self.lookup_states[target_id] = {

            "shortlist": {
                peer_id: (host, port)
            },

            "queried": set(),

            "in_flight": set()
        }

        print(
            "\nStarting iterative FIND_NODE lookup!"
        )

        print("Target ID:", target_id)

        self.continue_lookup(target_id)

    def continue_lookup(self, target_id):

        state = self.lookup_states.get(
            target_id
        )

        if state is None:
            return

        shortlist = state["shortlist"]

        for bucket in self.k_buckets:

            for peer_id, address in bucket:

                shortlist[peer_id] = address

        ordered = sorted(
            shortlist.items(),
            key=lambda peer:
            int(peer[0], 16) ^ int(target_id, 16)
        )

        candidates = [

            (peer_id, address)

            for peer_id, address in ordered

            if peer_id not in state["queried"]

            and peer_id not in state["in_flight"]

        ][:self.K]

        if not candidates:

            if not state["in_flight"]:

                print(
                    "\nIterative lookup completed!"
                )

                closest = [
                    address
                    for _, address in ordered[:self.K]
                ]

                print(
                    "Closest peers found:",
                    closest
                )

                self.lookup_states.pop(
                    target_id, None
                )

            return

        for peer_id, (host, port) in candidates:

            request_id = str(uuid.uuid4())

            message = {
                "type": "FIND_NODE",
                "request_id": request_id,
                "target_id": target_id
            }

            self.pending_requests[request_id] = {

                "target_id": target_id,

                "peer_id": peer_id
            }

            state["queried"].add(peer_id)

            state["in_flight"].add(peer_id)

            self.transport.sendto(
                json.dumps(message).encode(),
                (host, port)
            )

            print(
                "FIND_NODE request sent to:",
                (host, port)
            )


async def main():

    peer1 = DHTNode("127.0.0.1", 5000)

    peer2 = DHTNode("127.0.0.1", 5001)

    peer3 = DHTNode("127.0.0.1", 5002)

    try:

        await peer1.start()

        await peer2.start()

        await peer3.start()

        print("\nAll three peers started!")

        # Peer 2 joins through Peer 1

        peer2.join_network(
            "127.0.0.1", 5000
        )

        await asyncio.sleep(1)

        # Peer 3 joins through Peer 1

        peer3.join_network(
            "127.0.0.1", 5000
        )

        await asyncio.sleep(2)

        print(
            "\n========== PEER DETAILS =========="
        )

        print("Peer 1:", peer1.get_peers())

        print("Peer 2:", peer2.get_peers())

        print("Peer 3:", peer3.get_peers())

        # Test PING/PONG

        print(
            "\n========== PING PONG TEST =========="
        )

        result = await peer2.ping_peer(
            "127.0.0.1", 5000
        )

        print("Peer 1 alive status:", result)

        # Iterative FIND_NODE test

        print(
            "\n========== ITERATIVE FIND_NODE TEST =========="
        )

        peer2.find_node(
            "127.0.0.1",
            5000,
            peer3.node_id
        )

        await asyncio.sleep(3)

        print(
            "\nPeer 2 updated routing table:"
        )

        print(peer2.get_peers())

        print("\nPending requests:")

        print(peer2.pending_requests)

        print("\nPending PING requests:")

        print(peer2.pending_pings)

        print("\nRemaining lookup states:")

        print(peer2.lookup_states)

        print(
            "\nIterative FIND_NODE test completed!"
        )

        peer1.show_k_buckets()

        peer2.show_k_buckets()

        peer3.show_k_buckets()

    finally:

        for peer in (peer1, peer2, peer3):

            if peer.transport:

                peer.transport.close()


if __name__ == "__main__":

    asyncio.run(main())