import asyncio
import hashlib
import json
import uuid


# ==========================================
# DHT PROTOCOL
# ==========================================

class DHTProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):
        self.node = node

    def connection_made(self, transport):
        self.node.transport = transport
        print("DHT transport started:", self.node.address)

    def datagram_received(self, data, address):
        try:
            message = json.loads(data.decode())

            asyncio.create_task(
                self.node.handle_message(message, address)
            )

        except Exception as error:
            print("Message error:", error)


# ==========================================
# DHT NODE
# ==========================================

class DHTNode:

    BITS = 160
    K = 3

    def __init__(self, host, port):

        self.host = host
        self.port = port
        self.address = (host, port)

        self.node_id = hashlib.sha1(
            f"{host}:{port}".encode()
        ).hexdigest()

        self.transport = None

        self.routing_table = {}
        self.k_buckets = [[] for _ in range(self.BITS)]

        self.pending_requests = {}
        self.pending_pings = {}

        self.replacement_cache = {
            index: [] for index in range(self.BITS)
        }

        self.replacement_in_progress = set()

        print("\nDHT Node created!")
        print("Node ID:", self.node_id)

    # START NODE

    async def start(self):

        loop = asyncio.get_running_loop()

        await loop.create_datagram_endpoint(
            lambda: DHTProtocol(self),
            local_addr=self.address
        )

        print("Node started:", self.address)

    # CLOSE NODE

    def close(self):

        if self.transport:
            self.transport.close()
            self.transport = None

        print("Node closed:", self.address)

    # SEND MESSAGE

    def send_message(self, message, address):

        if self.transport:
            data = json.dumps(message).encode()
            self.transport.sendto(data, address)

    # GET BUCKET INDEX

    def get_bucket_index(self, peer_id):

        try:
            distance = (
                int(self.node_id, 16) ^ int(peer_id, 16)
            )

            if distance == 0:
                return None

            return distance.bit_length() - 1

        except (ValueError, TypeError):
            return None

    # ======================================
    # HANDLE INCOMING MESSAGES
    # ======================================

    async def handle_message(self, message, address):

        message_type = message.get("type")

        print("\nMessage received:", message_type)
        print("From:", address)

        # JOIN

        if message_type == "JOIN":

            print("JOIN request received!")

            peer = message.get("peer")

            if not peer or not peer.get("id"):
                print("Invalid JOIN request!")
                return

            await self.add_peer(peer)

            known_peers = self.get_closest_peers(
                peer["id"]
            )

            known_peers = [
                p for p in known_peers
                if p["id"] != peer["id"]
            ]

            known_peers.append({
                "id": self.node_id,
                "host": self.host,
                "port": self.port
            })

            self.send_message(
                {
                    "type": "PEERS",
                    "request_id": message.get("request_id"),
                    "peers": known_peers[:self.K]
                },
                address
            )

            print("PEERS response sent to:", address)

        # PEERS

        elif message_type == "PEERS":

            print("PEERS response received from:", address)

            request_id = message.get("request_id")

            future = self.pending_requests.pop(
                request_id, None
            )

            if future and not future.done():

                future.set_result(
                    message.get("peers", [])
                )

                print("JOIN response matched successfully!")

            else:
                print("No matching pending request!")

        # FIND_NODE

        elif message_type == "FIND_NODE":

            print("FIND_NODE request received!")

            target_id = message.get("target_id")

            try:
                int(target_id, 16)
            except (ValueError, TypeError):
                print("Invalid target ID!")
                return

            self.send_message(
                {
                    "type": "NODES",
                    "request_id": message.get("request_id"),
                    "nodes": self.get_closest_peers(target_id)
                },
                address
            )

            print("NODES response sent!")

        # NODES

        elif message_type == "NODES":

            print("NODES response received!")

            request_id = message.get("request_id")

            future = self.pending_requests.pop(
                request_id, None
            )

            if future and not future.done():

                future.set_result(
                    message.get("nodes", [])
                )

        # PING

        elif message_type == "PING":

            print("PING received from:", address)

            self.send_message(
                {
                    "type": "PONG",
                    "request_id": message.get("request_id"),
                    "peer": {
                        "id": self.node_id,
                        "host": self.host,
                        "port": self.port
                    }
                },
                address
            )

            print("PONG sent!")

        # PONG

        elif message_type == "PONG":

            print("\nPONG received from:", address)

            request_id = message.get("request_id")

            future = self.pending_pings.pop(
                request_id, None
            )

            if future and not future.done():
                future.set_result(True)

            peer = message.get("peer")

            if peer:
                await self.add_peer(peer)

    # ======================================
    # PING PEER
    # ======================================

    async def ping_peer(self, host, port, timeout=2):

        request_id = str(uuid.uuid4())

        future = asyncio.get_running_loop().create_future()

        self.pending_pings[request_id] = future

        self.send_message(
            {
                "type": "PING",
                "request_id": request_id
            },
            (host, port)
        )

        try:

            await asyncio.wait_for(
                future,
                timeout=timeout
            )

            return True

        except asyncio.TimeoutError:

            print("PING timeout:", host, port)
            return False

        finally:

            self.pending_pings.pop(
                request_id, None
            )

    # ======================================
    # ADD PEER
    # ======================================

    async def add_peer(self, peer):

        peer_id = peer.get("id")

        if not peer_id or peer_id == self.node_id:
            return

        bucket_index = self.get_bucket_index(peer_id)

        if bucket_index is None:
            return

        bucket = self.k_buckets[bucket_index]

        # Existing peer

        if peer_id in self.routing_table:

            existing_peer = self.routing_table[peer_id]

            if existing_peer in bucket:
                bucket.remove(existing_peer)

            bucket.append(peer)

            self.routing_table[peer_id] = peer

            return

        # Bucket has free space

        if len(bucket) < self.K:

            bucket.append(peer)

            self.routing_table[peer_id] = peer

            print("Peer added to bucket:", bucket_index)

            return

        # Avoid simultaneous replacement checks

        if bucket_index in self.replacement_in_progress:

            self.cache_replacement(bucket_index, peer)
            return

        self.replacement_in_progress.add(bucket_index)

        try:

            await self.consider_replacement(
                bucket_index,
                peer
            )

        finally:

            self.replacement_in_progress.discard(
                bucket_index
            )

    # ======================================
    # REPLACEMENT CACHE
    # ======================================

    def cache_replacement(self, bucket_index, peer):

        cache = self.replacement_cache[bucket_index]

        peer_id = peer["id"]

        if peer_id in self.routing_table:
            return

        if any(item["id"] == peer_id for item in cache):
            return

        cache.append(peer)

        if len(cache) > self.K:
            cache.pop(0)

        print(
            "Peer stored in replacement cache:",
            peer["host"],
            peer["port"]
        )

    # ======================================
    # K-BUCKET REPLACEMENT
    # ======================================

    async def consider_replacement(
        self,
        bucket_index,
        new_peer
    ):

        bucket = self.k_buckets[bucket_index]

        if len(bucket) < self.K:

            bucket.append(new_peer)

            self.routing_table[new_peer["id"]] = new_peer

            print("New peer added to bucket:", bucket_index)

            return

        # Check oldest peer

        old_peer = bucket[0]

        print(
            "\nChecking old peer:",
            old_peer["host"],
            old_peer["port"]
        )

        alive = await self.ping_peer(
            old_peer["host"],
            old_peer["port"]
        )

        if alive:

            print("Old peer is alive. Keeping it.")

            if old_peer in bucket:
                bucket.remove(old_peer)

            bucket.append(old_peer)

            self.cache_replacement(
                bucket_index,
                new_peer
            )

        else:

            print("Old peer failed. Replacing it.")

            if old_peer in bucket:
                bucket.remove(old_peer)

            self.routing_table.pop(
                old_peer["id"],
                None
            )

            candidates = self.replacement_cache[bucket_index]

            selected_peer = None

            while candidates:

                candidate = candidates.pop(0)

                if candidate["id"] not in self.routing_table:
                    selected_peer = candidate
                    break

            if selected_peer is None:
                selected_peer = new_peer

            if len(bucket) < self.K:

                bucket.append(selected_peer)

                self.routing_table[
                    selected_peer["id"]
                ] = selected_peer

                print(
                    "New peer added to bucket:",
                    bucket_index
                )

    # ======================================
    # GET CLOSEST PEERS
    # ======================================

    def get_closest_peers(self, target_id):

        try:
            target = int(target_id, 16)
        except (ValueError, TypeError):
            return []

        peers = list(self.routing_table.values())

        peers.sort(
            key=lambda peer: (
                int(peer["id"], 16) ^ target
            )
        )

        return peers[:self.K]

    # ======================================
    # GET ALL PEERS
    # ======================================

    def get_peers(self):

        return list(self.routing_table.values())

    # ======================================
    # DISPLAY K-BUCKETS
    # ======================================

    def show_k_buckets(self):

        print("\n========== K-BUCKETS ==========")

        for index, bucket in enumerate(self.k_buckets):

            if bucket:

                print(
                    "Bucket",
                    index,
                    ":",
                    [
                        peer["host"] + ":" + str(peer["port"])
                        for peer in bucket
                    ]
                )

        print("================================")

    # ======================================
    # DISPLAY REPLACEMENT CACHE
    # ======================================

    def show_replacement_cache(self):

        print("\n======= REPLACEMENT CACHE =======")

        for index, cache in self.replacement_cache.items():

            if cache:

                print(
                    "Bucket",
                    index,
                    ":",
                    [
                        peer["host"] + ":" + str(peer["port"])
                        for peer in cache
                    ]
                )

        print("=================================")

    # ======================================
    # JOIN NETWORK
    # ======================================

    async def join_network(
        self,
        bootstrap_host,
        bootstrap_port
    ):

        request_id = str(uuid.uuid4())

        future = asyncio.get_running_loop().create_future()

        self.pending_requests[request_id] = future

        self.send_message(
            {
                "type": "JOIN",
                "request_id": request_id,
                "peer": {
                    "id": self.node_id,
                    "host": self.host,
                    "port": self.port
                }
            },
            (bootstrap_host, bootstrap_port)
        )

        try:

            peers = await asyncio.wait_for(
                future,
                timeout=5
            )

            print("\nPeers received:", peers)

            for peer in peers:
                await self.add_peer(peer)

            print("Network joined successfully!")

        except asyncio.TimeoutError:

            print("JOIN request timed out.")

        finally:

            self.pending_requests.pop(
                request_id, None
            )

    # ======================================
    # ITERATIVE FIND NODE
    # ======================================

    async def find_node(self, target_id):

        print("\nStarting ITERATIVE FIND_NODE...")

        try:
            target = int(target_id, 16)
        except (ValueError, TypeError):
            print("Invalid target Node ID!")
            return []

        queried = set()

        # Initial shortlist

        peers = self.get_closest_peers(target_id)

        while peers:

            peers.sort(
                key=lambda peer: (
                    int(peer["id"], 16) ^ target
                )
            )

            peer = None

            for candidate in peers:

                if candidate["id"] not in queried:
                    peer = candidate
                    break

            if peer is None:
                break

            queried.add(peer["id"])

            print(
                "\nQuerying peer:",
                peer["host"],
                peer["port"]
            )

            request_id = str(uuid.uuid4())

            future = asyncio.get_running_loop().create_future()

            self.pending_requests[request_id] = future

            self.send_message(
                {
                    "type": "FIND_NODE",
                    "request_id": request_id,
                    "target_id": target_id
                },
                (peer["host"], peer["port"])
            )

            try:

                nodes = await asyncio.wait_for(
                    future,
                    timeout=3
                )

                print("FIND_NODE response received")

                for node in nodes:

                    if not node.get("id"):
                        continue

                    await self.add_peer(node)

                    if (
                        node["id"] not in queried
                        and all(
                            item["id"] != node["id"]
                            for item in peers
                        )
                    ):

                        peers.append(node)

                peers.sort(
                    key=lambda item: (
                        int(item["id"], 16) ^ target
                    )
                )

                peers = peers[:self.K]

            except asyncio.TimeoutError:

                print(
                    "FIND_NODE timeout:",
                    peer["host"],
                    peer["port"]
                )

            finally:

                self.pending_requests.pop(
                    request_id, None
                )

        print("\nIterative lookup completed!")

        closest = self.get_closest_peers(target_id)

        print("\nClosest known peers to target:")

        for item in closest:

            print(
                item["host"],
                item["port"],
                item["id"]
            )

        return closest


# ==========================================
# MAIN FUNCTION - FIVE PEER TEST
# ==========================================

async def main():

    peer1 = DHTNode("127.0.0.1", 5000)
    peer2 = DHTNode("127.0.0.1", 5002)
    peer3 = DHTNode("127.0.0.1", 5003)
    peer4 = DHTNode("127.0.0.1", 5004)
    peer5 = DHTNode("127.0.0.1", 5006)

    peers = [peer1, peer2, peer3, peer4, peer5]

    try:

        for peer in peers:
            await peer.start()

        print("\nAll five peers started!")

        await asyncio.sleep(1)

        await peer2.join_network("127.0.0.1", 5000)
        await peer3.join_network("127.0.0.1", 5000)
        await peer4.join_network("127.0.0.1", 5000)
        await peer5.join_network("127.0.0.1", 5000)

        await asyncio.sleep(1)

        print("\n========== PEER 2 K-BUCKETS ==========")

        peer2.show_k_buckets()

        target_id = peer5.node_id

        print("\nTarget Node ID:", target_id)

        await peer2.find_node(target_id)

        print("\n========== FINAL RESULTS ==========")

        peer2.show_k_buckets()

        print("\nPeer 2 pending requests:")
        print(peer2.pending_requests)

        print("\nPeer 2 pending pings:")
        print(peer2.pending_pings)

    finally:

        for peer in peers:

            if peer.transport is not None:
                peer.close()


if __name__ == "__main__":

    asyncio.run(main())