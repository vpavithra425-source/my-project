
import asyncio
import json
import argparse
import hashlib
import uuid
import psutil

# Kademlia configuration
K = 20
LOOKUP_TIMEOUT = 3
GOSSIP_INTERVAL = 5


class KademliaNode(asyncio.DatagramProtocol):

    def __init__(self, node_name):

        self.node_name = node_name

        # Generate 160-bit node ID
        self.node_id = hashlib.sha1(
            node_name.encode()
        ).hexdigest()

        self.node_id_int = int(self.node_id, 16)

        self.transport = None

        # Create 160 routing buckets
        self.routing_table = {
            i: [] for i in range(160)
        }

        # Pending FIND_NODE requests
        self.pending_requests = {}

        # Store received CPU/RAM information
        self.peer_resources = {}

        # Initialize CPU measurement
        psutil.cpu_percent(interval=None)

        print(f"\nNode ID: {self.node_id}")

    # ---------------------------------
    # START NODE
    # ---------------------------------

    def connection_made(self, transport):

        self.transport = transport

        address = transport.get_extra_info("sockname")

        print(f"\nMeshWeaver Node {self.node_name} is running.")
        print(f"Listening on port: {address[1]}")

    # ---------------------------------
    # XOR DISTANCE
    # ---------------------------------

    def calculate_distance(self, peer_id):

        peer_id_int = int(peer_id, 16)

        return self.node_id_int ^ peer_id_int

    # ---------------------------------
    # ROUTING BUCKET
    # ---------------------------------

    def calculate_bucket(self, peer_id):

        distance = self.calculate_distance(peer_id)

        if distance == 0:
            return None

        return distance.bit_length() - 1

    # ---------------------------------
    # ADD PEER
    # ---------------------------------

    def add_peer(self, peer_id, address):

        if peer_id == self.node_id:
            return

        try:
            if len(peer_id) != 40:
                return

            int(peer_id, 16)

        except ValueError:
            return

        bucket_index = self.calculate_bucket(peer_id)

        if bucket_index is None:
            return

        bucket = self.routing_table[bucket_index]

        # Update existing peer
        for peer in bucket:

            if peer["node_id"] == peer_id:

                peer["address"] = address
                return

        # Add new peer
        if len(bucket) < K:

            bucket.append({
                "node_id": peer_id,
                "address": address
            })

            print("\nNew peer added to routing table.")
            print(f"Peer ID: {peer_id}")
            print(f"Peer address: {address}")
            print(f"Bucket index: {bucket_index}")

    # ---------------------------------
    # GET ALL PEERS
    # ---------------------------------

    def get_all_peers(self):

        peers = []

        for bucket in self.routing_table.values():

            peers.extend(bucket)

        return peers

    # ---------------------------------
    # FIND CLOSEST PEERS
    # ---------------------------------

    def get_closest_peers(self, target_id):

        peers = self.get_all_peers()

        peers.sort(
            key=lambda peer: (
                int(peer["node_id"], 16)
                ^ int(target_id, 16)
            )
        )

        return peers[:K]

    # ---------------------------------
    # SEND MESSAGE
    # ---------------------------------

    def send_message(self, message, address):

        if self.transport is None:
            return

        data = json.dumps(message).encode()

        self.transport.sendto(data, address)

    # ---------------------------------
    # RECEIVE MESSAGES
    # ---------------------------------

    def datagram_received(self, data, addr):

        try:

            message = json.loads(data.decode())

            message_type = message.get("type")

            # JOIN REQUEST
            if message_type == "JOIN":

                peer_id = message.get("node_id")

                if not peer_id:
                    return

                self.add_peer(peer_id, addr)

                own_address = self.transport.get_extra_info(
                    "sockname"
                )

                peer_list = [{
                    "node_id": self.node_id,
                    "host": own_address[0],
                    "port": own_address[1]
                }]

                for peer in self.get_all_peers():

                    peer_list.append({
                        "node_id": peer["node_id"],
                        "host": peer["address"][0],
                        "port": peer["address"][1]
                    })

                response = {
                    "type": "PEERS",
                    "peers": peer_list
                }

                self.send_message(response, addr)

            # PEER DISCOVERY RESPONSE
            elif message_type == "PEERS":

                for peer in message.get("peers", []):

                    address = (
                        peer["host"],
                        peer["port"]
                    )

                    self.add_peer(
                        peer["node_id"],
                        address
                    )

            # FIND_NODE REQUEST
            elif message_type == "FIND_NODE":

                requester_id = message.get("sender_id")
                target_id = message.get("target_id")
                request_id = message.get("request_id")

                if not target_id or not request_id:
                    return

                if requester_id:
                    self.add_peer(requester_id, addr)

                closest_peers = self.get_closest_peers(
                    target_id
                )

                results = []

                for peer in closest_peers:

                    results.append({
                        "node_id": peer["node_id"],
                        "host": peer["address"][0],
                        "port": peer["address"][1]
                    })

                own_address = self.transport.get_extra_info(
                    "sockname"
                )

                results.append({
                    "node_id": self.node_id,
                    "host": own_address[0],
                    "port": own_address[1]
                })

                results.sort(
                    key=lambda peer: (
                        int(peer["node_id"], 16)
                        ^ int(target_id, 16)
                    )
                )

                response = {
                    "type": "NODES",
                    "request_id": request_id,
                    "nodes": results[:K]
                }

                self.send_message(response, addr)

                print(
                    f"\nFIND_NODE request received from {addr}"
                )

            # FIND_NODE RESPONSE
            elif message_type == "NODES":

                request_id = message.get("request_id")

                if request_id in self.pending_requests:

                    future = self.pending_requests[request_id]

                    if not future.done():

                        future.set_result(
                            message.get("nodes", [])
                        )

            # GOSSIP MESSAGE
            elif message_type == "GOSSIP":

                self.receive_gossip(message, addr)

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            ValueError,
            KeyError,
            TypeError
        ) as error:

            print(f"Invalid message: {error}")

    # ---------------------------------
    # FIND_NODE REQUEST TO A PEER
    # ---------------------------------

    async def query_peer(self, peer, target_id):

        request_id = str(uuid.uuid4())

        loop = asyncio.get_running_loop()

        future = loop.create_future()

        self.pending_requests[request_id] = future

        request = {
            "type": "FIND_NODE",
            "sender_id": self.node_id,
            "target_id": target_id,
            "request_id": request_id
        }

        try:

            self.send_message(
                request,
                peer["address"]
            )

            response = await asyncio.wait_for(
                future,
                timeout=LOOKUP_TIMEOUT
            )

            return response

        except asyncio.TimeoutError:

            print(
                f"Peer did not respond: {peer['address']}"
            )

            return []

        finally:

            self.pending_requests.pop(
                request_id,
                None
            )

    # ---------------------------------
    # ITERATIVE FIND_NODE
    # ---------------------------------

    async def find_node(self, target_id):

        print("\n================================")
        print("STARTING Kademlia FIND_NODE")
        print("================================")

        print(f"Target Node ID: {target_id}")

        queried = set()

        for _ in range(K):

            closest = self.get_closest_peers(target_id)

            candidates = [
                peer for peer in closest
                if peer["node_id"] not in queried
            ]

            if not candidates:
                break

            peer = candidates[0]

            queried.add(peer["node_id"])

            print(
                f"\nQuerying peer: {peer['address']}"
            )

            results = await self.query_peer(
                peer,
                target_id
            )

            for result in results:

                address = (
                    result["host"],
                    result["port"]
                )

                self.add_peer(
                    result["node_id"],
                    address
                )

        final_peers = self.get_closest_peers(target_id)

        print("\nFIND_NODE lookup completed.")

        print("\nClosest known nodes:")

        if not final_peers:
            print("No peers found.")

        for peer in final_peers:

            distance = (
                int(peer["node_id"], 16)
                ^ int(target_id, 16)
            )

            print(f"Node ID: {peer['node_id']}")
            print(f"Address: {peer['address']}")
            print(f"XOR distance: {distance}")
            print("-------------------------")

        return final_peers

    # =================================
    # GOSSIP PROTOCOL
    # =================================

    # COLLECT CPU AND RAM
    def get_resource_usage(self):

        cpu_usage = psutil.cpu_percent(
            interval=None
        )

        ram_usage = psutil.virtual_memory().percent

        return {
            "cpu": cpu_usage,
            "ram": ram_usage
        }

    # SEND GOSSIP
    def send_gossip(self):

        resources = self.get_resource_usage()

        message = {
            "type": "GOSSIP",
            "node_id": self.node_id,
            "node_name": self.node_name,
            "cpu": resources["cpu"],
            "ram": resources["ram"]
        }

        peers = self.get_all_peers()

        for peer in peers:

            self.send_message(
                message,
                peer["address"]
            )

        print("\nGOSSIP INFORMATION SENT")
        print("-------------------------")
        print(f"Node: {self.node_name}")
        print(f"CPU Usage: {resources['cpu']}%")
        print(f"RAM Usage: {resources['ram']}%")
        print(f"Peers contacted: {len(peers)}")

    # RECEIVE GOSSIP
    def receive_gossip(self, message, addr):

        node_id = message.get("node_id")

        if not node_id or node_id == self.node_id:
            return

        self.peer_resources[node_id] = {
            "node_name": message.get("node_name"),
            "address": addr,
            "cpu": message.get("cpu"),
            "ram": message.get("ram")
        }

        print("\nGOSSIP MESSAGE RECEIVED")
        print("-------------------------")
        print(f"Node: {message.get('node_name')}")
        print(f"Address: {addr}")
        print(f"CPU Usage: {message.get('cpu')}%")
        print(f"RAM Usage: {message.get('ram')}%")

    # AUTOMATIC GOSSIP EVERY 5 SECONDS
    async def gossip_loop(self):

        while True:

            await asyncio.sleep(GOSSIP_INTERVAL)

            self.send_gossip()

    # ---------------------------------
    # DISPLAY RESOURCE TABLE
    # ---------------------------------

    def print_resource_table(self):

        print("\n========== RESOURCE TABLE ==========")

        for node_id, resource in self.peer_resources.items():

            print(f"\nNode: {resource['node_name']}")
            print(f"Address: {resource['address']}")
            print(f"CPU: {resource['cpu']}%")
            print(f"RAM: {resource['ram']}%")

        print("====================================")

    # ---------------------------------
    # DISPLAY ROUTING TABLE
    # ---------------------------------

    def print_routing_table(self):

        print(
            f"\n========== NODE {self.node_name} "
            "ROUTING TABLE =========="
        )

        found = False

        for bucket_index, bucket in self.routing_table.items():

            if bucket:

                found = True

                print(f"\nBucket {bucket_index}:")

                for peer in bucket:

                    print(f"Node ID: {peer['node_id']}")
                    print(f"Address: {peer['address']}")

        if not found:

            print("No peers in routing table.")

        print("====================================")

        self.print_resource_table()

    # ---------------------------------
    # CLOSE NODE
    # ---------------------------------

    def close(self):

        if self.transport:

            self.transport.close()


# =====================================
# MAIN PROGRAM
# =====================================

async def main():

    parser = argparse.ArgumentParser(
        description="MeshWeaver Kademlia and Gossip Node"
    )

    parser.add_argument(
        "--node",
        required=True
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True
    )

    parser.add_argument(
        "--bootstrap"
    )

    parser.add_argument(
        "--lookup-name"
    )

    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535.")

    if args.lookup_name and not args.bootstrap:
        parser.error("--lookup-name requires --bootstrap.")

    if args.bootstrap:

        try:
            host, port = args.bootstrap.rsplit(":", 1)
            bootstrap_address = (host, int(port))

        except ValueError:
            parser.error("Invalid bootstrap address.")

    loop = asyncio.get_running_loop()

    transport, node = await loop.create_datagram_endpoint(
        lambda: KademliaNode(args.node),
        local_addr=("127.0.0.1", args.port)
    )

    try:

        # JOIN NETWORK
        if args.bootstrap:

            join_message = {
                "type": "JOIN",
                "node_id": node.node_id
            }

            node.send_message(
                join_message,
                bootstrap_address
            )

            print(
                f"\nJoining network through "
                f"{bootstrap_address}"
            )

            await asyncio.sleep(1)

        # OPTIONAL FIND_NODE
        if args.lookup_name:

            target_id = hashlib.sha1(
                args.lookup_name.encode()
            ).hexdigest()

            await node.find_node(target_id)

        # START AUTOMATIC GOSSIP
        gossip_task = asyncio.create_task(
            node.gossip_loop()
        )

        print("\nGossip Protocol started.")
        print("Resource sharing interval: 5 seconds.")

        # KEEP NODE RUNNING
        while True:

            await asyncio.sleep(15)

            node.print_routing_table()

    except asyncio.CancelledError:

        pass

    finally:

        if 'gossip_task' in locals():

            gossip_task.cancel()

        node.close()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nMeshWeaver node stopped.")
