import asyncio
from dht import DHTNode


async def main():

    peer1 = DHTNode("127.0.0.1", 5200)

    try:
        await peer1.start()

        print("\nPeer 1 started!")

        # Create four peer IDs in the same bucket
        base_id = int(peer1.node_id, 16)

        def make_peer(distance, port):
            peer_id = format(
                base_id ^ ((1 << 159) + distance),
                "040x"
            )

            return {
                "id": peer_id,
                "host": "127.0.0.1",
                "port": port
            }

        old_peer = make_peer(1, 5201)
        peer_b = make_peer(2, 5202)
        peer_c = make_peer(3, 5203)
        new_peer = make_peer(4, 5204)

        # Fill the same K-Bucket
        print("\nAdding three peers to the same bucket...")

        await peer1.add_peer(old_peer)
        await peer1.add_peer(peer_b)
        await peer1.add_peer(peer_c)

        print("\nInitial K-Buckets:")
        peer1.show_k_buckets()

        # Add fourth peer to trigger replacement
        print("\nAdding fourth peer...")

        await peer1.add_peer(new_peer)

        print("\nFinal K-Buckets:")
        peer1.show_k_buckets()

        # Verify results
        print("\n========== TEST RESULTS ==========")

        if old_peer["id"] not in peer1.routing_table:
            print("Dead peer successfully removed!")
        else:
            print("Dead peer is still present!")

        if new_peer["id"] in peer1.routing_table:
            print("New peer successfully added!")
        else:
            print("New peer was not added!")

        print("\nReplacement Cache:")
        peer1.show_replacement_cache()

    finally:
        peer1.close()


if __name__ == "__main__":
    asyncio.run(main())