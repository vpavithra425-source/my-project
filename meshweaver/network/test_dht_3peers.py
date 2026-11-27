import asyncio
from dht import DHTNode


async def main():

    peer1 = DHTNode("127.0.0.1", 5000)
    peer2 = DHTNode("127.0.0.1", 5001)
    peer3 = DHTNode("127.0.0.1", 5002)

    try:
        # Start all three peers
        await peer1.start()
        await peer2.start()
        await peer3.start()

        print("\nAll three DHT peers started!")

        # Join peer2 and peer3 to peer1
        await peer2.join_network("127.0.0.1", 5000)
        await peer3.join_network("127.0.0.1", 5000)

        await asyncio.sleep(1)

        print("\nPeer 1 routing table:")
        peer1.show_k_buckets()

        print("\nPeer 2 routing table:")
        peer2.show_k_buckets()

        print("\nPeer 3 routing table:")
        peer3.show_k_buckets()

        # Test node lookup
        print("\nPeer 1 searching for Peer 3:")
        await peer1.find_node(peer3.node_id)

        print("\nThree-peer DHT test completed!")

    finally:
        for peer in (peer1, peer2, peer3):
            if peer.transport:
                peer.transport.close()


if __name__ == "__main__":
    asyncio.run(main())