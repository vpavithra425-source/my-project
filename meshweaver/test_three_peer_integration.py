import asyncio

from network.peer import PeerNode
from network.dht import DHTNode
from network.monitoring.resources import get_resource_usage


def add_numbers(a, b):
    return a + b


async def main():

    peer1 = PeerNode()
    peer2 = PeerNode()
    peer3 = PeerNode()

    dht1 = DHTNode("127.0.0.1", 5300)
    dht2 = DHTNode("127.0.0.1", 5301)
    dht3 = DHTNode("127.0.0.1", 5302)

    try:
        # 1. Start three Peer Nodes
        addr1 = await peer1.start()
        addr2 = await peer2.start()
        addr3 = await peer3.start()

        print("\nAll three Peer Nodes started!")

        # 2. PING / PONG
        peer1.ping(*addr2)
        peer2.ping(*addr3)
        await asyncio.sleep(1)

        # 3. Start three DHT Nodes
        await dht1.start()
        await dht2.start()
        await dht3.start()

        # 4. Join DHT network
        await dht2.join_network("127.0.0.1", 5300)
        await dht3.join_network("127.0.0.1", 5300)

        await asyncio.sleep(1)

        # 5. Iterative FIND_NODE
        await dht2.find_node(dht3.node_id)

        await asyncio.sleep(1)

        # 6. Remote Task Execution
        result = await peer1.send_task(
            addr3[0],
            addr3[1],
            add_numbers,
            15,
            25
        )

        print("\nRemote task result:", result)

        # 7. Resource Monitoring
        usage = get_resource_usage()

        print("\nResource Monitoring:")
        print("CPU:", usage["cpu"], "%")
        print("RAM:", usage["ram"], "%")

        # 8. Gossip between three peers
        peer1.send_gossip(
            addr2[0], addr2[1],
            usage["cpu"], usage["ram"]
        )

        peer2.send_gossip(
            addr3[0], addr3[1],
            usage["cpu"], usage["ram"]
        )

        peer3.send_gossip(
            addr1[0], addr1[1],
            usage["cpu"], usage["ram"]
        )

        await asyncio.sleep(1)

        print("\nThree-peer integration test completed!")

    finally:
        for peer in (peer1, peer2, peer3):
            peer.close()

        for node in (dht1, dht2, dht3):
            if node.transport:
                node.transport.close()


if __name__ == "__main__":
    asyncio.run(main())