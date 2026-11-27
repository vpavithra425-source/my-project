import asyncio

from network.peer import PeerNode
from network.monitoring.resources import get_resource_usage


async def gossip_loop():
    peer1 = PeerNode()
    peer2 = PeerNode()

    address1 = await peer1.start()
    address2 = await peer2.start()

    print("Both peers are connected!")

    try:
        while True:
            usage = get_resource_usage()

            peer1.send_gossip(
                address2[0],
                address2[1],
                usage["cpu"],
                usage["ram"]
            )

            peer2.send_gossip(
                address1[0],
                address1[1],
                usage["cpu"],
                usage["ram"]
            )

            print("Gossip updates exchanged!")
            await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("Gossip stopped!")

    finally:
        peer1.close()
        peer2.close()


if __name__ == "__main__":
    asyncio.run(gossip_loop())