import asyncio
from network.peer import PeerNode


def add(a, b):
    return a + b


async def main():
    node1 = PeerNode()
    node2 = PeerNode()

    addr1 = await node1.start()
    addr2 = await node2.start()

    print("\nTesting PING...")
    node1.ping(*addr2)

    await asyncio.sleep(1)

    print("\nTesting remote task...")
    result = await node1.send_task(
        *addr2, add, 10, 20
    )

    print("Final result:", result)

    node1.close()
    node2.close()


asyncio.run(main())