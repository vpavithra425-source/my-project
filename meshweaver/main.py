import asyncio

from broker.queue import TaskQueue
from network.peer import PeerNode
from network.dht import DHTNode
from network.monitoring.resources import get_resource_usage


def add_numbers(a, b):
    return a + b


async def queue_worker(task_queue, peer):

    while True:
        host, port, function, args, kwargs = (
            await task_queue.get_task()
        )

        try:
            result = await peer.send_task(
                host, port, function, *args, **kwargs
            )

            print("Queue task result:", result)

        except Exception as error:
            print("Queue task failed:", error)

        finally:
            task_queue.task_done()


async def main():

    node1 = PeerNode()
    node2 = PeerNode()

    dht1 = DHTNode("127.0.0.1", 5000)
    dht2 = DHTNode("127.0.0.1", 5001)

    worker = None

    try:
        # 1. Start Peer Nodes
        addr1 = await node1.start()
        addr2 = await node2.start()

        print("\nPeer network started!")

        # 2. PING / PONG
        node1.ping(*addr2)
        await asyncio.sleep(1)

        # 3. Start DHT Nodes
        await dht1.start()
        await dht2.start()

        # 4. DHT JOIN
        await dht2.join_network("127.0.0.1", 5000)

        await asyncio.sleep(1)

        # 5. Iterative FIND_NODE
        await dht2.find_node(dht1.node_id)

        await asyncio.sleep(1)

        # 6. Task Queue and Remote Task Execution
        task_queue = TaskQueue()

        worker = asyncio.create_task(
            queue_worker(task_queue, node1)
        )

        await task_queue.add_task(
            (
                addr2[0],
                addr2[1],
                add_numbers,
                (10, 20),
                {}
            )
        )

        await task_queue.wait_until_empty()

        print("Queue processing completed!")

        worker.cancel()
        await asyncio.gather(
            worker,
            return_exceptions=True
        )
        worker = None

        # 7. Resource Monitoring
        usage = get_resource_usage()

        print("\nResource Monitoring:")
        print("CPU Usage:", usage["cpu"], "%")
        print("RAM Usage:", usage["ram"], "%")

        # 8. Gossip Protocol
        node1.send_gossip(
            addr2[0],
            addr2[1],
            usage["cpu"],
            usage["ram"]
        )

        node2.send_gossip(
            addr1[0],
            addr1[1],
            usage["cpu"],
            usage["ram"]
        )

        await asyncio.sleep(1)

        print("\nMeshWeaver integration test completed!")

    except Exception as error:
        print("\nIntegration error:", error)
        raise

    finally:
        if worker is not None:
            worker.cancel()
            await asyncio.gather(
                worker,
                return_exceptions=True
            )

        node1.close()
        node2.close()

        if dht1.transport:
            dht1.transport.close()

        if dht2.transport:
            dht2.transport.close()


if __name__ == "__main__":
    asyncio.run(main())