print("Queue Error Test Started!")

import asyncio
from broker.queue import TaskQueue
from network.peer import PeerNode


def failing_task():
    raise ValueError("Intentional test error")


async def main():
    peer1 = PeerNode()
    peer2 = PeerNode()
    task_queue = TaskQueue()
    worker = None

    try:
        addr1 = await peer1.start()
        addr2 = await peer2.start()

        async def queue_worker():
            while True:
                host, port, function, args, kwargs = await task_queue.get_task()

                try:
                    result = await peer1.send_task(
                        host, port, function, *args, **kwargs
                    )
                    print("Queue task result:", result)

                except Exception as error:
                    print("Queue task failed:", error)

                finally:
                    task_queue.task_done()

        worker = asyncio.create_task(queue_worker())

        await task_queue.add_task(
            (addr2[0], addr2[1], failing_task, (), {})
        )

        await task_queue.wait_until_empty()

        print("Queue error handling test completed!")

    finally:
        if worker:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

        peer1.close()
        peer2.close()


if __name__ == "__main__":
    asyncio.run(main())