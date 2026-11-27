import asyncio
from broker.queue import TaskQueue

async def main():
    queue = TaskQueue()

    await queue.add_task("Task 1")
    await queue.add_task("Task 2")

    task1 = await queue.get_task()
    print("Received:", task1)
    queue.task_done()

    task2 = await queue.get_task()
    print("Received:", task2)
    queue.task_done()

    await queue.wait_until_empty()
    print("All tasks completed!")

asyncio.run(main())