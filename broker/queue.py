import asyncio


class TaskQueue:

    def __init__(self):
        self.queue = asyncio.Queue()

    async def add_task(self, task):
        await self.queue.put(task)

    async def get_task(self):
        return await self.queue.get()

    def task_done(self):
        self.queue.task_done()

    async def wait_until_empty(self):
        await self.queue.join()