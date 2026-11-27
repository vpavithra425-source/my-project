
# Day 4 - Measuring Async Execution Time

import asyncio
import time


async def task1():
    print("Task 1 Started")
    await asyncio.sleep(2)
    print("Task 1 Completed")


async def task2():
    print("Task 2 Started")
    await asyncio.sleep(2)
    print("Task 2 Completed")


async def task3():
    print("Task 3 Started")
    await asyncio.sleep(2)
    print("Task 3 Completed")


async def main():

    start_time = time.perf_counter()

    await asyncio.gather(
        task1(),
        task2(),
        task3()
    )

    end_time = time.perf_counter()

    total_time = end_time - start_time

    print("Total Execution Time:", round(total_time, 2), "seconds")


asyncio.run(main())





