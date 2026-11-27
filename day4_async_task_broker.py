
# Day 4 - Mini Async Task Broker
# Queue-Based Task Processing with Multiple Workers

import asyncio


# Execute one task
async def execute_task(task):

    try:
        print("Task Received:", task)

        if not isinstance(task, dict):
            raise TypeError("Task must be a dictionary")

        required_fields = ["task_id", "operation", "num1", "num2"]

        for field in required_fields:
            if field not in task:
                raise ValueError(f"Missing field: {field}")

        num1 = task["num1"]
        num2 = task["num2"]
        operation = task["operation"]

        if not isinstance(num1, (int, float)) or isinstance(num1, bool):
            raise TypeError("Invalid first number")

        if not isinstance(num2, (int, float)) or isinstance(num2, bool):
            raise TypeError("Invalid second number")

        if not isinstance(operation, str):
            raise TypeError("Operation must be a string")

        # Simulate task processing
        await asyncio.sleep(2)

        if operation == "addition":
            result = num1 + num2

        elif operation == "subtraction":
            result = num1 - num2

        elif operation == "multiplication":
            result = num1 * num2

        elif operation == "division":
            if num2 == 0:
                raise ZeroDivisionError("Division by zero is not allowed")

            result = num1 / num2

        else:
            raise ValueError("Unsupported operation")

        return {
            "task_id": task["task_id"],
            "status": "Success",
            "result": result
        }

    except Exception as error:

        return {
            "task_id": task.get("task_id") if isinstance(task, dict) else None,
            "status": "Failed",
            "error": str(error)
        }


# Worker function
async def worker(worker_id, task_queue, results):

    while True:

        task = await task_queue.get()

        try:
            # None means worker should stop
            if task is None:
                print(f"Worker {worker_id} stopping.")
                break

            print(f"Worker {worker_id} is processing a task")

            response = await execute_task(task)

            results.append(response)

            print(f"Worker {worker_id} completed:", response)

        finally:
            task_queue.task_done()


# Main function
async def main():

    # Create an asynchronous task queue
    task_queue = asyncio.Queue()

    results = []

    # Tasks received by the broker
    tasks = [
        {
            "task_id": 1,
            "operation": "addition",
            "num1": 10,
            "num2": 20
        },
        {
            "task_id": 2,
            "operation": "subtraction",
            "num1": 50,
            "num2": 20
        },
        {
            "task_id": 3,
            "operation": "multiplication",
            "num1": 10,
            "num2": 5
        },
        {
            "task_id": 4,
            "operation": "division",
            "num1": 10,
            "num2": 0
        },
        {
            "task_id": 5,
            "operation": "power",
            "num1": 2,
            "num2": 3
        },
        {
            "task_id": 6,
            "operation": "addition",
            "num1": "ten",
            "num2": 20
        },
        {
            "task_id": 7,
            "operation": "addition",
            "num1": 10
        }
    ]

    # Add tasks to the queue
    for task in tasks:
        await task_queue.put(task)

    print("All tasks added to the queue.")

    # Start two workers
    workers = [
        asyncio.create_task(worker(1, task_queue, results)),
        asyncio.create_task(worker(2, task_queue, results))
    ]

    # Add stop signals for both workers
    for _ in workers:
        await task_queue.put(None)

    # Wait until all queued items are processed
    await task_queue.join()

    # Wait for workers to stop
    await asyncio.gather(*workers)

    # Display final results
    print("\n========== FINAL RESULTS ==========")

    for response in sorted(
        results,
        key=lambda item: item["task_id"] if item["task_id"] is not None else -1
    ):
        print(response)

    print("\nAll Tasks Processed!")


# Start program
if __name__ == "__main__":
    asyncio.run(main())
