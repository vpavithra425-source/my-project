
import asyncio
import cloudpickle


def add_numbers(a, b):
    return a + b


def subtract_numbers(a, b):
    return a - b


def multiply_numbers(a, b):
    return a * b


def divide_numbers(a, b):
    return a / b


class ClientNode(asyncio.DatagramProtocol):

    def __init__(self, response_future):
        self.response_future = response_future
        self.tasks = [
            (add_numbers, (10, 20)),
            (subtract_numbers, (50, 20)),
            (multiply_numbers, (10, 5)),
            (divide_numbers, (10, 0))
        ]
        self.current_task = 0
        self.failed_tasks = 0

    def connection_made(self, transport):
        self.transport = transport
        print("MeshWeaver Client Node started.")
        print(f"Total tasks: {len(self.tasks)}")
        self.send_next_task()

    def send_next_task(self):

        if self.current_task >= len(self.tasks):
            successful = len(self.tasks) - self.failed_tasks

            print("\nAll tasks processed.")
            print(f"Successful tasks: {successful}")
            print(f"Failed tasks: {self.failed_tasks}")

            if not self.response_future.done():
                self.response_future.set_result(True)

            return

        function, arguments = self.tasks[self.current_task]

        task_data = cloudpickle.dumps((function, arguments))

        print(
            f"\nSending task {self.current_task + 1}: "
            f"{function.__name__}{arguments}"
        )

        self.transport.sendto(task_data, ("127.0.0.1", 9999))

    def datagram_received(self, data, addr):

        try:
            status, result = cloudpickle.loads(data)

            if status == "success":
                print(f"Task result: {result}")
            else:
                self.failed_tasks += 1
                print(f"Task failed: {result}")

            self.current_task += 1
            self.send_next_task()

        except Exception as error:
            print(f"Response processing failed: {error}")

            if not self.response_future.done():
                self.response_future.set_exception(error)


async def main():

    loop = asyncio.get_running_loop()
    response_future = loop.create_future()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ClientNode(response_future),
        local_addr=("127.0.0.1", 0)
    )

    try:
        await asyncio.wait_for(response_future, timeout=20)

    except asyncio.TimeoutError:
        print("Server did not respond.")

    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
