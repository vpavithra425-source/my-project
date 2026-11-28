
import asyncio
import cloudpickle


class ServerNode(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport
        print("MeshWeaver Task Server is running on port 9999")

    def datagram_received(self, data, addr):
        print("\nTask received from client.")

        try:
            # Deserialize the received Python function and arguments
            task_function, task_arguments = cloudpickle.loads(data)

            print(f"Executing task: {task_function.__name__}")
            print(f"Arguments: {task_arguments}")

            # Execute the function
            result = task_function(*task_arguments)

            # Serialize the result
            response = cloudpickle.dumps(("success", result))

            print(f"Task completed. Result: {result}")

        except Exception as error:
            response = cloudpickle.dumps(("error", str(error)))
            print(f"Task failed: {error}")

        # Send the result back to the client
        self.transport.sendto(response, addr)

        print("Result sent back to client.")


async def main():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        ServerNode,
        local_addr=("127.0.0.1", 9999)
    )

    try:
        await asyncio.Future()

    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())

