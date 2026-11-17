
import asyncio


# Server node
class ServerNode(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport
        print("Server Node is running...")

    def datagram_received(self, data, addr):
        message = data.decode()

        print(f"Received message from client: {message}")

        response = "Hello Client! Message received successfully."

        self.transport.sendto(response.encode(), addr)


# Client node
class ClientNode(asyncio.DatagramProtocol):

    def __init__(self, response_future):
        self.response_future = response_future

    def connection_made(self, transport):
        self.transport = transport

        message = "Hello Server! I am a MeshWeaver node."

        print("Client is sending message...")
        self.transport.sendto(
            message.encode(),
            ("127.0.0.1", 9999)
        )

    def datagram_received(self, data, addr):
        response = data.decode()

        print(f"Response from server: {response}")

        if not self.response_future.done():
            self.response_future.set_result(response)


async def main():

    loop = asyncio.get_running_loop()

    # Start server
    server_transport, server_protocol = (
        await loop.create_datagram_endpoint(
            ServerNode,
            local_addr=("127.0.0.1", 9999)
        )
    )

    # Create client
    response_future = loop.create_future()

    client_transport, client_protocol = (
        await loop.create_datagram_endpoint(
            lambda: ClientNode(response_future),
            local_addr=("127.0.0.1", 0)
        )
    )

    try:
        await asyncio.wait_for(response_future, timeout=5)
    finally:
        client_transport.close()
        server_transport.close()


if __name__ == "__main__":
    asyncio.run(main())
