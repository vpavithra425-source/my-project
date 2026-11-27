import asyncio
import cloudpickle
import uuid


class PeerProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):
        self.node = node

    def connection_made(self, transport):
        self.transport = transport
        print("Peer network started!")

    def datagram_received(self, data, address):
        try:
            message = cloudpickle.loads(data)

            if message["type"] == "TASK":
                print("Remote task received!")

                function = cloudpickle.loads(message["function"])
                args = message["args"]
                kwargs = message["kwargs"]

                try:
                    result = function(*args, **kwargs)

                    response = {
                        "type": "RESULT",
                        "task_id": message["task_id"],
                        "result": result,
                        "error": None
                    }

                except Exception as error:
                    response = {
                        "type": "RESULT",
                        "task_id": message["task_id"],
                        "result": None,
                        "error": str(error)
                    }

                self.transport.sendto(
                    cloudpickle.dumps(response),
                    address
                )

            elif message["type"] == "RESULT":
                print("Task result received!")

                task_id = message["task_id"]
                future = self.node.pending_tasks.pop(task_id, None)

                if future and not future.done():
                    if message["error"]:
                        future.set_exception(
                            RuntimeError(message["error"])
                        )
                    else:
                        future.set_result(message["result"])

            elif message["type"] == "PING":
                self.transport.sendto(
                    cloudpickle.dumps({"type": "PONG"}),
                    address
                )

            elif message["type"] == "PONG":
                print("PONG received!")

            elif message["type"] == "GOSSIP":
                print("Gossip received from peer!")
                print("CPU:", message["cpu"], "%")
                print("RAM:", message["ram"], "%")

        except Exception as error:
            print("Network error:", error)

    def error_received(self, error):
        print("Network error:", error)


class PeerNode:

    def __init__(self):
        self.transport = None
        self.protocol = None
        self.pending_tasks = {}

    async def start(self, host="127.0.0.1", port=0):
        loop = asyncio.get_running_loop()

        self.transport, self.protocol = (
            await loop.create_datagram_endpoint(
                lambda: PeerProtocol(self),
                local_addr=(host, port)
            )
        )

        address = self.transport.get_extra_info("sockname")
        print("Peer running at:", address)

        return address

    def ping(self, host, port):
        message = cloudpickle.dumps({"type": "PING"})
        self.transport.sendto(message, (host, port))
        print("PING message sent!")

    def send_gossip(self, host, port, cpu, ram):
        message = {
            "type": "GOSSIP",
            "cpu": cpu,
            "ram": ram
        }

        self.transport.sendto(
            cloudpickle.dumps(message),
            (host, port)
        )

        print("Gossip update sent!")

    async def send_task(self, host, port, function, *args, **kwargs):
        loop = asyncio.get_running_loop()
        task_id = str(uuid.uuid4())

        future = loop.create_future()
        self.pending_tasks[task_id] = future

        message = {
            "type": "TASK",
            "task_id": task_id,
            "function": cloudpickle.dumps(function),
            "args": args,
            "kwargs": kwargs
        }

        self.transport.sendto(
            cloudpickle.dumps(message),
            (host, port)
        )

        print("Remote task sent!")

        try:
            return await asyncio.wait_for(future, timeout=10)
        finally:
            self.pending_tasks.pop(task_id, None)

    def close(self):
        if self.transport:
            self.transport.close()