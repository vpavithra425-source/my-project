
import argparse
import asyncio
import ctypes
import json
import math
import time
from ctypes import wintypes


GOSSIP_INTERVAL = 5
PEER_TIMEOUT = 15
MAX_PEERS = 50
MAX_MESSAGE_SIZE = 65507


# ==========================================
# CPU MONITORING USING WINDOWS API
# ==========================================

class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", wintypes.DWORD),
        ("dwHighDateTime", wintypes.DWORD),
    ]


class CPUMonitor:

    def __init__(self):
        self.kernel32 = ctypes.WinDLL("kernel32")

        self.get_system_times = self.kernel32.GetSystemTimes
        self.get_system_times.argtypes = [
            ctypes.POINTER(FILETIME),
            ctypes.POINTER(FILETIME),
            ctypes.POINTER(FILETIME),
        ]
        self.get_system_times.restype = wintypes.BOOL

        self.previous_times = None

    def get_times(self):
        idle = FILETIME()
        kernel = FILETIME()
        user = FILETIME()

        success = self.get_system_times(
            ctypes.byref(idle),
            ctypes.byref(kernel),
            ctypes.byref(user)
        )

        if not success:
            raise ctypes.WinError(ctypes.get_last_error())

        def convert(value):
            return (
                (value.dwHighDateTime << 32)
                | value.dwLowDateTime
            )

        return (
            convert(idle),
            convert(kernel),
            convert(user)
        )

    def get_cpu_usage(self):
        current = self.get_times()

        if self.previous_times is None:
            self.previous_times = current
            return None

        old_idle, old_kernel, old_user = self.previous_times
        new_idle, new_kernel, new_user = current

        self.previous_times = current

        idle_delta = new_idle - old_idle

        total_delta = (
            (new_kernel - old_kernel)
            + (new_user - old_user)
        )

        if total_delta <= 0:
            return None

        usage = (
            (total_delta - idle_delta) / total_delta
        ) * 100

        return round(max(0, min(100, usage)), 2)


# ==========================================
# RAM MONITORING USING WINDOWS API
# ==========================================

class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", wintypes.DWORD),
        ("dwMemoryLoad", wintypes.DWORD),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def get_ram_information():
    memory = MEMORYSTATUSEX()
    memory.dwLength = ctypes.sizeof(memory)

    kernel32 = ctypes.WinDLL("kernel32")

    get_memory_status = kernel32.GlobalMemoryStatusEx
    get_memory_status.argtypes = [
        ctypes.POINTER(MEMORYSTATUSEX)
    ]
    get_memory_status.restype = wintypes.BOOL

    success = get_memory_status(ctypes.byref(memory))

    if not success:
        raise ctypes.WinError(ctypes.get_last_error())

    mb = 1024 * 1024

    return {
        "total_ram_mb": round(memory.ullTotalPhys / mb, 2),
        "available_ram_mb": round(memory.ullAvailPhys / mb, 2),
        "used_ram_percent": memory.dwMemoryLoad
    }


# ==========================================
# PEER INFORMATION VALIDATION
# ==========================================

def is_valid_number(value, minimum, maximum):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and minimum <= value <= maximum
    )


def validate_peer(peer):
    if not isinstance(peer, dict):
        return False

    peer_id = peer.get("peer_id")
    host = peer.get("host")
    port = peer.get("port")

    if not isinstance(peer_id, str) or not peer_id:
        return False

    if len(peer_id) > 255:
        return False

    if not isinstance(host, str) or not host:
        return False

    if len(host) > 253:
        return False

    if not isinstance(port, int) or isinstance(port, bool):
        return False

    if not 1 <= port <= 65535:
        return False

    if peer_id != f"{host}:{port}":
        return False

    cpu = peer.get("cpu_percent")

    if cpu is not None and not is_valid_number(cpu, 0, 100):
        return False

    total_ram = peer.get("total_ram_mb")
    available_ram = peer.get("available_ram_mb")
    used_ram = peer.get("used_ram_percent")

    if not is_valid_number(total_ram, 0, float("inf")):
        return False

    if not is_valid_number(available_ram, 0, float("inf")):
        return False

    if available_ram > total_ram:
        return False

    if not is_valid_number(used_ram, 0, 100):
        return False

    timestamp = peer.get("timestamp")

    if not is_valid_number(timestamp, 0, float("inf")):
        return False

    return True


# ==========================================
# GOSSIP NODE
# ==========================================

class GossipNode(asyncio.DatagramProtocol):

    def __init__(self, host, port, seed_peers):
        self.host = host
        self.port = port
        self.peer_id = f"{host}:{port}"

        self.seed_peers = seed_peers
        self.known_peers = {}

        self.transport = None
        self.cpu_monitor = CPUMonitor()

        self.cpu_percent = None
        self.ram_information = {}

    def connection_made(self, transport):
        self.transport = transport

        print("\nMeshWeaver Gossip Node Started")
        print(f"Node ID: {self.peer_id}")
        print(f"Listening on: {self.host}:{self.port}")
        print(f"Gossip interval: {GOSSIP_INTERVAL} seconds")
        print(f"Maximum known peers: {MAX_PEERS}")

        asyncio.create_task(self.gossip_loop())

    def get_local_information(self):
        return {
            "peer_id": self.peer_id,
            "host": self.host,
            "port": self.port,
            "cpu_percent": self.cpu_percent,
            **self.ram_information,
            "timestamp": time.time()
        }

    def add_peer(self, peer):
        peer_id = peer["peer_id"]

        if peer_id == self.peer_id:
            return

        if peer_id not in self.known_peers:
            if len(self.known_peers) >= MAX_PEERS:
                print("Peer limit reached. Ignoring new peer.")
                return

        self.known_peers[peer_id] = {
            **peer,
            "last_seen": time.time()
        }

    def datagram_received(self, data, addr):

        if len(data) > MAX_MESSAGE_SIZE:
            print("Rejected oversized gossip message.")
            return

        try:
            message = json.loads(data.decode("utf-8"))

            if not isinstance(message, dict):
                return

            if message.get("type") != "gossip":
                return

            sender = message.get("sender")

            if not validate_peer(sender):
                print("Rejected invalid sender information.")
                return

            # Verify that the sender's port matches
            # the actual UDP source port.
            if sender["port"] != addr[1]:
                print("Rejected sender with mismatched port.")
                return

            if sender["peer_id"] == self.peer_id:
                return

            self.add_peer(sender)

            print(f"\nGossip received from: {sender['peer_id']}")

            peer_list = message.get("peers", [])

            if not isinstance(peer_list, list):
                return

            if len(peer_list) > MAX_PEERS:
                print("Rejected oversized peer list.")
                return

            for peer in peer_list:

                if not validate_peer(peer):
                    print("Ignored invalid peer information.")
                    continue

                if peer["peer_id"] == self.peer_id:
                    continue

                self.add_peer(peer)

                print(
                    f"Peer: {peer['peer_id']} | "
                    f"CPU: {peer['cpu_percent']}% | "
                    f"RAM: {peer['used_ram_percent']}%"
                )

        except (
            ValueError,
            UnicodeDecodeError,
            TypeError,
            RecursionError
        ) as error:
            print(f"Invalid gossip message: {error}")

    def remove_stale_peers(self):
        current_time = time.time()

        stale_peers = [
            peer_id
            for peer_id, peer in self.known_peers.items()
            if current_time - peer["last_seen"] > PEER_TIMEOUT
        ]

        for peer_id in stale_peers:
            del self.known_peers[peer_id]
            print(f"Removed inactive peer: {peer_id}")

    async def gossip_loop(self):

        while True:

            try:
                self.remove_stale_peers()

                self.cpu_percent = (
                    self.cpu_monitor.get_cpu_usage()
                )

                self.ram_information = get_ram_information()

                local_information = self.get_local_information()

                peer_list = [
                    local_information,
                    *self.known_peers.values()
                ]

                message = {
                    "type": "gossip",
                    "sender": local_information,
                    "peers": peer_list
                }

                encoded_message = json.dumps(message).encode("utf-8")

                destinations = {
                    (peer["host"], peer["port"])
                    for peer in self.seed_peers
                }

                for peer in self.known_peers.values():
                    destinations.add(
                        (peer["host"], peer["port"])
                    )

                destinations.discard((self.host, self.port))

                for destination in destinations:
                    self.transport.sendto(
                        encoded_message,
                        destination
                    )

                    print(
                        f"Gossip sent to "
                        f"{destination[0]}:{destination[1]}"
                    )

                print(
                    f"\n[{self.peer_id}] "
                    f"CPU: {self.cpu_percent}% | "
                    f"RAM Used: "
                    f"{self.ram_information['used_ram_percent']}%"
                )

                print(
                    f"Known peers: {len(self.known_peers)}"
                )

            except (OSError, ValueError, RuntimeError) as error:
                print(f"Gossip error: {error}")

            await asyncio.sleep(GOSSIP_INTERVAL)


# ==========================================
# START GOSSIP NODE
# ==========================================

async def main():

    parser = argparse.ArgumentParser(
        description="MeshWeaver Gossip Protocol"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True
    )

    parser.add_argument(
        "--peers",
        nargs="*",
        default=[]
    )

    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535.")

    seed_peers = []

    for peer in args.peers:

        try:
            peer_host, peer_port = peer.rsplit(":", 1)
            peer_port = int(peer_port)

            seed = {
                "peer_id": f"{peer_host}:{peer_port}",
                "host": peer_host,
                "port": peer_port
            }

            if (
                not peer_host
                or not 1 <= peer_port <= 65535
            ):
                raise ValueError

            seed_peers.append({
                "host": peer_host,
                "port": peer_port
            })

        except ValueError:
            parser.error(
                f"Invalid peer format: {peer}. "
                "Expected HOST:PORT."
            )

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: GossipNode(
            args.host,
            args.port,
            seed_peers
        ),
        local_addr=(args.host, args.port)
    )

    try:
        await asyncio.Future()

    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
