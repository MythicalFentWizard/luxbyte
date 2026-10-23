#!/usr/bin/env python3
"""A TCP relay that adds a fixed one-way delay in each direction.

Usage: relay.py LISTEN_PORT:TARGET_PORT:ROUND_TRIP_MS [...]

Each mapping listens on 127.0.0.1:LISTEN_PORT and forwards to
127.0.0.1:TARGET_PORT, holding every chunk for half the round trip before
passing it on, in order, in both directions. Throughput is not limited:
chunks queue behind each other's deadlines, not behind each other's sleeps.
"""

import asyncio
import sys
import time


async def pump(reader, writer, delay):
    queue = asyncio.Queue()

    async def read():
        try:
            while data := await reader.read(65536):
                queue.put_nowait((time.monotonic() + delay, data))
        finally:
            queue.put_nowait((0.0, b""))

    async def write():
        while True:
            deadline, data = await queue.get()
            if not data:
                break
            pause = deadline - time.monotonic()
            if pause > 0:
                await asyncio.sleep(pause)
            writer.write(data)
            await writer.drain()
        writer.close()

    await asyncio.gather(read(), write(), return_exceptions=True)


def handler(target, delay):
    async def handle(client_reader, client_writer):
        try:
            server_reader, server_writer = await asyncio.open_connection("127.0.0.1", target)
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(
            pump(client_reader, server_writer, delay),
            pump(server_reader, client_writer, delay),
            return_exceptions=True,
        )
    return handle


async def main():
    servers = []
    for mapping in sys.argv[1:]:
        listen, target, round_trip = mapping.split(":")
        delay = float(round_trip) / 2000.0
        servers.append(await asyncio.start_server(
            handler(int(target), delay), "127.0.0.1", int(listen)))
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == "__main__":
    asyncio.run(main())
