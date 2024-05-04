import argparse
import asyncio
import logging

from app.commands import CommandContext
from app.constants import DEFAULT_PORT

logging.basicConfig(level=logging.INFO)


class RedisProtocol:
    def parse(self, request: str):
        lines = request.strip().split("\r\n")
        command = lines[2].lower()
        args = lines[3:]
        return command, args


def dispatch_command(command, args):
    context = CommandContext(command)
    return context.execute(args)


async def handle_client(client_reader, client_writer):
    redis_protocol = RedisProtocol()

    while request := await client_reader.read(1024):
        if request:
            command, args = redis_protocol.parse(request.decode("utf-8"))
            response = dispatch_command(command, args)

            client_writer.write(response.encode())
            await client_writer.drain()

    client_writer.close()
    logging.info("Connection closed")


async def main():
    parser = argparse.ArgumentParser(description="Read CLI parameters.")
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"The port number to use. Defaults to {DEFAULT_PORT}.",
    )
    args = parser.parse_args()

    logging.info("Server is starting up...")
    server = await asyncio.start_server(handle_client, host="localhost", port=args.port)
    logging.info(f"Server started on localhost:{args.port}")
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
