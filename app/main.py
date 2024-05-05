import argparse
import asyncio
import logging
from functools import partial

from app import constants
from app.commands import CommandContext
from app.dataclasses import Mode, ServerConfig

logging.basicConfig(level=logging.INFO)


class RedisProtocol:
    def __init__(self, command_context: CommandContext):
        self.command_context = command_context

    def parse(self, request: str):
        args = [arg.lower() for arg in request.strip().split("\r\n")]
        return args[2], [
            arg for idx, arg in enumerate(args) if idx >= 4 and idx % 2 == 0
        ]

    def execute(self, command, args):
        return self.command_context.execute(command, args)


async def handle_client(redis_protocol: RedisProtocol, client_reader, client_writer):
    while request := await client_reader.read(1024):
        if request:
            command, args = redis_protocol.parse(request.decode("utf-8"))
            response = redis_protocol.execute(command, args)
            client_writer.write(response.encode())
            await client_writer.drain()

    client_writer.close()
    logging.info("Connection closed")


def parse_args():
    parser = argparse.ArgumentParser(description="Read CLI parameters.")
    parser.add_argument(
        "--port",
        type=int,
        default=constants.DEFAULT_PORT,
        help=f"The port number to use. Defaults to {constants.DEFAULT_PORT}.",
    )
    parser.add_argument(
        "--replicaof",
        nargs=2,
        metavar=("master_host", "master_port"),
        type=str,
        help="Specifies the host and port of the master server.",
    )

    return parser.parse_args()


async def main():
    args = parse_args()

    server_config = ServerConfig(
        mode=Mode.MASTER if args.replicaof is None else Mode.SLAVE
    )
    command_context = CommandContext(server_config)
    redis_protocol = RedisProtocol(command_context)
    handle_client_mode = partial(handle_client, redis_protocol)

    logging.info("Server is starting up...")
    server = await asyncio.start_server(
        handle_client_mode, host="localhost", port=args.port
    )
    logging.info(f"Server started on localhost:{args.port}")
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
