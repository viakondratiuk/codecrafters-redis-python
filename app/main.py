import argparse
import asyncio
import logging

from app import constants
from app.commands import CommandContext
from app.dataclasses import Mode, ServerConfig

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.protocol = RedisProtocol()
        self.command_context = CommandContext(config)

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )
        await server.serve_forever()

    async def handle_client(self, client_reader, client_writer):
        while request := await client_reader.read(1024):
            if request:
                command, args = self.protocol.parse(request.decode("utf-8"))
                response = self.command_context.execute(command, args)
                client_writer.write(response.encode())
                await client_writer.drain()
        client_writer.close()
        logging.info("Connection closed")


class RedisProtocol:
    def parse(self, request: str):
        args = [arg.lower() for arg in request.strip().split("\r\n")]
        return args[2], [
            arg for idx, arg in enumerate(args) if idx >= 4 and idx % 2 == 0
        ]


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

    config = ServerConfig(
        host="localhost",
        port=args.port,
        mode=Mode.MASTER if args.replicaof is None else Mode.SLAVE,
    )
    server = RedisServer(config)
    logging.info(f"Server is starting on {config.host}:{config.port}")
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
