import argparse
import asyncio
import logging

from app import constants
from app.commands import CommandContext
from app.dataclasses import Mode, Response, ServerConfig
from app.request import RedisRequest
from app.response import RESP, RedisResponse

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.command_context = CommandContext(config)

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        while request := await reader.read(1024):
            if request:
                print(request.decode("utf-8"))
                command, args = RedisRequest.parse(request.decode("utf-8"))
                data = self.command_context.execute(command, args)
                response = RedisResponse.encode(data)
                writer.write(response.encode())
                await writer.drain()
        writer.close()
        logging.info("Connection closed")

    async def send_handshake(self, master_host, master_port):
        handshake = RedisResponse.encode(
            Response([Response("PING", RESP.BULK_STRING)], RESP.ARRAY)
        )
        _, writer = await asyncio.open_connection(master_host, master_port)
        try:
            writer.write(handshake.encode())
            await writer.drain()
        finally:
            writer.close()


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
    if config.mode == Mode.SLAVE:
        await server.send_handshake(args.replicaof[0], int(args.replicaof[1]))

    logging.info(f"Server is starting on {config.host}:{config.port}")
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
