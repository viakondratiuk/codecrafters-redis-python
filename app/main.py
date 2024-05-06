import argparse
import asyncio
import logging

from app import constants
from app.commands import Command
from app.dataclasses import Mode, ServerConfig
from app.request import Request
from app.response import Response

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.master_link = None
        if config.mode == Mode.SLAVE:
            self.master_link = RedisClient(config)

    async def start(self):
        if self.config.mode == Mode.SLAVE:
            await self.master_link.handshake()

        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        while request := await reader.read(1024):
            if request:
                command, args = Request.parse(request.decode())
                logging.info(f"Executing command: {command} with args: {args}")
                result = Command.run(command, self.config, *args)
                response = Response.encode(result)
                logging.info(f"Response: {response}")
                writer.write(response.encode())
                await writer.drain()
        writer.close()
        logging.info("Connection closed")


class RedisClient:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.master_host, self.config.master_port
        )

    async def send_command(self, command_function, *args):
        command = Response.encode(command_function(*args))
        logging.info(f"Sending command:\r\n{command}")
        self.writer.write(command.encode())
        await self.writer.drain()

        try:
            response = await asyncio.wait_for(self.reader.read(1024), timeout=10)  # Timeout of 10 seconds
            decoded_response = response.decode()
            logging.info(f"Received response:\r\n{decoded_response}")
            return decoded_response
        except asyncio.TimeoutError:
            logging.error("Timed out waiting for response")
            return None
        except Exception as e:
            logging.error(f"Error receiving response: {e}")
            return None

    async def handshake(self):
        if not self.writer:
            await self.connect()

        await self.send_command(Command.ping)
        await self.send_command(
            Command.replconf, "listening-port", str(self.config.port))
        await self.send_command(Command.replconf, "capa", "psync2")

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()


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
        port=int(args.port),
    )
    if args.replicaof is not None:
        config.mode = Mode.SLAVE
        config.master_host = args.replicaof[0]
        config.master_port = int(args.replicaof[1])

    server = RedisServer(config)
    logging.info(f"Server is starting on {config.host}:{config.port}")
    await server.start()
    logging.info("Server started")


if __name__ == "__main__":
    asyncio.run(main())
