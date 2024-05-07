import asyncio
import logging

from app.commands import CommandBuilder, CommandRunner
from app.dataclasses import Mode, ServerConfig
from app.decoder import Decoder

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
                command, args = Decoder.decode(request.decode())
                logging.info(f"Running command: {command} with args: {args}")
                response = CommandRunner.run(command, self.config, *args)
                logging.info(f"Sending response: {response}")
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

    async def send(self, command):
        logging.info(f"Sending command:\r\n{command}")
        self.writer.write(command.encode())
        await self.writer.drain()

        try:
            response = await asyncio.wait_for(self.reader.read(1024), timeout=10)
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

        await self.send(CommandBuilder.build("PING"))
        await self.send(
            CommandBuilder.build("REPLCONF", "listening-port", str(self.config.port))
        )
        await self.send(CommandBuilder.build("REPLCONF", "capa", "psync2"))
        await self.send(CommandBuilder.build("PSYNC", "?", "-1"))

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
