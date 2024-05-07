import asyncio
import logging

from app.commands import CommandBuilder, CommandRunner, Command
from app.dataclasses import Mode, ServerConfig, Address
from app.decoder import Decoder

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.master_link = None
        if config.mode == Mode.REPLICA:
            self.master_link = RedisReplica(config)

    async def start(self):
        if self.config.mode == Mode.REPLICA:
            await self.master_link.handshake()

        server = await asyncio.start_server(
            self.handle_client, self.config.my.host, self.config.my.port
        )
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        while request := await reader.read(4096):
            if request:
                command, args = Decoder.decode(request.decode())
                logging.info(f"Running command: {command} with args: {args}")
                response = CommandRunner.run(command, self.config, *args)
                if self.config.mode == Mode.MASTER and command == Command.REPLCONF.value and "listening-port" in args:
                    self.config.replicas.append(
                        Address(
                            host="localhost",
                            port=args[2]
                        )
                    )
                if self.config.mode == Mode.MASTER and command == Command.SET.value:
                    for replica_addr in self.config.replicas:
                        _, replica_writer = await asyncio.open_connection(
                            replica_addr.host, replica_addr.port
                        )
                        replica_writer.write(request)
                logging.info(f"Sending response: {response}")
                writer.write(response)
                await writer.drain()
        writer.close()
        logging.info("Connection closed")


class RedisReplica:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.master.host, self.config.master.port
        )

    async def send(self, command):
        logging.info(f"Sending command:\r\n{command}")
        self.writer.write(command)
        await self.writer.drain()

        try:
            response = await asyncio.wait_for(self.reader.read(4096), timeout=10)
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
            CommandBuilder.build("REPLCONF", "listening-port", str(self.config.my.port))
        )
        await self.send(CommandBuilder.build("REPLCONF", "capa", "psync2"))
        await self.send(CommandBuilder.build("PSYNC", "?", "-1"))

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
