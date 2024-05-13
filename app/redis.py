import asyncio
import logging
from typing import AsyncIterator

from app import constants
from app.commands import Command, CommandBuilder, CommandRunner
from app.dataclasses import ServerConfig
from app.decoder import RESPDecoder

logging.basicConfig(level=logging.INFO)


class NetworkIO:
    async def readlines(self, reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
        while line := await reader.read(constants.CHUNK_SIZE):
            yield line

    async def write(self, writer: asyncio.StreamWriter, data: bytes):
        writer.write(data)
        await writer.drain()


class RedisServer(NetworkIO):
    def __init__(self, config: ServerConfig):
        self.config = config

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.config.addr.host, self.config.addr.port
        )
        async with server:
            await server.serve_forever()

    async def additional_handling(self, reader, writer, request, command, args):
        pass

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connection with client: {addr}")

        try:
            async for request in self.readlines(reader):
                if not request:
                    break
                logging.info(
                    f"{self.config.mode.value}:Received request\r\n>> {request}\r\n"
                )
                commands = RESPDecoder.decode(request)
                for command, *args in commands:
                    logging.info(
                        f"{self.config.mode.value}:Run command\r\n>> {command.upper()} {args}\r\n"
                    )
                    responses = CommandRunner.run(command, self.config, *args)

                    for response in responses:
                        logging.info(
                            f"{self.config.mode.value}:Send response\r\n>> {response}\r\n"
                        )
                        await self.write(writer, response)

                    await self.additional_handling(
                        reader, writer, request, command, args
                    )

        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{self.config.mode.value}:Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"{self.config.mode.value}:Connection closed")


class RedisMaster(RedisServer):
    def __init__(self, config: ServerConfig):
        super().__init__(config)

    async def additional_handling(self, reader, writer, request, command, args):
        if command == Command.REPLCONF.value and "listening-port" in args:
            self.config.replicas.append((reader, writer))
            logging.info(f"{self.config.mode.value}:Replica added")

        if Command.is_propagated(command):
            await self.propagate(request)
            logging.info(f"{self.config.mode.value}:Command {command} propagated")

    async def propagate(self, request):
        for _, replica_writer in self.config.replicas:
            try:
                await self.write(replica_writer, request)
            except Exception as e:
                logging.error(
                    f"{self.config.mode.value}:Failed to connect to replica, error: {e}"
                )


class RedisSlave(RedisServer):
    def __init__(self, config: ServerConfig):
        super().__init__(config)
        self.reader = None
        self.writer = None

    async def start(self):
        await self.connect_to_master()
        asyncio.create_task(self.send_handshake())

        await super().start()

    async def connect_to_master(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.master_addr.host, self.config.master_addr.port
        )

    async def send_command(self, command):
        logging.info(f"{self.config.mode.value}:Send command\r\n>> {command}\r\n")
        await self.write(self.writer, command)
        response = await self.reader.readuntil(b"\r\n")
        logging.info(f"{self.config.mode.value}:Received response\r\n>> {response}\r\n")

    async def send_handshake(self):
        await self.send_command(CommandBuilder.build("PING"))
        await self.send_command(
            CommandBuilder.build(
                "REPLCONF", "listening-port", str(self.config.addr.port)
            )
        )
        await self.send_command(CommandBuilder.build("REPLCONF", "capa", "psync2"))
        await self.send_command(CommandBuilder.build("PSYNC", "?", "-1"))

        # read for rdb
        res = await self.reader.readuntil(constants.BTERM)
        await self.reader.readexactly(int(res[1:-2]))
        logging.info(f"{self.config.mode.value}:Handshake completed")

        await self.receive_commands()

    async def receive_commands(self):
        addr = self.writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connection with master: {addr}")

        try:
            async for request in self.readlines(self.reader):
                if not request:
                    break
                logging.info(
                    f"{self.config.mode.value}:Received master request\r\n>> {request}\r\n"
                )
                commands = RESPDecoder.decode(request)
                for command, *args in commands:
                    logging.info(
                        f"{self.config.mode.value}:Run naster command\r\n>> {command.upper()} {args}\r\n"
                    )
                    responses = CommandRunner.run(command, self.config, *args)
                    if command == Command.REPLCONF.value:
                        await self.write(self.writer, responses[0])
        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{self.config.mode.value}:Error handling client {addr}: {e}")
