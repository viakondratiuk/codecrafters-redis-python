import asyncio
import logging
from typing import AsyncIterator
import re

from app.commands import Command, CommandBuilder, CommandRunner
from app import constants
from app.dataclasses import Mode, ServerConfig
from app.decoder import Decoder

logging.basicConfig(level=logging.INFO)

class NetworkIO:
    async def readlines(self, reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
        while line := await reader.read(constants.CHUNK_SIZE):
            yield line


class RedisServer(NetworkIO):
    def __init__(self, config: ServerConfig):
        self.config = config
        self.master_link = None
        self
        if config.mode == Mode.SLAVE:
            self.master_link = RedisReplica(config)

    async def start(self):
        if self.config.mode == Mode.SLAVE:
            asyncio.create_task(self.master_link.handshake())

        server = await asyncio.start_server(
            self.handle_client, self.config.my.host, self.config.my.port
        )
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connection with client: {addr}")

        try:
            async for request in self.readlines(reader):
                if not request:
                    break
                logging.info(f"{self.config.mode.value}:Received request\r\n>> {request}\r\n")
                commands = Decoder.decode(request)
                for command, *args in commands:
                    logging.info(f"{self.config.mode.value}:Run command\r\n>> {command.upper()} {args}\r\n")
                    responses = CommandRunner.run(command, self.config, *args)
                
                    for response in responses:
                        logging.info(f"{self.config.mode.value}:Send response\r\n>> {response}\r\n")
                        writer.write(response)
                        await writer.drain()

                    if self.config.mode == Mode.MASTER:
                        if command == Command.REPLCONF.value and "listening-port" in args:
                            self.config.replicas.append((reader, writer))
                        if command == Command.SET.value:
                            await self.propagate_to_replicas(request)

        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{self.config.mode.value}:Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"{self.config.mode.value}:Connection closed")

    async def propagate_to_replicas(self, request):
        for _, replica_writer in self.config.replicas:
            try:
                replica_writer.write(request)
                await replica_writer.drain()
            except Exception as e:
                logging.error(f"{self.config.mode.value}:Failed to connect to replica, error: {e}")


class RedisReplica(NetworkIO):
    def __init__(self, config: ServerConfig):
        self.config = config
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.master.host, self.config.master.port
        )

    async def send_handshake(self, command):
        logging.info(f"{self.config.mode.value}:Send handshake command\r\n>> {command}\r\n")
        self.writer.write(command)
        await self.writer.drain()
        response = await self.reader.readuntil(b"\r\n")
        logging.info(f"{self.config.mode.value}:Received handshake response\r\n>> {response}\r\n")

        # try:
        #     response = await asyncio.wait_for(self.reader.read(4096), timeout=10)
        #     logging.info(f"{self.config.mode.value}:Received handshake response\r\n>> {response}\r\n")
        #     try:
        #         decoded_response = response.decode()
        #     except UnicodeDecodeError:
        #         decoded_response = response
        #     return decoded_response
        # except asyncio.TimeoutError:
        #     logging.error("{self.config.mode.value}:Timed out waiting for response")
        #     return None
        # except Exception as e:
        #     logging.error(f"{self.config.mode.value}:Error receiving response: {e}")
        #     return None
        
    async def receive_commands(self):
        addr = self.writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connection with master: {addr}")
        
        try:
            async for request in self.readlines(self.reader):
                if not request:
                    break
                logging.info(f"{self.config.mode.value}:Received master request\r\n>> {request}\r\n")
                commands = Decoder.decode(request)
                for command, *args in commands:
                    logging.info(f"{self.config.mode.value}:Run naster command\r\n>> {command.upper()} {args}\r\n")
                    _ = CommandRunner.run(command, self.config, *args)
        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{self.config.mode.value}:Error handling client {addr}: {e}")

    async def handshake(self):
        if not self.writer:
            await self.connect()

        await self.send_handshake(CommandBuilder.build("PING"))
        logging.info(f"{self.config.mode.value}:PING")

        await self.send_handshake(
            CommandBuilder.build("REPLCONF", "listening-port", str(self.config.my.port))
        )
        logging.info(f"{self.config.mode.value}:REPLCONF1")

        await self.send_handshake(CommandBuilder.build("REPLCONF", "capa", "psync2"))
        logging.info(f"{self.config.mode.value}:REPLCONF2")
        
        await self.send_handshake(CommandBuilder.build("PSYNC", "?", "-1"))
        logging.info(f"{self.config.mode.value}:PSYNC1")

        # read for rdb
        res = await self.reader.readuntil(b"\r\n")
        await self.reader.readexactly(int(res[1:-2]))
        logging.info(f"{self.config.mode.value}:PSYNC2")
        logging.info(f"{self.config.mode.value}:Handshake completed")

        await self.receive_commands()

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
