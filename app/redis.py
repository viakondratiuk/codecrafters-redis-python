import asyncio
import logging

from app.commands import CommandBuilder, CommandRunner, Command
from app.dataclasses import Mode, ServerConfig, Address
from app.decoder import Decoder
from app.constants import CHUNK_SIZE
from typing import AsyncIterator

logging.basicConfig(level=logging.INFO)


class RedisServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.master_link = None
        if config.mode == Mode.SLAVE:
            self.master_link = RedisReplica(config)

    async def start(self):
        if self.config.mode == Mode.SLAVE:
            await self.master_link.handshake()

        server = await asyncio.start_server(
            self.handle_client, self.config.my.host, self.config.my.port
        )
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, 
                            writer: asyncio.StreamWriter)-> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Connection established with {addr}")
        
        try:
            async for request in self.readlines(reader):
                if not request:
                    break
                command, args = Decoder.decode(request.decode())
                logging.info(f"Running command: {command} with args: {args}")
                response = CommandRunner.run(command, self.config, *args)
                logging.info(f"Sending response: {response}")
                writer.write(response)
                await writer.drain()
                
                if self.config.mode == Mode.MASTER: 
                    if command == Command.REPLCONF.value and "listening-port" in args:
                        self.config.replicas.append((reader, writer))
                    if command == Command.SET.value:
                        await self.propagate_to_replicas(request)
                    
        except ConnectionResetError:
            logging.error(f"Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info("Connection closed")

    async def readlines(self, reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
        while line := await reader.read(CHUNK_SIZE):
            yield line

    async def propagate_to_replicas(self, request):
        for _, replica_writer in self.config.replicas:
            try:
                replica_writer.write(request)
                await replica_writer.drain()
            except Exception as e:
                logging.error(f"Failed to connect to replica, error: {e}")


class RedisReplica:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.config.master.host, self.config.master.port)

    async def send(self, command):
        logging.info(f"Sending command:\r\n{command}")
        self.writer.write(command)
        await self.writer.drain()

        try:
            response = await asyncio.wait_for(self.reader.read(4096), timeout=10)
            try:
                decoded_response = response.decode()
            except UnicodeDecodeError:
                decoded_response = response
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
