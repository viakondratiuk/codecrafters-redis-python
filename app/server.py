import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass
from typing import AsyncIterator

from app import constants
from app.commands import CommandBuilder, CommandsRunner
from app.dataclasses import ServerConfig
from app.decoder import RESPDecoder

logging.basicConfig(level=logging.INFO)


class ServerPort(ABC):
    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def handle_connection(self, reader: StreamReader, writer: StreamWriter):
        pass


# TODO: Do I really need it? extend it with functionality?
class NetworkIO:
    async def readlines(self, reader: StreamReader) -> AsyncIterator[bytes]:
        while line := await reader.read(constants.CHUNK_SIZE):
            yield line

    async def write(self, writer: StreamWriter, data: bytes):
        writer.write(data)
        await writer.drain()


@dataclass
class RedisMaster(ServerPort, NetworkIO):
    # TODO: Split to master and slave parts
    config: ServerConfig

    async def start(self):
        server = await asyncio.start_server(
            self.handle_connection, self.config.host, self.config.port
        )
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader: StreamReader, writer: StreamWriter):
        peer = writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connected with client: {peer}")

        try:
            async for requests in self.readlines(reader):
                if not requests:
                    break
                logging.info(
                    f"{self.config.mode.value}:Received request\r\n>> {requests}\r\n"
                )
                commands = RESPDecoder.decode(requests)
                for request, cmd, *args in commands:
                    cmd_obj = CommandsRunner.create(self.config, cmd, *args)
                    response = CommandsRunner.run(cmd_obj, *args)
                    if cmd_obj.is_replica:
                        self.config.replicas.append((reader, writer))
                        logging.info(f"{self.config.mode.value}:Replica added")
                    if cmd_obj.is_propagated:
                        await self.propagate(request)
                    logging.info(
                        f"{self.config.mode.value}:Send response\r\n>> {response}\r\n"
                    )
                    await self.write(writer, response)
        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {peer}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(
                f"{self.config.mode.value}:Connection closed with client: {peer}"
            )

    async def propagate(self, request: bytes) -> None:
        logging.info(f"{self.config.mode.value}:Propagated\r\n>> {request}\r\n")
        for _, writer in self.config.replicas:
            await self.write(writer, request)


@dataclass
class RedisSlave(RedisMaster):
    async def start(self):
        await self.connect_to_master()
        asyncio.create_task(self.send_handshake())

        await super().start()

    async def connect_to_master(self):
        self.master_reader, self.master_writer = await asyncio.open_connection(
            self.config.master_host, self.config.master_port
        )

    async def send_handshake(self):
        handshake = [
            CommandBuilder.build("PING"),
            CommandBuilder.build("REPLCONF", "listening-port", str(self.config.port)),
            CommandBuilder.build("REPLCONF", "capa", "psync2"),
            CommandBuilder.build("PSYNC", "?", "-1"),
        ]
        for command in handshake:
            await self.send_command(command)

        # read for rdb
        res = await self.master_reader.readuntil(constants.BTERM)
        await self.master_reader.readexactly(int(res[1:-2]))
        logging.info(f"{self.config.mode.value}:Handshake completed")

        await self.receive_commands()

    async def send_command(self, command):
        logging.info(f"{self.config.mode.value}:Send command\r\n>> {command}\r\n")
        await self.write(self.master_writer, command)
        response = await self.master_reader.readuntil(constants.BTERM)
        logging.info(f"{self.config.mode.value}:Received response\r\n>> {response}\r\n")

    async def receive_commands(self):
        peer = self.master_writer.get_extra_info("peername")
        logging.info(f"{self.config.mode.value}:Connection with master: {peer}")

        try:
            async for requests in self.readlines(self.master_reader):
                if not requests:
                    break
                logging.info(
                    f"{self.config.mode.value}:Received master request\r\n>> {requests}\r\n"
                )
                commands = RESPDecoder.decode(requests)
                for request, cmd, *args in commands:
                    cmd_obj = CommandsRunner.create(self.config, cmd, *args)
                    response = CommandsRunner.run(cmd_obj, *args)
                    self.config.offset += len(request)

                    if cmd_obj.is_server_answer:
                        logging.info(
                            f"{self.config.mode.value}:Send response\r\n>> {response}\r\n"
                        )
                        await self.write(self.master_writer, response)
        except ConnectionResetError:
            logging.error(f"{self.config.mode.value}:Connection reset by peer: {peer}")
