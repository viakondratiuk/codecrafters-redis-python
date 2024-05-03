import asyncio
import logging
import time
from enum import Enum

logging.basicConfig(level=logging.INFO)

MEMO = {}
PXS = {}


class Byte(Enum):
    ARRAY = "*"
    STRING = "+"


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"


class RedisProtocol:
    def parse(self, request: str):
        lines = request.strip().split("\r\n")
        command = lines[2].lower()
        args = lines[3:]
        return command, args


class Response:
    @staticmethod
    def _format(payload: list):
        return "\r\n".join(payload + [""])

    @staticmethod
    def _build_response(prefix: str, messages: tuple, formatter=lambda x: x):
        payload = [f"{prefix}{formatter(message)}" for message in messages]
        return Response._format(payload)

    @staticmethod
    def ok(*messages):
        return Response._build_response("+", messages)

    @staticmethod
    def error(*messages):
        return Response._build_response("-", messages)

    @staticmethod
    def data(*messages):
        return Response._build_response(
            "$", messages, lambda msg: f"{len(msg)}\r\n{msg}"
        )

    @staticmethod
    def null(*messages):
        return Response._format(["$-1"])


def dispatch_command(command, args):
    match command:
        case Command.PING.value:
            return Response.ok("PONG")

        case Command.ECHO.value:
            if len(args) < 2:
                return Response.error("ECHO command requires a value")
            value = args[1]
            return Response.data(value)

        case Command.SET.value:
            if len(args) < 4:
                return Response.error("SET command requires a key and value")
            key, value = args[1], args[3]
            px = float("inf")
            if len(args) > 4 and args[5] == "PX".lower():
                if len(args) < 8:
                    return Response.error(
                        "SET command with PX requires a valid expiration time"
                    )
                px = time.time() * 1000 + int(args[7])
            MEMO[key] = value
            PXS[key] = px

            return Response.ok("OK")

        case Command.GET.value:
            if len(args) < 2:
                return Response.error("GET command requires a key")

            key = args[1]
            px = PXS.get(key, float("inf"))
            if time.time() * 1000 > px:
                MEMO.pop(key, None)
                PXS.pop(key, None)
                return Response.null()

            value = MEMO.get(key)
            if value is None:
                return Response.null()

            return Response.data(value)

        case _:
            return Response.error("Unknown command")


async def main():
    logging.info("Server is starting up...")

    server = await asyncio.start_server(handle_client, host="localhost", port=6379)
    logging.info("Server started on localhost:6379")
    await server.serve_forever()


async def handle_client(client_reader, client_writer):
    redis_protocol = RedisProtocol()

    while request := await client_reader.read(1024):
        if request:
            command, args = redis_protocol.parse(request.decode("utf-8"))
            response = dispatch_command(command, args)

            client_writer.write(response.encode())
            await client_writer.drain()

    client_writer.close()
    logging.info("Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
