import asyncio
import logging
from enum import Enum


logging.basicConfig(level=logging.INFO)

MEMO = {}


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
    def format(status: str, *messages):
        response = []
        match status:
            case "OK":
                response = ["+" + message for message in messages]
            case "ERR":
                response = ["-" + message for message in messages]
            case "DATA":
                response = [f"${len(message)}\r\n{message}" for message in messages]
            case "NULL":
                response = [f"$-1"]
        response.append("")
        return "\r\n".join(response)
    

def dispatch_command(command, args):
    match command:
        case Command.PING.value:
            return Response.format("OK", "PONG")
        case Command.ECHO.value:
            args.pop(0)
            return Response.format("DATA", *args)
        case Command.SET.value:
            MEMO[args[1]] = args[3]
            return Response.format("OK", "OK")
        case Command.GET.value:
            try:
                value = MEMO[args[1]]
                return Response.format("DATA", *[value])
            except KeyError:
                return Response.format("NULL")
        case _:
            return Response.format("ERR", "Unknown command")
           

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
