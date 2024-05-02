import asyncio
import logging
from enum import Enum

# Setup basic logging
logging.basicConfig(level=logging.INFO)

class Byte(Enum):
    ARRAY = "*"
    STRING = "+"


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"


class RedisProtocol:
    def parse(self, request: str):
        lines = request.strip().split("\r\n")
        command = lines[2].lower()
        args = lines[3:]
        return command, args

class Response:
    @staticmethod
    def format_response(status: str, *messages):
        if status == "OK":
            parts = ["+" + message for message in messages]
        elif status == "ERR":
            parts = ["-" + message for message in messages]
        elif status == "DATA":
            messages_ = [messages[1]]
            parts = [f"${len(message)}\r\n{message}" for message in messages_]
        parts.append("")
        return "\r\n".join(parts)

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

def dispatch_command(command, args):
    match command:
        case Command.PING.value:
            return Response.format_response("OK", "PONG")
        case Command.ECHO.value:
            return Response.format_response("DATA", *args)
        case _:
            return Response.format_response("ERR", "Unknown command")

if __name__ == "__main__":
    asyncio.run(main())
