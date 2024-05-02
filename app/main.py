import asyncio
from enum import Enum


PONG = "+PONG\r\n"


class Byte(Enum):
    ARRAY = "*"
    STRING = "+"


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"


# *2\r\n$4\r\necho\r\n$3\r\nhey\r\n
# echo -ne "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n" | nc localhost 6379
class RedisProtocol:
    def parse(self, request: str):
        return request.split("\r\n")
    

class Handlers:
    @staticmethod
    def ping(args):
        return PONG
    
    @staticmethod
    def echo(args):
        response = []
        
        for arg in args:
            response.append(f"${len(arg)}")
            response.append(arg)
        response.append("")

        return "\r\n".join(response)
    

HANDLERS_MAP = {
    Command.PING: Handlers.ping,
    Command.ECHO: Handlers.echo,
}


async def main():    
    print("Logs from your program will appear here!")
    
    server = await asyncio.start_server(handle_client, host="localhost", port=6379)
    await server.serve_forever()
    

async def handle_client(client_reader, client_writer):
    redis_protocol = RedisProtocol()
    
    while request := await client_reader.read(1024):
        
        args = redis_protocol.parse(request.decode("utf-8"))
        cmd = args[2].lower()
        response = ""
        match cmd:
            case "ping":
                response = PONG
            case "echo":
                response = Handlers.echo([args[4]])

        client_writer.write(response.encode())
        
        await client_writer.drain()
    
    client_writer.close()


if __name__ == "__main__":
    asyncio.run(main())
