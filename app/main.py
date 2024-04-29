import asyncio

PONG = b"+PONG\r\n"


async def main():    
    print("Logs from your program will appear here!")
    
    server = await asyncio.start_server(handle_client, host="localhost", port=6379)
    await server.serve_forever()
    

async def handle_client(client_reader, client_writer):
    while True:
        data = await client_reader.read(1024)
        if not data:
            break
        client_writer.write(PONG)
        await client_writer.drain()
    client_writer.close()


if __name__ == "__main__":
    asyncio.run(main())
