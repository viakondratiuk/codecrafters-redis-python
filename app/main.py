import argparse
import asyncio
import logging

from app import constants
from app.dataclasses import Address, Mode, ServerConfig
from app.redis import RedisMaster, RedisSlave

logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser(description="Read CLI parameters.")
    parser.add_argument(
        "--port",
        type=int,
        default=constants.DEFAULT_PORT,
        help=f"The port number to use. Defaults to {constants.DEFAULT_PORT}.",
    )
    parser.add_argument(
        "--replicaof",
        # nargs=2,
        # metavar=("master_host", "master_port"),
        type=str,
        help="Specifies the host and port of the master server.",
    )

    return parser.parse_args()


async def main():
    args = parse_args()

    address = Address(host="localhost", port=int(args.port))
    config = ServerConfig(addr=address)
    if args.replicaof is not None:
        config.mode = Mode.SLAVE
        # host, port = (args.replicaof[0], args.replicaof[1])
        host, port = args.replicaof.split(" ")
        config.master_addr = Address(host=host, port=int(port))

    server = RedisMaster(config) if args.replicaof is None else RedisSlave(config)

    logging.info(f"Server is starting on {config.addr.host}:{config.addr.port}")
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
