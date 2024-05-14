import time
from enum import Enum

from app.constants import EMPTY_RDB
from app.dataclasses import ServerConfig
from app.encoder import RESPEncoder
from app.utils import read_db

CACHE = {}
PXS = {}

# TODO: make active cache, when add to cache, create task which will pop from cache when time ends


class Command(str, Enum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    INFO = "INFO"
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"

    @staticmethod
    def is_propagated(cmd: str):
        return cmd in set([Command.SET.value])


# TODO: Maybe rename as response to, or send_ping, respond_ping
class CommandRunner:
    @staticmethod
    def run(command: str, config: ServerConfig, *args):
        match command:
            case Command.PING.value:
                return [CommandRunner.ping()]
            case Command.ECHO.value:
                return [CommandRunner.echo(*args)]
            case Command.GET.value:
                return [CommandRunner.get(*args)]
            case Command.SET.value:
                return [CommandRunner.set(*args)]
            case Command.INFO.value:
                return [CommandRunner.info(config, *args)]
            case Command.REPLCONF.value:
                if "GETACK" in args:
                    return [CommandRunner.getack(*args)]
                return [CommandRunner.replconf(*args)]
            case Command.PSYNC.value:
                return [
                    CommandRunner.psync(config, *args),
                    CommandRunner.rdb_file(*args),
                ]
            case _:
                return [CommandRunner.unknown()]

    def unknown():
        return RESPEncoder.error("Unknown command")

    @staticmethod
    def ping():
        return RESPEncoder.simple_string("PONG")

    @staticmethod
    def echo(*args):
        if len(args) == 0:
            return RESPEncoder.error("ECHO command requires a value")
        value = args[0]
        return RESPEncoder.bulk_string(value)

    @staticmethod
    def get(*args):
        if len(args) == 0:
            return RESPEncoder.error("GET command requires a key")

        key = args[0]
        px = PXS.get(key, float("inf"))
        if time.time() * 1000 > px:
            CACHE.pop(key, None)
            PXS.pop(key, None)
            return RESPEncoder.null_bulk_string()

        value = CACHE.get(key)
        if value is None:
            return RESPEncoder.null_bulk_string()

        return RESPEncoder.bulk_string(value)

    @staticmethod
    def set(*args):
        if len(args) < 2:
            return RESPEncoder.error("SET command requires a key and value")
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return RESPEncoder.error(
                    "SET command with PX requires a valid expiration time"
                )
            px = time.time() * 1000 + int(args[3])
        CACHE[key] = value
        PXS[key] = px

        return RESPEncoder.simple_string("OK")

    @staticmethod
    def info(config: ServerConfig, *args):
        if len(args) == 0:
            return RESPEncoder.error("INFO command requires an argument")

        info = [
            f"role:{config.mode.value}",
            f"master_replid:{config.master_replid}",
            f"master_repl_offset:{config.master_repl_offset}",
        ]
        return RESPEncoder.bulk_string(",".join(info))

    @staticmethod
    def replconf(*args):
        return RESPEncoder.simple_string("OK")

    @staticmethod
    def getack(*args):
        response = ["REPLCONF", "ACK", str(args[0])]
        return RESPEncoder.array([RESPEncoder.bulk_string(a) for a in response])

    @staticmethod
    def psync(config: ServerConfig, *args):
        return RESPEncoder.simple_string(f"FULLRESYNC {config.master_replid} 0")

    @staticmethod
    def rdb_file(*args):
        data = read_db(EMPTY_RDB)
        return RESPEncoder.rdb_file(data)


class CommandBuilder:
    @staticmethod
    def build(*args):
        return RESPEncoder.array([RESPEncoder.bulk_string(a) for a in args])
