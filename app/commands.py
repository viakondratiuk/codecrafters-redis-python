import time
from enum import Enum

from app.dataclasses import Result, ServerConfig
from app.response import RESP

MEMO = {}
PXS = {}


class CommandCode(str, Enum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"
    REPLCONF = "replconf"


class Command:
    @staticmethod
    def run(command: CommandCode, config: ServerConfig, *args):
        match command:
            case CommandCode.PING.value:
                return Command.pong()
            case CommandCode.ECHO.value:
                return Command.echo(*args)
            case CommandCode.GET.value:
                return Command.get(*args)
            case CommandCode.SET.value:
                return Command.set(*args)
            case CommandCode.INFO.value:
                return Command.info(config, *args)
            case CommandCode.REPLCONF.value:
                return Result("OK", RESP.SIMPLE_STRING)
            case _:
                return Command.unknown()

    @staticmethod
    def pong():
        return Result("PONG", RESP.SIMPLE_STRING)

    @staticmethod
    def echo(*args):
        if len(args) == 0:
            return Result("ECHO command requires a value", RESP.ERROR)
        value = args[0]
        return Result(value, RESP.BULK_STRING)

    @staticmethod
    def get(*args):
        if len(args) == 0:
            return Result("GET command requires a key", RESP.ERROR)

        key = args[0]
        px = PXS.get(key, float("inf"))
        if time.time() * 1000 > px:
            MEMO.pop(key, None)
            PXS.pop(key, None)
            return Result("", RESP.NULL_BUK_STRING)

        value = MEMO.get(key)
        if value is None:
            return Result("", RESP.NULL_BUK_STRING)

        return Result(value, RESP.BULK_STRING)

    @staticmethod
    def set(*args):
        if len(args) < 2:
            return Result("SET command requires a key and value", RESP.ERROR)
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return Result(
                    "SET command with PX requires a valid expiration time", RESP.ERROR
                )
            px = time.time() * 1000 + int(args[3])
        MEMO[key] = value
        PXS[key] = px
        return Result("OK", RESP.SIMPLE_STRING)

    @staticmethod
    def info(config: ServerConfig, *args):
        if len(args) == 0:
            return Result("INFO command requires an argument", RESP.ERROR)
        info = [
            f"role:{config.mode.value}",
            f"master_replid:{config.master_replid}",
            f"master_repl_offset:{config.master_repl_offset}",
        ]
        return Result(",".join(info), RESP.BULK_STRING)

    @staticmethod
    def ping():
        return Result([Result("PING", RESP.BULK_STRING)], RESP.ARRAY)

    @staticmethod
    def replconf(key, value):
        return Result(
            [
                Result("REPLCONF", RESP.BULK_STRING),
                Result(key, RESP.BULK_STRING),
                Result(value, RESP.BULK_STRING),
            ],
            RESP.ARRAY,
        )

    def unknown():
        return Result("Unknown command", RESP.ERROR)
