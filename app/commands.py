import time
from abc import ABC, abstractmethod
from enum import Enum

from app.dataclasses import Response, ServerConfig
from app.response import RESP

MEMO = {}
PXS = {}


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"


class CommandStrategy(ABC):
    def __init__(self, config: ServerConfig):
        self.config = config

    @abstractmethod
    def execute(self, args):
        pass


class PongCommand(CommandStrategy):
    def execute(self, args):
        return Response("PONG", RESP.SIMPLE_STRING)


class PingCommand(CommandStrategy):
    def execute(self, args):
        return Response([Response("PING", RESP.BULK_STRING)], RESP.ARRAY)


class EchoCommand(CommandStrategy):
    def execute(self, args):
        if len(args) == 0:
            return "ECHO command requires a value", RESP.ERROR
        return Response(args[0], RESP.BULK_STRING)


class SetCommand(CommandStrategy):
    def execute(self, args):
        if len(args) < 2:
            return Response("SET command requires a key and value", RESP.ERROR)
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return Response(
                    "SET command with PX requires a valid expiration time", RESP.ERROR
                )
            px = time.time() * 1000 + int(args[3])
        MEMO[key] = value
        PXS[key] = px
        return Response("OK", RESP.SIMPLE_STRING)


class GetCommand(CommandStrategy):
    def execute(self, args):
        if len(args) == 0:
            return Response("GET command requires a key", RESP.ERROR)

        key = args[0]
        px = PXS.get(key, float("inf"))
        if time.time() * 1000 > px:
            MEMO.pop(key, None)
            PXS.pop(key, None)
            return Response("", RESP.NULL_BUK_STRING)

        value = MEMO.get(key)
        if value is None:
            return Response("", RESP.NULL_BUK_STRING)
        return Response(value, RESP.BULK_STRING)


class InfoCommand(CommandStrategy):
    def execute(self, args):
        if len(args) == 0:
            return Response("INFO command requires an argument", RESP.ERROR)
        info = [
            f"role:{self.config.mode.value}",
            f"master_replid:{self.config.master_replid}",
            f"master_repl_offset:{self.config.master_repl_offset}",
        ]
        return Response(",".join(info), RESP.BULK_STRING)


class UnknownCommand(CommandStrategy):
    def execute(self, args):
        return Response("Unknown command", RESP.ERROR)


class CommandContext:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.strategies = {
            Command.PING.value: PongCommand(config),
            Command.ECHO.value: EchoCommand(config),
            Command.SET.value: SetCommand(config),
            Command.GET.value: GetCommand(config),
            Command.INFO.value: InfoCommand(config),
        }

    def execute(self, command: Command, args):
        self.strategy = self.strategies.get(command, UnknownCommand(self.config))
        return self.strategy.execute(args)
