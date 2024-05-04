import time
from abc import ABC, abstractmethod
from enum import Enum

from app.response import Response

MEMO = {}
PXS = {}


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"


class CommandStrategy(ABC):
    @abstractmethod
    def execute(self, args):
        pass


class PingCommand(CommandStrategy):
    def execute(self, args):
        return Response.ok("PONG")


class EchoCommand(CommandStrategy):
    def execute(self, args):
        if len(args) < 2:
            return Response.error("ECHO command requires a value")
        return Response.data(args[1])


class SetCommand(CommandStrategy):
    def execute(self, args):
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


class GetCommand(CommandStrategy):
    def execute(self, args):
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


class InfoCommand(CommandStrategy):
    def execute(self, args):
        if len(args) < 2:
            return Response.error("INFO command requires an argument")
        return Response.data("role:master")


class UnknownCommand(CommandStrategy):
    def execute(self, args):
        return Response.error("Unknown command")


class CommandContext:
    def __init__(self, command):
        self.strategies = {
            Command.PING.value: PingCommand(),
            Command.ECHO.value: EchoCommand(),
            Command.SET.value: SetCommand(),
            Command.GET.value: GetCommand(),
            Command.INFO.value: InfoCommand(),
        }
        self.strategy = self.strategies.get(command, UnknownCommand())

    def execute(self, args):
        return self.strategy.execute(args)
