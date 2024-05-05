import time
from abc import ABC, abstractmethod
from enum import Enum

from app.dataclasses import ServerConfig
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
    def __init__(self, server_config: ServerConfig):
        self.server_config = server_config

    @abstractmethod
    def execute(self, args):
        pass


class PingCommand(CommandStrategy):
    def execute(self, args):
        return Response.ok("PONG")


class EchoCommand(CommandStrategy):
    def execute(self, args):
        if len(args) == 0:
            return Response.error("ECHO command requires a value")
        return Response.data(args[0])


class SetCommand(CommandStrategy):
    def execute(self, args):
        if len(args) < 2:
            return Response.error("SET command requires a key and value")
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return Response.error(
                    "SET command with PX requires a valid expiration time"
                )
            px = time.time() * 1000 + int(args[3])
        MEMO[key] = value
        PXS[key] = px
        return Response.ok("OK")


class GetCommand(CommandStrategy):
    def execute(self, args):
        if len(args) == 0:
            return Response.error("GET command requires a key")

        key = args[0]
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
        if len(args) == 0:
            return Response.error("INFO command requires an argument")
        return Response.data(f"role:{self.server_config.mode.value}")


class UnknownCommand(CommandStrategy):
    def execute(self, args):
        return Response.error("Unknown command")


class CommandContext:
    def __init__(self, server_config: ServerConfig):
        self.server_config = server_config
        self.strategies = {
            Command.PING.value: PingCommand(server_config),
            Command.ECHO.value: EchoCommand(server_config),
            Command.SET.value: SetCommand(server_config),
            Command.GET.value: GetCommand(server_config),
            Command.INFO.value: InfoCommand(server_config),
        }

    def execute(self, command: Command, args):
        self.strategy = self.strategies.get(command, UnknownCommand(self.server_config))
        return self.strategy.execute(args)
