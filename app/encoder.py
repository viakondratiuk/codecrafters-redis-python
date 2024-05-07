from enum import Enum

from app.constants import TERMINATOR


class RESP(str, Enum):
    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL_BUK_STRING = "_"
    BOOLEAN = "#"
    DOUBLE = ","


class Encoder:
    @staticmethod
    def bulk_string(value: str):
        return f"{RESP.BULK_STRING.value}{len(value)}{TERMINATOR}{value}{TERMINATOR}"

    @staticmethod
    def simple_string(value: str):
        return f"{RESP.SIMPLE_STRING.value}{value}{TERMINATOR}"

    @staticmethod
    def error(value: str):
        return f"{RESP.SIMPLE_STRING.value}{value}{TERMINATOR}"

    @staticmethod
    def integer(value: int):
        return f"{RESP.INTEGER.value}{value}{TERMINATOR}"

    @staticmethod
    def boolean(value: bool):
        return f"{RESP.BOOLEAN.value}{value}{TERMINATOR}"

    @staticmethod
    def double(value: float):
        return f"{RESP.DOUBLE.value}{value}{TERMINATOR}"

    @staticmethod
    def array(items: list):
        result = f"{RESP.ARRAY.value}{len(items)}{TERMINATOR}"
        result += "".join(items)
        return result

    @staticmethod
    def null_bulk_string():
        return f"$-1{TERMINATOR}"
