from enum import Enum

from app.constants import TERM


class RESP(str, Enum):
    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL_BUK_STRING = "_"
    BOOLEAN = "#"
    DOUBLE = ","
    RDB_FILE = "$"


class RESPEncoder:
    @staticmethod
    def bulk_string(value: str):
        return f"{RESP.BULK_STRING.value}{len(value)}{TERM}{value}{TERM}".encode()

    @staticmethod
    def simple_string(value: str):
        return f"{RESP.SIMPLE_STRING.value}{value}{TERM}".encode()

    @staticmethod
    def error(value: str):
        return f"{RESP.ERROR.value}{value}{TERM}".encode()

    @staticmethod
    def integer(value: int):
        return f"{RESP.INTEGER.value}{value}{TERM}".encode()

    @staticmethod
    def boolean(value: bool):
        return f"{RESP.BOOLEAN.value}{value}{TERM}".encode()

    @staticmethod
    def double(value: float):
        return f"{RESP.DOUBLE.value}{value}{TERM}".encode()

    @staticmethod
    def array(items: list[bytes]):
        result = f"{RESP.ARRAY.value}{len(items)}{TERM}".encode()
        result += b"".join(items)
        return result

    @staticmethod
    def null_bulk_string():
        return f"$-1{TERM}".encode()

    @staticmethod
    def rdb_file(value: bytes):
        return f"{RESP.RDB_FILE.value}{len(value)}{TERM}".encode() + value
