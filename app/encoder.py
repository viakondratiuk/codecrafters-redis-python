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
    RDB_FILE = "$"


class Encoder:
    @staticmethod
    def bulk_string(value: str, is_encode: bool = True):
        result = f"{RESP.BULK_STRING.value}{len(value)}{TERMINATOR}{value}{TERMINATOR}"
        if is_encode:
            return result.encode()
        return result

    @staticmethod
    def simple_string(value: str):
        return f"{RESP.SIMPLE_STRING.value}{value}{TERMINATOR}".encode()

    @staticmethod
    def error(value: str):
        return f"{RESP.SIMPLE_STRING.value}{value}{TERMINATOR}".encode()

    @staticmethod
    def integer(value: int):
        return f"{RESP.INTEGER.value}{value}{TERMINATOR}".encode()

    @staticmethod
    def boolean(value: bool):
        return f"{RESP.BOOLEAN.value}{value}{TERMINATOR}".encode()

    @staticmethod
    def double(value: float):
        return f"{RESP.DOUBLE.value}{value}{TERMINATOR}".encode()

    @staticmethod
    def array(items: list):
        result = f"{RESP.ARRAY.value}{len(items)}{TERMINATOR}"
        result += "".join(items)
        return result.encode()

    @staticmethod
    def null_bulk_string():
        return f"$-1{TERMINATOR}".encode()
    
    @staticmethod
    def rdb_file(value: bytes):
        return f"{RESP.RDB_FILE.value}{len(value)}{TERMINATOR}".encode() + value
    
