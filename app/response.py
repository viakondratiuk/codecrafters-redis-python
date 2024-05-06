from enum import Enum

from app.dataclasses import Result


class RESP(str, Enum):
    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL_BUK_STRING = "_"
    BOOLEAN = "#"
    DOUBLE = ","


class Response:
    @staticmethod
    def encode(result: Result) -> str:
        match result.type:
            case RESP.SIMPLE_STRING:
                return f"{RESP.SIMPLE_STRING.value}{result.data}\r\n"
            case RESP.ERROR:
                return f"{RESP.ERROR.value}{result.data}\r\n"
            case RESP.INTEGER:
                return f"{RESP.INTEGER.value}{result.data}\r\n"
            case RESP.BULK_STRING:
                return (
                    f"{RESP.BULK_STRING.value}{len(result.data)}\r\n{result.data}\r\n"
                )
            case RESP.ARRAY:
                out = f"{RESP.ARRAY.value}{len(result.data)}\r\n"
                for item in result.data:
                    out += Response.encode(item)
                return out
            case RESP.NULL_BUK_STRING:
                return "$-1\r\n"
            case RESP.BOOLEAN:
                return f"{RESP.BOOLEAN.value}{result.data}\r\n"
            case RESP.DOUBLE:
                return f"{RESP.DOUBLE.value}{result.data}\r\n"
