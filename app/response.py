from enum import Enum

from app.dataclasses import Response


class RESP(str, Enum):
    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL_BUK_STRING = "_"
    BOOLEAN = "#"
    DOUBLE = ","


class RedisResponse:
    @staticmethod
    def encode(data: Response) -> str:
        match data.type:
            case RESP.SIMPLE_STRING:
                return f"{RESP.SIMPLE_STRING.value}{data.data}\r\n"
            case RESP.ERROR:
                return f"{RESP.ERROR.value}{data.data}\r\n"
            case RESP.INTEGER:
                return f"{RESP.INTEGER.value}{data.data}\r\n"
            case RESP.BULK_STRING:
                return f"{RESP.BULK_STRING.value}{len(data.data)}\r\n{data.data}\r\n"
            case RESP.ARRAY:
                result = f"{RESP.ARRAY.value}{len(data.data)}\r\n"
                for item in data.data:
                    result += RedisResponse.encode(item)
                return result
            case RESP.NULL_BUK_STRING:
                return "$-1\r\n"
            case RESP.BOOLEAN:
                return f"{RESP.BOOLEAN.value}{data.data}\r\n"
            case RESP.DOUBLE:
                return f"{RESP.DOUBLE.value}{data.data}\r\n"
