from abc import ABC, abstractmethod


class DataStorePort(ABC):
    @abstractmethod
    def get(self, key: str) -> str:
        pass

    @abstractmethod
    def set(self, key: str, value: str, px: int) -> None:
        pass

    @abstractmethod
    def pop(self, key: str) -> None:
        pass


class DataStore(DataStorePort):
    def __init__(self):
        self._data: dict[str, (str, str)] = {}

    def get(self, key: str) -> str:
        return self._data.get(key, (None, float("inf")))

    def set(self, key: str, value: str, px: int = float("inf")) -> None:
        self._data[key] = (value, px)

    def pop(self, key: str) -> None:
        self._data.pop(key, None)

    def __str__(self) -> str:
        return str(self._data)
