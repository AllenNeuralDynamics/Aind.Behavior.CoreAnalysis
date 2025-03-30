from dataclasses import dataclass
from typing import Generator, Mapping, Union
from collections import UserDict

from .base import DataStream


class Node(UserDict[str, Union[DataStream, "Node"]]):
    """Mapping that represents a node of the dataset."""

    def __getattr__(self, key: str) -> Union[DataStream, "Node"]:
        """Allow dot notation access."""
        if key in self.__dict__:
            return self.__dict__[key]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"

    def __str__(self):
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"

    def walk_data_streams(self) -> Generator[DataStream, None, None]:
        """Walk through the dataset tree and yield data streams."""
        for value in self.__dict__.values():
            if isinstance(value, DataStream):
                yield value
            elif isinstance(value, Node):
                yield from value.walk_data_streams()


@dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: Node
