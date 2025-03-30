from dataclasses import dataclass
from typing import Generator, Mapping, Self, TypeVar, Union
from .base import DataStream


class DatasetNode(Mapping[str, Union[DataStream, "DatasetNode"]]):
    """Mapping that represents a node of the dataset."""

    def __init__(self, **kwargs: Union[DataStream, "DatasetNode"]) -> None:
        self.__dict__.update(kwargs)

    def __getitem__(self, key: str) -> Union[DataStream, "DatasetNode"]:
        return self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__.values())

    def __len__(self) -> int:
        return len(self.__dict__)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"

    def __str__(self):
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"

    def walk_data_streams(self) -> Generator[DataStream, None, None]:
        """Walk through the dataset tree and yield data streams."""
        for value in self.__dict__.values():
            if isinstance(value, DataStream):
                yield value
            elif isinstance(value, DatasetNode):
                yield from value.walk_data_streams()


@dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DatasetNode
