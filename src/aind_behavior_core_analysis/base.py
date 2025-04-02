import abc
import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class FilePathParams(abc.ABC):
    path: os.PathLike
