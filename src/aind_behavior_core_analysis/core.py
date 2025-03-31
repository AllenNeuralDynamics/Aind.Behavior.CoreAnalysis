import os

from pydantic import BaseModel, Field
from typing import Any
from aind_behavior_core_analysis.base import _Writer


class FileReaderParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")


class FileWriterParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")


def EmptyWriter(data: Any, params: Any) -> None:
    return None
