import abc
import os

from pydantic import BaseModel, Field


class FilePathParams(abc.ABC, BaseModel):
    path: os.PathLike = Field(description="Path to the file")
