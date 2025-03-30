import os

from pydantic import BaseModel, Field


class FileReaderParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")


class FileWriterParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")
