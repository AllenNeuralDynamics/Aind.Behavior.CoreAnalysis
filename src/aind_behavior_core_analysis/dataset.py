from dataclasses import dataclass

from aind_behavior_core_analysis.base import DataStreamGroup


@dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStreamGroup
