from aind_behavior_services.data_types import SoftwareEvent
from typing import Optional, Any
from os import PathLike
import json
import pandas as pd

from . import DataStream, DataStreamSource, StreamCollection


from aind_behavior_services.data_types import SoftwareEvent, RenderSynchState


class SoftwareEventStream(DataStream):
    """Represents a generic Software event."""

    @staticmethod
    def _reader(path: PathLike) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            events = [SoftwareEvent.model_validate_json(json.loads(line)) for line in lines]
        return events

    def load_from_file(self,
                       path: Optional[PathLike] = None,
                       force_reload: bool = False) -> None:
        """Loads the datastream from a file"""
        force_reload = True if self._data is None else force_reload
        if force_reload:
            if path is None:
                path = self._path
            with open(path, "r") as f:
                self._data = pd.DataFrame(
                    [self._load_single_event(value=event) for event in f.readlines()]
                    )
                self._data.rename(columns={"timestamp": "Seconds"}, inplace=True)
                self._data.set_index("Seconds", inplace=True)

    def json_normalize(self, *args, **kwargs):
        df = pd.concat([
            self.data,
            pd.json_normalize(self._data["data"], args, kwargs).set_index(self.data.index)
            ], axis=1)
        return df

    @classmethod
    def parse(self, value: str, **kwargs) -> pd.DataFrame:
        """Loads the datastream from a value"""
        ds = SoftwareEvent(**kwargs)
        ds._data = pd.DataFrame(
            [SoftwareEvent._load_single_event(value=line) for line in value.split("\n")]
            )
        ds._data.rename(columns={"timestamp": "Seconds"}, inplace=True)
        ds._data.set_index("Seconds", inplace=True)
        return ds
