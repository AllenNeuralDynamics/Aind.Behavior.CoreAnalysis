import os
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from cv2 import CAP_PROP_FRAME_COUNT, CAP_PROP_FRAME_HEIGHT, CAP_PROP_FRAME_WIDTH, VideoCapture

from .base import DataStream, FilePathBaseParam


@dataclass(frozen=True)
class CameraData:
    metadata: pd.DataFrame
    video_path: os.PathLike

    @property
    def has_video(self) -> bool:
        if not (self.video_path is not None and os.path.exists(self.video_path)):
            return False
        # Not sure why this would fail, but I since its a check, lets make sure we catch it
        try:
            with self.as_video_capture() as video:
                return video.isOpened()
        except Exception:
            return False

    @contextmanager
    def as_video_capture(self):
        cap = VideoCapture(str(self.video_path))
        try:
            yield cap
        finally:
            cap.release()

    @property
    def video_frame_count(self) -> int:
        with self.as_video_capture() as video:
            return int(video.get(CAP_PROP_FRAME_COUNT))

    @property
    def video_frame_size(self) -> t.Tuple[int, int]:
        with self.as_video_capture() as video:
            return int(video.get(CAP_PROP_FRAME_WIDTH)), int(video.get(CAP_PROP_FRAME_HEIGHT))


@dataclass
class CameraParams(FilePathBaseParam):
    metadata_name: str = "metadata"
    video_name: str = "video"


class Camera(DataStream[CameraData, CameraParams]):
    @staticmethod
    def _reader(params: CameraParams) -> CameraData:
        # Read the metadata and validate the required columns
        metadata = pd.read_csv(Path(params.path) / (params.metadata_name + ".csv"), header=0)
        required_columns = {"ReferenceTime", "CameraFrameNumber", "CameraFrameTime"}
        if not required_columns.issubset(metadata.columns):
            raise ValueError(f"Metadata is missing required columns: {required_columns - set(metadata.columns)}")
        metadata.set_index("ReferenceTime", inplace=True)

        candidates_path = list(Path(params.path).glob(f"{params.video_name}.*"))
        if len(candidates_path) == 0:
            raise FileNotFoundError(
                f"No video file found with name '{params.video_name}' and any extension in {params.path}"
            )
        else:
            video_path = candidates_path[0]

        return CameraData(metadata=metadata, video_path=video_path)

    make_params = CameraParams
