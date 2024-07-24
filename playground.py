from dataclasses import dataclass
from typing import Dict, Union
from pathlib import Path


from aind_behavior_core_analysis.io.data_stream import (
    HarpDataStreamSourceBuilder,
    CsvStream,
    SoftwareEventStream,
    SingletonStream,
)
from aind_behavior_core_analysis.io._core import DataStreamSource, DataStreamBuilderPattern


NodeType = Union[Dict[str, DataStreamSource], DataStreamSource]


@dataclass
class DataContract:
    behavior: Dict[str, NodeType]
    behavior_videos: Dict[str, NodeType]
    config: Dict[str, NodeType]


root_path = Path(r"C:\Users\bruno.cruz\OneDrive - Allen Institute\data_demos\716455_20240719T111034")


def video_folder_parser(root_path: Path):
    return {camera for camera in root_path.glob("behavior_videos/*")}


dataset = DataContract(
    behavior={
        "Behavior": DataStreamSource(
            path=root_path / "behavior" / "Behavior.harp",
            builder=HarpDataStreamSourceBuilder(default_inference_mode="register_0"),
        ),
        "SniffDetector": DataStreamSource(
            path=root_path / "behavior" / "SniffDetector.harp",
            builder=HarpDataStreamSourceBuilder(default_inference_mode="register_0"),
        ),
        "Treadmill": DataStreamSource(
            path=root_path / "behavior" / "Treadmill.harp",
            builder=HarpDataStreamSourceBuilder(default_inference_mode="register_0"),
        ),
        "Lickometer": DataStreamSource(
            path=root_path / "behavior" / "Lickometer.harp",
            builder=HarpDataStreamSourceBuilder(default_inference_mode="register_0"),
        ),
        "Olfactometer": DataStreamSource(
            path=root_path / "behavior" / "Olfactometer.harp",
            builder=HarpDataStreamSourceBuilder(default_inference_mode="register_0"),
        ),
        "OperationControl": DataStreamSource(
            path=root_path / "behavior" / "OperationControl",
            builder=DataStreamBuilderPattern(stream_type=CsvStream, pattern="*.csv"),
        ),
        "RendererSynchState": DataStreamSource(
            path=root_path / "behavior" / "Renderer",
            name="SynchState",
            builder=DataStreamBuilderPattern(stream_type=CsvStream, pattern="RendererSynchState.csv"),
        ),
        "SoftwareEvents": DataStreamSource(
            path=root_path / "behavior" / "SoftwareEvents",
            builder=DataStreamBuilderPattern(stream_type=SoftwareEventStream, pattern="*.json"),
        ),
    },
    behavior_videos={
        camera.name: DataStreamSource(
            path=camera, builder=DataStreamBuilderPattern(stream_type=CsvStream, pattern="metadata.csv")
        )
        for camera in (root_path / "behavior-videos").iterdir()
    },
    config={
        "rig": DataStreamSource(
            path=root_path / "other" / "Config",
            builder=DataStreamBuilderPattern(SingletonStream, pattern="rig_input.json"),
        ),
        "task_logic": DataStreamSource(
            path=root_path / "other" / "Config",
            builder=DataStreamBuilderPattern(SingletonStream, pattern="task_logic_input.json"),
        ),
        "session": DataStreamSource(
            path=root_path / "other" / "Config",
            builder=DataStreamBuilderPattern(SingletonStream, pattern="session_input.json"),
        ),
    },
)
