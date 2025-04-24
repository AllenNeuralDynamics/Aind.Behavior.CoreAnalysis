from pathlib import Path

from aind_behavior_services.data_types import SoftwareEvent

from aind_behavior_core_analysis import Dataset, DataStreamGroup, StaticDataStreamGroup
from aind_behavior_core_analysis.harp import (
    DeviceYmlByFile,
    HarpDeviceReaderParams,
    harp_device_reader,
)
from aind_behavior_core_analysis.json import (
    MultiLinePydanticModelDfReaderParams,
    multi_line_pydantic_model_df_reader,
)
from aind_behavior_core_analysis.mux import MuxReaderParams, file_pattern_mux_reader

dataset_root = Path(r"path/to/dataset/root")
my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=StaticDataStreamGroup(
        {
            "HarpBehavior": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/Behavior.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpManipulator": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/StepperDriver.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpTreadmill": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/Treadmill.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpOlfactometer": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/Olfactometer.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpSniffDetector": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/SniffDetector.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpLickometer": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/Lickometer.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpClockGenerator": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/ClockGenerator.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpEnvironmentSensor": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=dataset_root / "behavior/EnvironmentSensor.harp",
                    device_yml_hint=DeviceYmlByFile(),
                ),
            ),
            "HarpCommands": StaticDataStreamGroup(
                {
                    "HarpBehavior": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/Behavior.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpManipulator": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/StepperDriver.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpTreadmill": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/Treadmill.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpOlfactometer": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/Olfactometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpSniffDetector": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/SniffDetector.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpLickometer": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/Lickometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpClockGenerator": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/ClockGenerator.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    "HarpEnvironmentSensor": DataStreamGroup(
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/HarpCommands/EnvironmentSensor.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                }
            ),
            "SoftwareEvents": DataStreamGroup(
                reader=file_pattern_mux_reader,
                reader_params=MuxReaderParams(
                    path=dataset_root / "behavior/SoftwareEvents",
                    glob_pattern=["*.json"],
                    inner_reader=multi_line_pydantic_model_df_reader,
                    inner_reader_params=MultiLinePydanticModelDfReaderParams(
                        path="",
                        model=SoftwareEvent,
                        index="timestamp",
                    ),
                ),
            ),
        }
    ),
)


print(my_dataset.data_streams.get_stream("HarpManipulator").load().get_stream("WhoAmI").load().data)

exc = my_dataset.data_streams.load_branch()

for e in exc if exc is not None else []:
    print(f"Stream: {e[0]}")
    print(f"Exception: {e[2]}")
    print()

my_dataset.data_streams.get_stream("HarpBehavior").load()
print(my_dataset.data_streams.get_stream("HarpBehavior").get_stream("WhoAmI").read())

my_dataset.data_streams.get_stream("HarpCommands").load_branch()
print(my_dataset.data_streams.get_stream("HarpCommands").get_stream("HarpBehavior").get_stream("OutputSet").read())

my_dataset.data_streams.get_stream("SoftwareEvents").load()
my_dataset.data_streams.get_stream("SoftwareEvents").get_stream("Block").load()
print(my_dataset.data_streams.get_stream("SoftwareEvents"))
print(my_dataset.data_streams.get_stream("SoftwareEvents").get_stream("DepletionVariable").read())

# my_dataset.print()
