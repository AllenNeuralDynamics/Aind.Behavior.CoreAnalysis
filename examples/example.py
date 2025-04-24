from pathlib import Path

from aind_behavior_services.data_types import SoftwareEvent
from aind_behavior_services.rig import AindBehaviorRigModel
from aind_behavior_services.session import AindBehaviorSessionModel
from aind_behavior_services.task_logic import AindBehaviorTaskLogicModel

from aind_behavior_core_analysis import Dataset, DataStream, DataStreamGroup, StaticDataStreamGroup
from aind_behavior_core_analysis.csv import CsvReaderParams, csv_reader
from aind_behavior_core_analysis.harp import (
    DeviceYmlByFile,
    HarpDeviceReaderParams,
    harp_device_reader,
)
from aind_behavior_core_analysis.json import (
    MultiLinePydanticModelDfReaderParams,
    PydanticModelReaderParams,
    multi_line_pydantic_model_df_reader,
    pydantic_model_reader,
)
from aind_behavior_core_analysis.mux import MuxReaderParams, file_pattern_mux_reader
from aind_behavior_core_analysis.text import TextReaderParams, text_reader

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
            "OperationControl": DataStreamGroup(
                reader=file_pattern_mux_reader,
                reader_params=MuxReaderParams(
                    path=dataset_root / "behavior/OperationControl",
                    glob_pattern=["*.csv"],
                    inner_reader=csv_reader,
                    inner_reader_params=CsvReaderParams(
                        path="",
                        strict_header=True,
                        delimiter=",",
                        index="Seconds",
                    ),
                ),
            ),
            "Renderer": StaticDataStreamGroup(
                {
                    "RendererSynchState": DataStream(
                        reader=csv_reader,
                        reader_params=CsvReaderParams(
                            path=dataset_root / "behavior/Renderer/RendererSynchState.csv",
                            strict_header=True,
                            delimiter=",",
                        ),
                    )
                }
            ),
            "UpdaterEvents": DataStreamGroup(
                reader=file_pattern_mux_reader,
                reader_params=MuxReaderParams(
                    path=dataset_root / "behavior/UpdaterEvents",
                    glob_pattern=["*.json"],
                    inner_reader=multi_line_pydantic_model_df_reader,
                    inner_reader_params=MultiLinePydanticModelDfReaderParams(
                        path="",
                        model=SoftwareEvent,
                        index="timestamp",
                    ),
                ),
            ),
            "Logs": StaticDataStreamGroup(
                {
                    "Launcher": DataStream(
                        reader=text_reader,
                        reader_params=TextReaderParams(
                            path=dataset_root / "behavior/Logs/launcher.log",
                        ),
                    ),
                    "EndSession": DataStream(
                        reader=pydantic_model_reader,
                        reader_params=(
                            PydanticModelReaderParams(
                                path=dataset_root / "behavior/Logs/EndSession.json",
                                model=SoftwareEvent,
                            )
                        ),
                    ),
                }
            ),
            "InputSchemas": StaticDataStreamGroup(
                {
                    "rig": DataStream(
                        reader=pydantic_model_reader,
                        reader_params=PydanticModelReaderParams(
                            model=AindBehaviorRigModel,
                            path=dataset_root / "behavior/Logs/rig_input.json",
                        ),
                    ),
                    "task_logic": DataStream(
                        reader=pydantic_model_reader,
                        reader_params=PydanticModelReaderParams(
                            model=AindBehaviorTaskLogicModel,
                            path=dataset_root / "behavior/Logs/tasklogic_input.json",
                        ),
                    ),
                    "session": DataStream(
                        reader=pydantic_model_reader,
                        reader_params=PydanticModelReaderParams(
                            model=AindBehaviorSessionModel,
                            path=dataset_root / "behavior/Logs/session_input.json",
                        ),
                    ),
                }
            ),
        }
    ),
)


print(my_dataset.data_streams.at("HarpManipulator").load().at("WhoAmI").load().data)

exc = my_dataset.data_streams.load_branch()

for e in exc if exc is not None else []:
    print(f"Stream: {e[0]}")
    print(f"Exception: {e[2]}")
    print()

print(my_dataset.data_streams.at("HarpBehavior").at("WhoAmI").read())

print(my_dataset.data_streams.at("HarpCommands").at("HarpBehavior").at("OutputSet").read())
print(my_dataset.data_streams.at("SoftwareEvents"))
print(my_dataset.data_streams.at("SoftwareEvents").at("DepletionVariable").read())

print(my_dataset.data_streams.at("OperationControl").at("IsStopped").data)
print(my_dataset.data_streams.at("Renderer").at("RendererSynchState").data)

print(my_dataset.data_streams.at("InputSchemas").at("session").data)
# my_dataset.print()
