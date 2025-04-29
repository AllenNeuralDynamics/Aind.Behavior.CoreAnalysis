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
from aind_behavior_core_analysis.utils import load_branch

dataset_root = Path(r"path/to/dataset/root")
my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=StaticDataStreamGroup(
        name="Dataset",
        description="Root of the dataset",
        data_streams=[
            StaticDataStreamGroup(
                name="Behavior",
                description="Data from the Behavior modality",
                data_streams=[
                    DataStreamGroup(
                        name="HarpBehavior",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/Behavior.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpManipulator",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/StepperDriver.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpTreadmill",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/Treadmill.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpOlfactometer",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/Olfactometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpSniffDetector",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/SniffDetector.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpLickometer",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/Lickometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpClockGenerator",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/ClockGenerator.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamGroup(
                        name="HarpEnvironmentSensor",
                        reader=harp_device_reader,
                        reader_params=HarpDeviceReaderParams(
                            path=dataset_root / "behavior/EnvironmentSensor.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    StaticDataStreamGroup(
                        name="HarpCommands",
                        description="Commands sent to Harp devices",
                        data_streams=[
                            DataStreamGroup(
                                name="HarpBehavior",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/Behavior.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpManipulator",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/StepperDriver.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpTreadmill",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/Treadmill.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpOlfactometer",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/Olfactometer.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpSniffDetector",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/SniffDetector.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpLickometer",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/Lickometer.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpClockGenerator",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/ClockGenerator.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            DataStreamGroup(
                                name="HarpEnvironmentSensor",
                                reader=harp_device_reader,
                                reader_params=HarpDeviceReaderParams(
                                    path=dataset_root / "behavior/HarpCommands/EnvironmentSensor.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                        ],
                    ),
                    DataStreamGroup(
                        name="SoftwareEvents",
                        description="Software events generated by the workflow. The timestamps of these events are low precision and should not be used to align to physiology data.",
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
                    DataStreamGroup(
                        name="OperationControl",
                        description="Events related with conditions and task logic computed online.",
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
                    DataStream(
                        name="RendererSynchState",
                        description="Contains information that allows the post-hoc alignment of visual stimuli to the behavior data.",
                        reader=csv_reader,
                        reader_params=CsvReaderParams(
                            path=dataset_root / "behavior/Renderer/RendererSynchState.csv",
                            strict_header=True,
                            delimiter=",",
                        ),
                    ),
                    DataStreamGroup(
                        name="UpdaterEvents",
                        description="Events generated when one of the updaters is triggered in the task logic.",
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
                    StaticDataStreamGroup(
                        name="Logs",
                        data_streams=[
                            DataStream(
                                name="Launcher",
                                description="Contains the console log of the launcher process.",
                                reader=text_reader,
                                reader_params=TextReaderParams(
                                    path=dataset_root / "behavior/Logs/launcher.log",
                                ),
                            ),
                            DataStream(
                                name="EndSession",
                                description="A file that determines the end of the session. If the file is empty, the session is still running or it was not closed properly.",
                                reader=pydantic_model_reader,
                                reader_params=(
                                    PydanticModelReaderParams(
                                        path=dataset_root / "behavior/Logs/EndSession.json",
                                        model=SoftwareEvent,
                                    )
                                ),
                            ),
                        ],
                    ),
                    StaticDataStreamGroup(
                        name="InputSchemas",
                        description="Configuration files for the behavior rig, task_logic and session.",
                        data_streams=[
                            DataStream(
                                name="rig",
                                reader=pydantic_model_reader,
                                reader_params=PydanticModelReaderParams(
                                    model=AindBehaviorRigModel,
                                    path=dataset_root / "behavior/Logs/rig_input.json",
                                ),
                            ),
                            DataStream(
                                name="task_logic",
                                reader=pydantic_model_reader,
                                reader_params=PydanticModelReaderParams(
                                    model=AindBehaviorTaskLogicModel,
                                    path=dataset_root / "behavior/Logs/tasklogic_input.json",
                                ),
                            ),
                            DataStream(
                                name="session",
                                reader=pydantic_model_reader,
                                reader_params=PydanticModelReaderParams(
                                    model=AindBehaviorSessionModel,
                                    path=dataset_root / "behavior/Logs/session_input.json",
                                ),
                            ),
                        ],
                    ),
                ],
            ),
        ],
    ),
)


print(my_dataset.data_streams.at("Behavior").at("HarpManipulator").load().at("WhoAmI").load().data)

exc = load_branch(my_dataset.data_streams)

for e in exc if exc is not None else []:
    print(f"Stream: {e[0]}")
    print(f"Exception: {e[1]}")
    print()

print(my_dataset.data_streams.at("Behavior").at("HarpBehavior").at("WhoAmI").read())

print(my_dataset.data_streams.at("Behavior").at("HarpCommands").at("HarpBehavior").at("OutputSet").read())
print(my_dataset.data_streams.at("Behavior").at("SoftwareEvents"))
print(my_dataset.data_streams.at("Behavior").at("SoftwareEvents").at("DepletionVariable").read())

print(my_dataset.data_streams.at("Behavior").at("OperationControl").at("IsStopped").data)
print(my_dataset.data_streams.at("Behavior").at("RendererSynchState").data)

print(my_dataset.data_streams.at("Behavior").at("InputSchemas").at("session").data)
# my_dataset.print()

with open("my_dataset.txt", "w", encoding="UTF-8") as f:
    f.write(my_dataset.tree(exclude_params=True))
