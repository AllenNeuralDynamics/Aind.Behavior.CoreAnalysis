from pathlib import Path

from aind_behavior_services.data_types import SoftwareEvent
from aind_behavior_services.rig import AindBehaviorRigModel
from aind_behavior_services.session import AindBehaviorSessionModel
from aind_behavior_services.task_logic import AindBehaviorTaskLogicModel

from aind_behavior_core_analysis import Dataset, DataStreamCollection
from aind_behavior_core_analysis.csv import Csv
from aind_behavior_core_analysis.harp import (
    DeviceYmlByFile,
    HarpDevice,
)
from aind_behavior_core_analysis.json import (
    MultiLinePydanticModel,
    PydanticModel,
)
from aind_behavior_core_analysis.mux import FromFileMux
from aind_behavior_core_analysis.text import Text
from aind_behavior_core_analysis.utils import load_branch

dataset_root = Path(r"C:\Users\bruno.cruz\Downloads\789924_2025-04-14T175107Z")
my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=DataStreamCollection(
        name="Dataset",
        description="Root of the dataset",
        data_streams=[
            DataStreamCollection(
                name="Behavior",
                description="Data from the Behavior modality",
                data_streams=[
                    HarpDevice(
                        name="HarpBehavior",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/Behavior.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpManipulator",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/StepperDriver.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpTreadmill",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/Treadmill.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpOlfactometer",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/Olfactometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpSniffDetector",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/SniffDetector.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpLickometer",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/Lickometer.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpClockGenerator",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/ClockGenerator.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    HarpDevice(
                        name="HarpEnvironmentSensor",
                        reader_params=HarpDevice.make_params(
                            path=dataset_root / "behavior/EnvironmentSensor.harp",
                            device_yml_hint=DeviceYmlByFile(),
                        ),
                    ),
                    DataStreamCollection(
                        name="HarpCommands",
                        description="Commands sent to Harp devices",
                        data_streams=[
                            HarpDevice(
                                name="HarpBehavior",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/Behavior.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpManipulator",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/StepperDriver.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpTreadmill",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/Treadmill.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpOlfactometer",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/Olfactometer.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpSniffDetector",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/SniffDetector.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpLickometer",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/Lickometer.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpClockGenerator",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/ClockGenerator.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                            HarpDevice(
                                name="HarpEnvironmentSensor",
                                reader_params=HarpDevice.make_params(
                                    path=dataset_root / "behavior/HarpCommands/EnvironmentSensor.harp",
                                    device_yml_hint=DeviceYmlByFile(),
                                ),
                            ),
                        ],
                    ),
                    FromFileMux(
                        name="SoftwareEvents",
                        description="Software events generated by the workflow. The timestamps of these events are low precision and should not be used to align to physiology data.",
                        reader_params=FromFileMux.make_params(path=dataset_root / "behavior/SoftwareEvents"),
                        include_glob_pattern=["*.json"],
                        inner_data_stream=MultiLinePydanticModel,
                        inner_param_factory=lambda p: MultiLinePydanticModel.make_params(
                            path=p,
                            model=SoftwareEvent,
                            index="timestamp",
                        ),
                        as_collection=True,
                    ),
                    FromFileMux(
                        name="OperationControl",
                        description="Events related with conditions and task logic computed online.",
                        reader_params=FromFileMux.make_params(
                            path=dataset_root / "behavior/OperationControl",
                            include_glob_pattern=["*.csv"],
                            inner_data_stream=Csv,
                            inner_param_factory=lambda p: Csv.make_params(
                                path=p,
                                index="Seconds",
                            ),
                            as_collection=True,
                        ),
                    ),
                    Csv(
                        name="RendererSynchState",
                        description="Contains information that allows the post-hoc alignment of visual stimuli to the behavior data.",
                        reader_params=Csv.make_params(path=dataset_root / "behavior/Renderer/RendererSynchState.csv"),
                    ),
                    FromFileMux(
                        name="UpdaterEvents",
                        description="Events generated when one of the updaters is triggered in the task logic.",
                        reader_params=FromFileMux.make_params(
                            path=dataset_root / "behavior/UpdaterEvents",
                            include_glob_pattern=["*.json"],
                            inner_data_stream=MultiLinePydanticModel,
                            inner_param_factory=lambda p: MultiLinePydanticModel.make_params(
                                path=p,
                                model=SoftwareEvent,
                                index="timestamp",
                            ),
                        ),
                    ),
                    DataStreamCollection(
                        name="Logs",
                        data_streams=[
                            Text(
                                name="Launcher",
                                description="Contains the console log of the launcher process.",
                                reader_params=Text.make_params(
                                    path=dataset_root / "behavior/Logs/launcher.log",
                                ),
                            ),
                            MultiLinePydanticModel(
                                name="EndSession",
                                description="A file that determines the end of the session. If the file is empty, the session is still running or it was not closed properly.",
                                reader_params=MultiLinePydanticModel.make_params(
                                    path=dataset_root / "behavior/Logs/EndSession.json",
                                    model=SoftwareEvent,
                                ),
                            ),
                        ],
                    ),
                    DataStreamCollection(
                        name="InputSchemas",
                        description="Configuration files for the behavior rig, task_logic and session.",
                        data_streams=[
                            PydanticModel(
                                name="rig",
                                reader_params=PydanticModel.make_params(
                                    model=AindBehaviorRigModel,
                                    path=dataset_root / "behavior/Logs/rig_input.json",
                                ),
                            ),
                            PydanticModel(
                                name="task_logic",
                                reader_params=PydanticModel.make_params(
                                    model=AindBehaviorTaskLogicModel,
                                    path=dataset_root / "behavior/Logs/tasklogic_input.json",
                                ),
                            ),
                            PydanticModel(
                                name="session",
                                reader_params=PydanticModel.make_params(
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
print(my_dataset.data_streams.at("Behavior").at("SoftwareEvents").at("DepletionVariable"))

print(my_dataset.data_streams.at("Behavior").at("OperationControl").at("IsStopped").data)
print(my_dataset.data_streams.at("Behavior").at("RendererSynchState").data)

print(my_dataset.data_streams.at("Behavior").at("InputSchemas").at("session").data)

# my_dataset.print()

with open("my_dataset.txt", "w", encoding="UTF-8") as f:
    f.write(my_dataset.tree(exclude_params=True))
