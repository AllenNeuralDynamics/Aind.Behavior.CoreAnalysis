from aind_behavior_services.data_types import SoftwareEvent

from aind_behavior_core_analysis import Dataset, DataStreamGroup
from aind_behavior_core_analysis.harp import DeviceYmlByRegister0, HarpDeviceReaderParams, harp_device_reader
from aind_behavior_core_analysis.json import (
    MultiLinePydanticModelDfReaderParams,
    multi_line_pydantic_model_df_reader,
)
from aind_behavior_core_analysis.mux import MuxReaderParams, file_pattern_mux_reader

my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=DataStreamGroup.group(
        {
            "HarpBehavior": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=r"C:\Users\bruno.cruz\Downloads\789903_2025-04-02T182737Z\behavior\Behavior.harp",
                    device_yml_hint=DeviceYmlByRegister0(),
                ),
            ),
            "SoftwareEvents": DataStreamGroup(
                reader=file_pattern_mux_reader,
                reader_params=MuxReaderParams(
                    path=r"C:\Users\bruno.cruz\Downloads\789903_2025-04-02T182737Z\behavior\SoftwareEvents",
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


my_dataset.data_streams["HarpBehavior"].load()
print(my_dataset.data_streams["HarpBehavior"]["WhoAmI"].read())

my_dataset.data_streams["SoftwareEvents"].load()
my_dataset.data_streams["SoftwareEvents"]["Block"].load()
print(my_dataset.data_streams["SoftwareEvents"])
print(my_dataset.data_streams["SoftwareEvents"]["DepletionVariable"].read())

# my_dataset.print()
