from aind_behavior_core_analysis.base import DataStream
from aind_behavior_core_analysis.csv import CsvBuilder, CsvReaderParams, CsvWriterParams
from aind_behavior_core_analysis.dataset import Dataset, DataStreamGroup
from aind_behavior_core_analysis.harp import DeviceYmlByRegister0, HarpDeviceBuilder, HarpDeviceReaderParams
from aind_behavior_core_analysis.json import JsonBuilder, JsonReaderParams, JsonWriterParams

my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=DataStreamGroup.group(
        {
            "foo": DataStream(
                io=CsvBuilder,
                reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                writer_params=CsvWriterParams(path=r"C:\Users\bruno.cruz\Downloads\foo_write.csv"),
            ),
            "bar": DataStream(
                io=JsonBuilder,
                reader_params=JsonReaderParams(path=r"C:\Users\bruno.cruz\Downloads\test.json"),
                writer_params=JsonWriterParams(path=r"C:\Users\bruno.cruz\Downloads\test_wr.json"),
            ),
            "custom_node": DataStreamGroup.group(
                {
                    "baz": DataStream(
                        io=CsvBuilder,
                        reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                        writer_params=CsvWriterParams(path=r"C:\Users\bruno.cruz\Downloads\baz_write.csv"),
                    )
                }
            ),
            "HarpBehavior": DataStreamGroup(
                io=HarpDeviceBuilder,
                reader_params=HarpDeviceReaderParams(
                    path=r"C:\Users\bruno.cruz\Desktop\0123456789_2025-01-28T023311Z\behavior\Behavior.harp",
                    device_yml_hint=DeviceYmlByRegister0(),
                ),
                writer_params=None,
            ),
        }
    ),
)


my_dataset.print()