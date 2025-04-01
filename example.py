from aind_behavior_core_analysis import Dataset, DataStream, DataStreamGroup
from aind_behavior_core_analysis.csv import CsvReaderParams, CsvWriterParams, csv_reader, csv_writer
from aind_behavior_core_analysis.harp import DeviceYmlByRegister0, HarpDeviceReaderParams, harp_device_reader
from aind_behavior_core_analysis.json import JsonReaderParams, JsonWriterParams, json_reader, json_writer

my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=DataStreamGroup.group(
        {
            "foo": DataStream(
                reader=csv_reader,
                writer=csv_writer,
                reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                writer_params=CsvWriterParams(path=r"C:\Users\bruno.cruz\Downloads\foo_write.csv"),
            ),
            "bar": DataStream(
                reader=json_reader,
                writer=json_writer,
                reader_params=JsonReaderParams(path=r"C:\Users\bruno.cruz\Downloads\test.json"),
                writer_params=JsonWriterParams(path=r"C:\Users\bruno.cruz\Downloads\test_wr.json"),
            ),
            "custom_node": DataStreamGroup.group(
                {
                    "baz": DataStream(
                        reader=csv_reader,
                        reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                    )
                }
            ),
            "HarpBehavior": DataStreamGroup(
                reader=harp_device_reader,
                reader_params=HarpDeviceReaderParams(
                    path=r"C:\Users\bruno.cruz\Desktop\0123456789_2025-01-28T023311Z\behavior\Behavior.harp",
                    device_yml_hint=DeviceYmlByRegister0(),
                ),
            ),
        }
    ),
)


my_dataset.print()
