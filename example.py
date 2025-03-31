from aind_behavior_core_analysis._csv import CsvBuilder, CsvReaderParams, CsvWriterParams
from aind_behavior_core_analysis._json import JsonBuilder, JsonReaderParams, JsonWriterParams
from aind_behavior_core_analysis.base import DataStream
from aind_behavior_core_analysis.dataset import Dataset, DataStreamGroup

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
        }
    ),
)


def print_data_stream_tree(node, prefix="", *, exclude_params: bool = False):
    """
    Prints the structure of the data streams using emojis.
    :param node: The current node (DatasetNode or DataStream).
    :param prefix: The prefix for the tree structure.
    """

    icon_map = {
        DataStream: "📄",
        DataStreamGroup: "📂",
    }

    if isinstance(node, DataStream):
        reader = node.io._reader.__name__ if node.io._reader else "None"
        writer = node.io._writer.__name__ if node.io._writer else "None"
        reader_params = node.reader_params if reader != "None" else ""
        writer_params = node.writer_params if writer != "None" else ""
        if exclude_params:
            _str = f"{prefix}⬇️  {reader} \n" + f"{prefix}⬆️  {writer}\n"
        else:
            _str = (
                f"{prefix}⬇️  {reader}: \n"
                + f"{prefix}   <{reader_params}>\n"
                + f"{prefix}⬆️  {writer}:\n"
                + f"{prefix}   <{writer_params}>"
            )
        print(_str)
    elif isinstance(node, DataStreamGroup):
        for key, child in node.data_streams.items():
            print(f"{prefix}{icon_map[type(child)]} {key}")
            print_data_stream_tree(child, prefix + "    ", exclude_params=exclude_params)


print_data_stream_tree(my_dataset.data_streams, exclude_params=True)
