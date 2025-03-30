from aind_behavior_core_analysis._csv import CsvBuilder, CsvReaderParams, CsvWriterParams
from aind_behavior_core_analysis._json import JsonBuilder, JsonReaderParams, JsonWriterParams
from aind_behavior_core_analysis.base import DataStream
from aind_behavior_core_analysis.dataset import Dataset, Node

my_dataset = Dataset(
    name="my_dataset",
    version="1.0",
    description="My dataset",
    data_streams=Node(
        {
            "foo": DataStream(
                builder=CsvBuilder,
                reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                writer_params=CsvWriterParams(path=r"C:\Users\bruno.cruz\Downloads\foo_write.csv"),
            ),
            "bar": DataStream(
                builder=JsonBuilder,
                reader_params=JsonReaderParams(path=r"C:\Users\bruno.cruz\Downloads\test.json"),
                writer_params=JsonWriterParams(path=r"C:\Users\bruno.cruz\Downloads\test_wr.json"),
            ),
            "custom_node": Node(
                {
                    "baz": DataStream(
                        builder=CsvBuilder,
                        reader_params=CsvReaderParams(path=r"C:\Users\bruno.cruz\Downloads\customers-100.csv"),
                        writer_params=CsvWriterParams(path=r"C:\Users\bruno.cruz\Downloads\baz_write.csv"),
                    )
                }
            ),
        }
    ),
)


def print_data_stream_tree(node, prefix=""):
    """
    Prints the structure of the data streams using emojis.
    :param node: The current node (DatasetNode or DataStream).
    :param prefix: The prefix for the tree structure.
    """
    if isinstance(node, DataStream):
        reader = node.builder._reader.__name__ if node.builder._reader else "None"
        writer = node.builder._writer.__name__ if node.builder._writer else "None"
        reader_params = node.reader_params if reader != "None" else ""
        writer_params = node.writer_params if writer != "None" else ""
        print(
            f"{prefix}📄 "
            + f"{reader}: \n{prefix}   <{reader_params}>\n{prefix}---->\n{prefix}   {writer}:\n {prefix}   <{writer_params}>"
        )
    elif isinstance(node, Node):
        for key, child in node.items():
            print(f"{prefix}📂 {key}")
            print_data_stream_tree(child, prefix + "    ")


print_data_stream_tree(my_dataset.data_streams)
