from dataclasses import dataclass
from typing import Literal

from aind_behavior_core_analysis.base import DataStream, DataStreamGroup, _is_unset


@dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStreamGroup

    def print(self, exclude_params: bool = False, print_if_none: bool = False) -> None:
        """
        Prints the structure of the dataset using emojis.
        :param exclude_params: If True, excludes the parameters from the output.
        :param print_if_none: If True, prints "None" for None values.
        """
        print_data_stream_tree(self.data_streams, exclude_params=exclude_params, print_if_none=print_if_none)


def print_data_stream_tree(
    node: DataStreamGroup | DataStream, prefix="", *, exclude_params: bool = False, print_if_none: bool = False
) -> None:
    """
    Prints the structure of the data streams using emojis.
    :param node: The current node (DatasetNode or DataStream).
    :param prefix: The prefix for the tree structure.
    """

    icon_map = {
        DataStream: "📄",
        DataStreamGroup: "📂",
        None: "❓",
        "reader": "⬇️",
        "writer": "⬆️",
    }

    def _print_io(
        reader_or_writer,
        params,
        prefix: str,
        io_type: Literal["reader", "writer"],
        *,
        print_if_none: bool = False,
        exclude_params: bool = False,
    ) -> str:
        if not print_if_none and _is_unset(reader_or_writer):
            return ""
        io_name = reader_or_writer.__name__ if reader_or_writer else "None"
        params = params if reader_or_writer else ""
        _str = f"{prefix}{icon_map[io_type]}{io_name} \n"
        if not exclude_params:
            _str += f"{prefix}   <{params}>\n"
        return _str

    s_builder = ""
    s_builder += _print_io(
        node._reader,
        node._reader_params,
        prefix,
        "reader",
        print_if_none=print_if_none,
        exclude_params=exclude_params,
    )
    s_builder += _print_io(
        node._writer,
        node._writer_params,
        prefix,
        "writer",
        print_if_none=print_if_none,
        exclude_params=exclude_params,
    )
    s_builder = s_builder.rstrip("\n")
    print(s_builder) if s_builder else None

    if isinstance(node, DataStreamGroup):
        if not node.has_data:
            print(f"{prefix}{prefix}{icon_map[None]} Not loaded")
        else:
            for key, child in node.data_streams.items():
                print(f"{prefix}{icon_map[type(child)]} {key}")
                print_data_stream_tree(child, prefix + "    ", exclude_params=exclude_params)
