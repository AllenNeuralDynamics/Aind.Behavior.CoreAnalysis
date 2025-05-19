from typing import List, Optional, Tuple

from ._core import DataStream, DataStreamCollectionBase


def load_branch(
    group: DataStream | DataStreamCollectionBase, strict: bool = False
) -> Optional[List[Tuple[DataStream, Exception]]]:
    """Recursively load all data streams in the branch using breadth-first traversal.

    This method first loads the data for the current node, then proceeds to load
    all child nodes in a breadth-first manner.
    """
    exceptions = []
    group.load()
    if hasattr(group, "walk_data_streams"):
        for stream in group.walk_data_streams():
            if strict:
                stream.load()
            else:
                try:
                    stream.load()
                except Exception as e:
                    exceptions.append((stream, e))
    return None if strict else exceptions


def print_data_stream_tree(node: DataStream, prefix: str = "", is_last: bool = True, parents: list[bool] = []) -> str:
    icon_map = {
        False: "📄",
        True: "📂",
        None: "❓",
    }

    node_icon = icon_map[node.is_collection]
    node_icon += f"{icon_map[None]}" if not node.has_data else ""

    line_prefix = ""
    for parent_is_last in parents[:-1]:
        line_prefix += "    " if parent_is_last else "│   "

    if parents:
        branch = "└── " if is_last else "├── "
        line_prefix += branch

    tree_representation = f"{line_prefix}{node_icon} {node.name}\n"

    if node.is_collection and node.has_data:
        for i, child in enumerate(node.data):
            child_is_last = i == len(node.data) - 1
            tree_representation += print_data_stream_tree(
                child, prefix="", is_last=child_is_last, parents=parents + [is_last]
            )

    return tree_representation
