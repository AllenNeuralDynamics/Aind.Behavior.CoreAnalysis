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
