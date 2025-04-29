from typing import List, Optional, Tuple

from ._core import DataStream, DataStreamGroup


def load_branch(group: DataStreamGroup, strict: bool = False) -> Optional[List[Tuple[str, DataStream, Exception]]]:
    """Recursively load all data streams in the branch using breadth-first traversal.

    This method first loads the data for the current node, then proceeds to load
    all child nodes in a breadth-first manner.
    """
    if strict:
        group.load()
        for _, stream in group.walk_data_streams():
            stream.load()
        return None
    else:
        exceptions = []
        for key, stream in group.walk_data_streams():
            try:
                stream.load()
            except Exception as e:
                exceptions.append((key, stream, e))
        return exceptions
