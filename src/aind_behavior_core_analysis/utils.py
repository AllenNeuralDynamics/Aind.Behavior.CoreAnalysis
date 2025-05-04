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


def print_data_stream_tree(node: DataStream) -> str:
    icon_map = {
        False: "📄",
        True: "📂",
        None: "❓",
    }

    node_icon = icon_map[node.is_collection]
    node_icon += f"{icon_map[None]}" if not node.has_data else ""

    if node.is_collection and node.has_data:
        html = f'<li><span class="caret">{node_icon} {node.name}</span>'
        html += '<ul class="nested">'
        for child in node.data:
            child_html = print_data_stream_tree(child)
            html += child_html
        html += "</ul></li>"
    else:
        html = f"<li>{node_icon} {node.name}</li>"

    return html
