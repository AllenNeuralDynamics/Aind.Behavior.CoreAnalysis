from .base import DataStream


def print_data_stream_tree(
    node: DataStream,
    prefix: str = "",
    is_last: bool = True,
    parents: list[bool] = [],
    show_params: bool = False,
    show_type: bool = False,
    show_missing_indicator: bool = True,
) -> str:
    """Generates a tree representation of a data stream hierarchy.

    Creates a formatted string displaying the hierarchical structure of a data stream
    and its children as a tree with branch indicators and icons.

    Args:
        node: The data stream node to start printing from.
        prefix: Prefix string to prepend to each line, used for indentation.
        is_last: Whether this node is the last child of its parent.
        parents: List tracking whether each ancestor was a last child, used for drawing branches.
        show_params: Whether to render parameters of the datastream.
        show_type: Whether to render the class name of the datastream.
        show_missing_indicator: Whether to render the missing data indicator.

    Returns:
        str: A formatted string representing the data stream tree.

    Examples:
        ```python
        from contraqctor.contract import Dataset, csv, json
        from contraqctor.contract.utils import print_data_stream_tree

        # Create a dataset with streams
        csv_stream = csv.Csv("data", reader_params=csv.CsvParams(path="data.csv"))
        json_stream = json.Json("config", reader_params=json.JsonParams(path="config.json"))
        dataset = Dataset("experiment", [csv_stream, json_stream], version="1.0.0")

        # Print the tree
        tree = print_data_stream_tree(dataset)
        print(tree)
        # Output:
        # 📂 experiment
        # ├── 📄 data
        # └── 📄 config
        ```
    """
    icon_map = {
        False: "📄",
        True: "📂",
        None: "❓",
    }

    node_icon = icon_map[node.is_collection]
    if not node.has_data and show_missing_indicator:
        node_icon += f"{icon_map[None]}"

    line_prefix = ""
    for parent_is_last in parents[:-1]:
        line_prefix += "    " if parent_is_last else "│   "

    if parents:
        branch = "└── " if is_last else "├── "
        line_prefix += branch

    # Build node label with name, type, and parameters
    node_label = node.name

    if show_type:
        node_label += f" [{node.__class__.__name__}]"

    if show_params and hasattr(node, "reader_params") and node.reader_params:
        params_str = str(node.reader_params)
        node_label += f" ({params_str})"

    tree_representation = f"{line_prefix}{node_icon} {node_label}\n"

    if node.is_collection and node.has_data:
        for i, child in enumerate(node.data):
            child_is_last = i == len(node.data) - 1
            tree_representation += print_data_stream_tree(
                child,
                prefix="",
                is_last=child_is_last,
                parents=parents + [is_last],
                show_params=show_params,
                show_type=show_type,
                show_missing_indicator=show_missing_indicator,
            )

    return tree_representation
