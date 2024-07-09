import re
from typing import Union, Sequence


StrPattern = Union[str, Sequence[str]]


def validate_str_pattern(pattern: StrPattern) -> None:
    """
    Validates a string pattern or a sequence of string patterns.

    Args:
        pattern (StrPattern): The string pattern or sequence of string patterns to validate.

    Raises:
        re.error: If any of the patterns is not a valid regex pattern.

    Returns:
        None
    """
    if isinstance(pattern, Sequence):
        for pat in pattern:
            validate_str_pattern(pat)
    else:
        try:
            re.compile(pattern)
        except re.error as err:
            raise re.error(f"Pattern {pattern} is not a valid regex pattern") from err
