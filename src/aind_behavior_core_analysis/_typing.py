from typing import Any, Generic, Protocol, TypeAlias, TypeVar, Union, cast, final

# Type variables
TData = TypeVar("TData", bound=Union[Any, "_UnsetData"])
"""TypeVar: Type variable bound to Union[Any, "_UnsetData"] for data types."""

TReaderParams = TypeVar("TReaderParams", contravariant=True)
"""TypeVar: Contravariant type variable for reader parameters."""

TData_co = TypeVar("TData_co", covariant=True)
"""TypeVar: Covariant type variable for data output types."""


class IReader(Protocol, Generic[TData_co, TReaderParams]):
    """Protocol defining the reader interface.

    A reader takes parameters and returns data of a specified type.

    Args:
        params: The parameters for reading data.

    Returns:
        The data read using the provided parameters.
    """

    def __call__(self, params: TReaderParams) -> TData_co: ...


@final
class _UnsetReader(IReader[TData, TReaderParams]):
    """A sentinel reader class representing an unset reader.

    This class implements the IReader protocol but throws an error when called,
    indicating that a proper reader has not been set.
    """

    def __call__(self, params: Any) -> Any:
        """Raises an error since the reader is not set.

        Args:
            params: The parameters that would be passed to a reader.

        Raises:
            NotImplementedError: Always raised since this is a placeholder reader.
        """
        raise NotImplementedError("Reader is not set.")


@final
class _UnsetParams:
    """A singleton class representing unset parameter values.

    This class implements the singleton pattern and is used as a sentinel value
    to distinguish between parameters that weren't provided and those explicitly
    set to None.

    Attributes:
        _instance: The singleton instance of this class.

    Examples:
        >>> unset = _UnsetParams()
        >>> print(unset)
        <UnsetParams>
        >>> unset is _UnsetParams()
        True
    """

    _instance = None

    def __new__(cls):
        """Creates or returns the singleton instance.

        Returns:
            The singleton instance of _UnsetParams.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        """Returns the string representation.

        Returns:
            String representation of the unset parameters.
        """
        return "<UnsetParams>"

    def __str__(self):
        """Returns the string representation for print().

        Returns:
            String representation of the unset parameters.
        """
        return "<UnsetParams>"


@final
class _UnsetData:
    """A singleton class representing unset data.

    This class implements the singleton pattern and is used as a sentinel value
    to indicate that data has not been set.

    Attributes:
        _instance: The singleton instance of this class.
    """

    _instance = None

    def __new__(cls):
        """Creates or returns the singleton instance.

        Returns:
            The singleton instance of _UnsetData.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        """Returns the string representation.

        Returns:
            String representation of the unset data.
        """
        return "<UnsetData>"

    def __str__(self):
        """Returns the string representation for print().

        Returns:
            String representation of the unset data.
        """
        return "<UnsetData>"


UnsetParams = cast(Any, _UnsetParams())
"""A singleton instance of _UnsetParams used as a sentinel value."""

UnsetReader: _UnsetReader = _UnsetReader()
"""A singleton instance of _UnsetReader used as a sentinel value."""

UnsetData: Any = _UnsetData()
"""A singleton instance of _UnsetData used as a sentinel value."""

UnsetParamsType: TypeAlias = _UnsetParams
"""Type alias for the _UnsetParams class."""


def is_unset(obj: Any) -> bool:
    """Checks if an object is one of the unset sentinel values.

    Args:
        obj: The object to check.

    Returns:
        True if the object is an unset sentinel value, False otherwise.
    """
    return (obj is UnsetReader) or (obj is UnsetParams) or (obj is UnsetData)
