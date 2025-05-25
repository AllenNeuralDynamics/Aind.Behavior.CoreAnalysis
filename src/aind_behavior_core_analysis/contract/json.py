import dataclasses
import json
import os
from typing import Generic, Optional, Type, TypeVar

import aind_behavior_services
import aind_behavior_services.data_types
import pandas as pd
import pydantic

from .base import DataStream, FilePathBaseParam


@dataclasses.dataclass
class JsonParams:
    """Parameters for JSON file processing.
    
    Defines parameters for reading JSON files with specified encoding.
    
    Attributes:
        path: Path to the JSON file.
        encoding: Character encoding for the JSON file. Defaults to UTF-8.
    """
    path: os.PathLike
    encoding: str = "UTF-8"


class Json(DataStream[dict[str, str], JsonParams]):
    """JSON file data stream provider.
    
    A data stream implementation for reading single JSON objects from files.
    
    Args:
        DataStream: Base class for data stream providers.
    """
    @staticmethod
    def _reader(params: JsonParams) -> dict[str, str]:
        """Read JSON file into a dictionary.
        
        Args:
            params: Parameters for JSON file reading configuration.
            
        Returns:
            dict: Dictionary containing the parsed JSON data.
        """
        with open(params.path, "r", encoding=params.encoding) as file:
            data = json.load(file)
        return data

    make_params = JsonParams


class MultiLineJson(DataStream[list[dict[str, str]], JsonParams]):
    """Multi-line JSON file data stream provider.
    
    A data stream implementation for reading JSON files where each line 
    contains a separate JSON object.
    
    Args:
        DataStream: Base class for data stream providers.
    """
    @staticmethod
    def _reader(params: JsonParams) -> list[dict[str, str]]:
        """Read multi-line JSON file into a list of dictionaries.
        
        Args:
            params: Parameters for JSON file reading configuration.
            
        Returns:
            list: List of dictionaries, each containing a parsed JSON object from one line.
        """
        with open(params.path, "r", encoding=params.encoding) as file:
            data = [json.loads(line) for line in file]
        return data

    make_params = JsonParams


_TModel = TypeVar("_TModel", bound=pydantic.BaseModel)


@dataclasses.dataclass
class PydanticModelParams(FilePathBaseParam, Generic[_TModel]):
    """Parameters for Pydantic model-based JSON file processing.
    
    Extends the base file path parameters with Pydantic model specification
    for parsing JSON into typed objects.
    
    Attributes:
        model: Pydantic model class to use for parsing JSON data.
        encoding: Character encoding for the JSON file. Defaults to UTF-8.
    """
    model: Type[_TModel]
    encoding: str = "UTF-8"


class PydanticModel(DataStream[_TModel, PydanticModelParams[_TModel]]):
    """Pydantic model-based JSON data stream provider.
    
    A data stream implementation for reading JSON files as Pydantic model instances.
    
    Args:
        DataStream: Base class for data stream providers.
    """
    @staticmethod
    def _reader(params: PydanticModelParams[_TModel]) -> _TModel:
        """Read JSON file and parse it as a Pydantic model.
        
        Args:
            params: Parameters for Pydantic model-based reading configuration.
            
        Returns:
            _TModel: Instance of the specified Pydantic model populated from JSON data.
        """
        with open(params.path, "r", encoding=params.encoding) as file:
            return params.model.model_validate_json(file.read())

    make_params = PydanticModelParams


@dataclasses.dataclass
class ManyPydanticModelParams(FilePathBaseParam, Generic[_TModel]):
    """Parameters for loading multiple Pydantic models from a file.
    
    Extends the base file path parameters with Pydantic model specification
    and options for converting to a DataFrame.
    
    Attributes:
        model: Pydantic model class to use for parsing JSON data.
        encoding: Character encoding for the JSON file. Defaults to UTF-8.
        index: Optional column name to set as the DataFrame index.
        column_names: Optional dictionary mapping original column names to new names.
    """
    model: Type[_TModel]
    encoding: str = "UTF-8"
    index: Optional[str] = None
    column_names: Optional[dict[str, str]] = None


class ManyPydanticModel(DataStream[pd.DataFrame, ManyPydanticModelParams[_TModel]]):
    """Multi-model JSON data stream provider.
    
    A data stream implementation for reading multiple JSON objects from a file,
    parsing them as Pydantic models, and returning them as a DataFrame.
    
    Args:
        DataStream: Base class for data stream providers.
    """
    @staticmethod
    def _reader(params: ManyPydanticModelParams[_TModel]) -> pd.DataFrame:
        """Read multiple JSON objects and convert them to a DataFrame.
        
        Args:
            params: Parameters for multi-model reading configuration.
            
        Returns:
            pd.DataFrame: DataFrame containing data from multiple model instances.
        """
        with open(params.path, "r", encoding=params.encoding) as file:
            model_ls = pd.DataFrame([params.model.model_validate_json(line).model_dump() for line in file])
        if params.column_names is not None:
            model_ls.rename(columns=params.column_names, inplace=True)
        if params.index is not None:
            model_ls.set_index(params.index, inplace=True)
        return model_ls

    make_params = ManyPydanticModelParams


@dataclasses.dataclass
class SoftwareEventsParams(ManyPydanticModelParams):
    """Parameters for software events file processing.
    
    A specialized version of ManyPydanticModelParams that defaults to using
    the SoftwareEvent model from aind_behavior_services.
    
    Attributes:
        model: Set to SoftwareEvent model and not modifiable after initialization.
        encoding: Character encoding for the JSON file. Defaults to UTF-8.
        index: Optional column name to set as the DataFrame index.
        column_names: Optional dictionary mapping original column names to new names.
    """
    model: Type[aind_behavior_services.data_types.SoftwareEvent] = dataclasses.field(
        default=aind_behavior_services.data_types.SoftwareEvent, init=False
    )
    encoding: str = "UTF-8"
    index: Optional[str] = None
    column_names: Optional[dict[str, str]] = None


class SoftwareEvents(ManyPydanticModel[aind_behavior_services.data_types.SoftwareEvent]):
    """Software events data stream provider.
    
    A specialized data stream for reading software event logs from JSON files
    using the SoftwareEvent model from aind_behavior_services.
    
    Args:
        ManyPydanticModel: Base class for multi-model data stream providers.
    """
    make_params = SoftwareEventsParams
