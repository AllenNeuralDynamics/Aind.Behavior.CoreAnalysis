# aind-behavior-core-analysis

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![CodeStyle](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

A repository with core primitives for analysis shared across all `Aind.Behavior` tasks

This repository is part of a bigger infrastructure that is summarized [here](https://github.com/AllenNeuralDynamics/Aind.Behavior.Services).

## Getting started and API usage

The current goal of the API is to provide users with a way to instantiate "data contracts" and corresponding data ingestion logic. For instance, loading the data from different streams and converting them into a common format (e.g. `pandas.DataFrame`) can be done by:

```python

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Union

from aind_behavior_core_analysis.io._core import DataStreamCollection
from aind_behavior_core_analysis.io.data_stream import (
    CsvStream,
    DataStreamCollectionFromFilePattern,
    HarpDataStreamCollectionFactory,
    SoftwareEventStream,
)

NodeType = Union[Dict[str, DataStreamCollection], DataStreamCollection]

@dataclass
class DataContract:
    behavior: Dict[str, NodeType]

root_path = Path(r"test_2024-11-05T190325Z")

dataset = DataContract(
    behavior={
        "Behavior": HarpDataStreamCollectionFactory(
            path=root_path / "behavior" / "Behavior.harp", default_inference_mode="register_0"
        ).build(),
        "LoadCells": HarpDataStreamCollectionFactory(
            path=root_path / "behavior" / "LoadCells.harp", default_inference_mode="register_0"
        ).build(),
        "RendererSynchState": DataStreamCollectionFromFilePattern(
            path=root_path / "behavior" / "Renderer", pattern="RendererSynchState.csv", stream_type=CsvStream
        ).build(),
        "SoftwareEvents": DataStreamCollectionFromFilePattern(
            path=root_path / "behavior" / "SoftwareEvents", pattern="*.json", stream_type=SoftwareEventStream
        ).build(),
    },
)

load_cell_data = dataset.behavior["LoadCells"]["LoadCellData"].load()
load_cell_data.plot()

```

## Installing and Upgrading

if you choose to clone the repository, you can install the package by running the following command from the root directory of the repository:

```
pip install .
```

Otherwise, you can use pip:

```
pip install aind-behavior-core-analysis@https://github.com/AllenNeuralDynamics/Aind.Behavior.CoreAnalysis
```


## Contributing

If you would like to contribute to this repository, open an `Issue` and/or `Pull Request` on this repository. 

### Linters and testing

- Install the provided linting and testing tools in the `project.toml`.

#### Tests

To run tests locally, run the following command from the root directory of the repository:

```
python -m unittest
```

#### Linters


- Use **flake8** to check that code is up to standards (no unused imports, etc.):

```
flake8 .
```

- Use **black** to automatically format the code into PEP standards:

```
black .
```

- Use **isort** to automatically sort import statements:

```
isort .
```
