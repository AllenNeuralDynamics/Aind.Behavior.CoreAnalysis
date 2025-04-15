# aind-behavior-core-analysis

![CI](https://github.com/AllenNeuralDynamics/Aind.Behavior.CoreAnalysis/actions/workflows/ci.yml/badge.svg)
[![PyPI - Version](https://img.shields.io/pypi/v/aind-behavior-core-analysis)](https://pypi.org/project/aind-behavior-core-analysis/)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

A repository with core primitives for analysis shared across all `Aind.Behavior` tasks

This repository is part of a bigger infrastructure that is summarized [here](https://github.com/AllenNeuralDynamics/Aind.Behavior.Services).

> ⚠️ **Caution:**  
> This repository is currently under active development and is subject to frequent changes. Features and APIs may evolve without prior notice.

## Getting started and API usage

The current goal of the API is to provide users with a way to instantiate "data contracts" and corresponding data ingestion logic. For instance, loading the data from different streams and converting them into a common format (e.g. `pandas.DataFrame`) can be done by:
For examples of what this looks like, please check the [Examples](./examples/) folder.

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
