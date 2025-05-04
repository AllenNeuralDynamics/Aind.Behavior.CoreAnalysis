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

The current goal of the API is to provide users with a way to instantiate "data contracts" and corresponding data ingestion logic. For instance, loading the data from different streams and converting them into a common format (e.g. `pandas.DataFrame`). For examples of what this looks like, please check the [Examples](./examples/) folder.

## Installing and Upgrading

if you choose to clone the repository, you can install the package by running the following command from the root directory of the repository:

```
pip install .
```

Otherwise, you can use pip:

```
pip install aind-behavior-core-analysis@https://github.com/AllenNeuralDynamics/Aind.Behavior.CoreAnalysis
```

## Contributors

Contributions to this repository are welcome! However, please ensure that your code adheres to the recommended DevOps practices below:

### Linting

We use [ruff](https://docs.astral.sh/ruff/) as our primary linting tool.

### Testing

Attempt to add tests when new features are added.
To run the currently available tests, run `python -m unittest` from the root of the repository.

### Lock files

We use [uv](https://docs.astral.sh/uv/) to manage our lock files.

### Versioning

Where possible, adhere to [Semantic Versioning](https://semver.org/).