# aind-behavior-core-analysis

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)

A repository with core primitives for analysis shared across all `Aind.Behavior` tasks

This repository is part of a bigger infrastructure summarized below:

![Dependency Tree](https://raw.githubusercontent.com/AllenNeuralDynamics/Aind.Behavior.Services/main/assets/dependency_tree.drawio.svg)

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
