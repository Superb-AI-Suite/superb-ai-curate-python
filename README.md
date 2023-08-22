# `superb-ai-curate`

[![Coverage Status](https://coveralls.io/repos/github/Superb-AI-Suite/superb-ai-curate-python/badge.svg?branch=main)](https://coveralls.io/github/Superb-AI-Suite/superb-ai-curate-python?branch=main)
[![Version](https://img.shields.io/pypi/v/superb-ai-curate)](https://pypi.org/project/superb-ai-curate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

`superb-ai-curate` is the Python client for interacting with [Superb Curate](https://superb-ai.com/).

## Installation

You don't need this source code unless you want to modify the package. If you just want to use the package, just run:

```bash
$ pip install --upgrade superb-ai-curate
```

### Requirements

Python 3.7+

## Documentation

You can also find the documentation for `superb-ai-curate` [on the website](https://superb-ai-curate-python.readthedocs.io/en/latest/).

* [Introduction](https://superb-ai-curate-python.readthedocs.io/en/latest/intro/overview.html)
* [Installation](https://superb-ai-curate-python.readthedocs.io/en/latest/intro/install.html)
* [Tutorial](https://superb-ai-curate-python.readthedocs.io/en/latest/intro/tutorial.html)

## Usage

An Access Key is required to use the python client. This can be generated from the Settings > Access menu on the Superb AI Curate website. For more details on access key issuance and management, you can check the [Access Key Management](https://docs.superb-ai.com/reference/access-key-management) documentation. The Team Name refers to the organization name that your personal account belongs to.

```python
import spb_curate
from spb_curate import curate

spb_curate.access_key = "..."
spb_curate.team_name = "..."

dataset = curate.fetch_dataset(id="...")

images = [
    curate.Image(
        key="<unique image key>",
        source=curate.ImageSourceLocal(asset="/path/to/image"),
        metadata={"weather": "clear", "timeofday": "daytime"},
    ),
    curate.Image(
        key="<unique image key>",
        source=curate.ImageSourceLocal(asset="/path/to/image"),
        metadata={"weather": "clear", "timeofday": "daytime"},
    ),
]

job: curate.Job = dataset.add_images(images=images)
job.wait_until_complete()

```

### Configuring per-request

For use with multiple credentials, the requests can be configured at the function level.

```python
from spb_curate import curate

dataset = curate.fetch_dataset(access_key="...", team_name="...", id="...")
```

### Logging

If required, the client can be configured to produce basic logging output. There are two levels that are logged to, `INFO` and `DEBUG`. For production use, `INFO` is the recommended logging level, however `DEBUG` can be used for more verbosity.

There are several methods for setting the log level.

1. Environment variable

```bash
$ export SPB_LOG_LEVEL = "INFO"
```

2. Superb AI Curate Python client global setting

```python
import spb_curate

spb_curate.log_level = "INFO"
```

3. Python logging library

```python
import logging

logging.basicConfig()
logging.getLogger("superb-ai").setLevel(logging.INFO)
```

### Development

The development environment relies on [Poetry](https://python-poetry.org/) for package management, testing and building.

```bash
$ poetry install -E dev
$ poetry run pytest --cov=spb_curate tests
```
