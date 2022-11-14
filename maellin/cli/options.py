"""Contains the logic for all of the default options for Flake8."""
from __future__ import annotations

import argparse


def _arg_parser() -> argparse.ArgumentParser:
    """Register preliminary options.
    The preliminary options include:
    - ``-v``/``--verbose``
    - ``--output-file``
    - ``--append-config``
    - ``--config``
    - ``--isolated``
    - ``--enable-extensions``
    """
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument(
        "-d",
        "--dag_dir",
        default='.dags',
        action="store",
        help="""Configure the location of your dags. If left blank a new directory .dags is created in the project root"""
    )

    return parser
