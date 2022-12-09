
#   Maellin.io is a lightweight workflows package for
#   developing, scheduling, and monitor data processing
#   workflows in pure python. It comes with a rich set
#   of APIs for connecting to a myraid of data sources
#   and orchestrating tasks using directed acyclic graphs (DAGs).
#   Copyright (C) 2022  Carl Chatterton
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.
from __future__ import annotations
import sys
from typing import Sequence
import platform

from maellin import __version__


def main(argv: Sequence[str] | None = None) -> int:
    """
    Command-line implementation of maellin that executes the main bit of the application.

    Args:
    argv (Sequence[str]): The arguments to be passed to the application for parsing.
        Defaults to None.
    """
    print(
        r"""
        Welcome to
        __  __                  _   _   _               _
        |  \/  |   __ _    ___  | | | | (_)  _ __       (_)   ___
        | |\/| |  / _` |  / _ \ | | | | | | | '_ \      | |  / _ \
        | |  | | | (_| | |  __/ | | | | | | | | | |  _  | | | (_) |
        |_|  |_|  \__,_|  \___| |_| |_| |_| |_| |_| (_) |_|  \___/

        The easiest way to author data workflows with minimal setup!

        Maellin.io verison %s
        Using Python version %s (%s, %s)""" % (
            __version__,
            platform.python_version(),
            platform.python_build()[0],
            platform.python_build()[1]))

    if argv is None:
        argv = sys.argv[1:]

    # app.run(argv)
    return  # app.exit_code()
