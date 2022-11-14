
"""Command-line implementation of maellin."""
from __future__ import annotations
import argparse
import sys
from typing import Sequence
import platform

from maellin.app.main import app
from maellin import __version__


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the main bit of the application.
    This handles the creation of an instance of :class:`Application`, runs it,
    and then exits the application.
    :param argv:
        The arguments to be passed to the application for parsing.
    """
    print(
        r"""
        Welcome to
        __  __                  _   _   _               _         
        |  \/  |   __ _    ___  | | | | (_)  _ __       (_)   ___  
        | |\/| |  / _` |  / _ \ | | | | | | | '_ \      | |  / _ \ 
        | |  | | | (_| | |  __/ | | | | | | | | | |  _  | | | (_) |
        |_|  |_|  \__,_|  \___| |_| |_| |_| |_| |_| (_) |_|  \___/                                                             
        verison %s""" % __version__)
    
    print("Using Python version %s (%s, %s)" % (
        platform.python_version(),
        platform.python_build()[0],
        platform.python_build()[1]
        )
    )
        
    if argv is None:
        argv = sys.argv[1:]

    #app.run(argv)
    return app.exit_code()