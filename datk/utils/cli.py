# coding=utf-8
"""Utilities for writing scripts or command parsers"""

from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter,
                      RawDescriptionHelpFormatter)


class NoExitArgParse(ArgumentParser):
    """Do no exit on error, raise RuntimeError exception instead.

    This subclass is useful when `ArgumentParser` is being used to
    parse custom commands inside an interactive command line
    application.

    """
    def error(self, message):
        raise RuntimeError(message)


class RawArgDefaultFormatter(ArgumentDefaultsHelpFormatter,
                             RawDescriptionHelpFormatter):
    """Combine raw help text formatting with default argument display."""
    pass
