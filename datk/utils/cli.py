# coding=utf-8
"""Utilities for writing scripts or command parsers"""

from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    RawDescriptionHelpFormatter,
    Action,
    ArgumentError,
)


class NoExitArgParse(ArgumentParser):
    """Do no exit on error, raise RuntimeError exception instead.

    This subclass is useful when `ArgumentParser` is being used to
    parse custom commands inside an interactive command line
    application.

    """

    def error(self, message):
        raise RuntimeError(message)


class RawArgDefaultFormatter(
    ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter
):
    """Combine raw help text formatting with default argument display."""

    pass


def var_nargs(nmin, nmax):
    "Variable nargs custom action"

    class VarNArgsAction(Action):
        """Variable nargs custom action.

        We can pass nmin and nmax to the constructor, or pass it in a closure.
        The latter leads to less code.  The closure idea is from the SO answer:
        https://stackoverflow.com/a/4195302, and to understand how Actions
        work, see: https://docs.python.org/3/library/argparse.html#action.
        NOTE: the nmax limit is not evident in the help string.  It shows as
        one or more args.

        """

        def __call__(self, parser, namespace, values, option_string):
            if nmin <= len(values) <= nmax:
                setattr(namespace, self.dest, values)
            else:
                raise ArgumentError(
                    self,
                    f"received {len(values)} arguments,"
                    f" must be {nmin} <= n <= {nmax}.",
                )

    return VarNArgsAction
