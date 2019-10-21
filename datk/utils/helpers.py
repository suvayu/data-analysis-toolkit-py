# coding=utf-8
"""Helper utilities"""

import resource
import re
from datetime import datetime
from copy import deepcopy
from functools import wraps
from importlib import import_module
from warnings import filterwarnings

import numpy as np


def import_from(module, name):
    return getattr(import_module(module), name)


def is_nan_or_inf(x):
    """Is NaN or Inf"""
    return np.isnan(x) or np.isinf(x)


def add_class_property(cls, prop):
    """Dynamically add a property and a setter that does deepcopy.

    Note: a deleter is not defined, so if your property needs special care to
    delete, please do not use this helper function.

    """

    def _getter(self):
        return getattr(self, f"_{prop}", None)

    def _setter(self, val):
        setattr(self, f"_{prop}", deepcopy(val))

    _prop = property(_getter, _setter, None, f"Property {prop}")
    setattr(cls, prop, _prop)


def get_properties(obj, props):
    """Retrieve a sequence of properties from an object

    """
    return [getattr(obj, p) for p in props]


def get_property(objs, prop):
    """Retrieve a property from a sequence of objects

    """
    return [getattr(o, prop) for o in objs]


def sanitise(string):
    """Sanitise string for use as group/directory name"""
    return "_".join(re.findall(re.compile("[^ &()-]+"), string))


def str2datetime(timestamp):
    """Convert timestamp strings to time tuples"""
    return datetime.strptime(timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")


def fullpath(_ls):
    """Decorator for ls implementations.

    Some ls implementations list the files/folders in a directory,
    whereas some list the full path.  This converts the former to full
    path output (e.g. use with os.listdir).

    """

    @wraps(_ls)
    def _ls_fullpath(path):
        return ["{}/{}".format(path, f) for f in _ls(path)]

    return _ls_fullpath


def suppress_warnings():
    """Suppress a few well understood warnings from Pandas and Numpy.

    Warning from pandas about skipping directories, and using imp
    instead of importlib.

    """
    filterwarnings(
        action="ignore",
        category=ImportWarning,
        message="Not importing directory.*",
    )
    filterwarnings(
        action="ignore", category=ResourceWarning, message="unclosed file.*"
    )
    filterwarnings(
        action="ignore",
        category=PendingDeprecationWarning,
        message="the imp module is deprecated.*",
    )
    filterwarnings(
        action="ignore",
        category=np.VisibleDeprecationWarning,
        message="using a non-integer number.*",
    )
    filterwarnings(
        action="ignore",
        category=DeprecationWarning,
        message=".*inspect.getargspec() is deprecated.*",
    )


def resource_summary(prefix="", raw=False):
    """Get memory usage"""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    usage = (*usage[:2], (usage[2] * resource.getpagesize()) / 1e6)
    if raw:
        return usage
    else:
        print("{}: usertime={} systime={} mem={} mb".format(prefix, *usage))
