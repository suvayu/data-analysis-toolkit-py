# coding=utf-8
"""Utilities for filesystem operations"""

import logging
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import s3fs


logger = logging.getLogger(__name__)


class LocalFS(pa.filesystem.LocalFileSystem):
    """An __fspath__ API compatible filesystem class for the local filesystem.

    This extends LocalFileSystem from pyarrow to handle a pre-existing
    directories in `mkdir`.  The __fspath__ API is described in PEP 519.  For
    more, see the docstring of `to_parquet` below.

    """

    def mkdir(self, *args, exist_ok=False, **kwargs):
        """Create a directory

        Parameters
        ----------
        path
            path to create, can be a string, or a `pathlib.Path`

        create_parents (optional, default=True)
            Whether to create intermediate directories

        exist_ok (optional, default=False)
            If pre-existing directories are acceptable or not

        """
        try:
            super().mkdir(*args, **kwargs)
        except FileExistsError:
            if exist_ok:
                pass
            else:
                raise


@contextmanager
def dirpath(path, mkdir=True, fs=None):
    """Canonicalise the path and ensure the directory tree exists"""
    path = (
        Path(path).expanduser().resolve()
        if fs is None
        else Path(path).expanduser()
    )
    fs = LocalFS() if fs is None else fs
    if mkdir:
        try:
            fs.mkdir(path, exist_ok=True)
        except PermissionError as err:
            logger.exception("")  # logs the exception
            raise err
        else:
            yield path
    else:
        if fs.exists(path) and fs.isdir(path):
            yield path
        else:
            # FIXME: need custom exception class, and separate non-existent
            # from non-directory, also see:
            # https://stackoverflow.com/a/6090497/289784
            logger.error(f"{path} does not exist or is not a directory")
            raise FileNotFoundError(path)


def dwim_path_fs(path):
    """Given a local path or an S3 URL, return the path/key and a filesystem

    see the docstring of `to_parquet` for a detailed explanation.

    """
    path = path.split("://", 1)
    if len(path) > 1:
        if path[0] == "s3":
            return (Path(path[1]), s3fs.S3FileSystem(anon=False))
        else:
            raise ValueError(f"unknown protocol: {path[0]}")
    else:
        return (Path(path[0]).expanduser().resolve(), None)


def recursive_delete(path):
    import os.path
    import shutil
    from pyarrow import hdfs

    fs = hdfs.connect()
    if os.path.exists(path):  # if local filesystem
        shutil.rmtree(path)
    elif fs.exists(path):  # if HDFS
        fs.delete(path, recursive=True)
    else:
        print("something went really wrong!")
