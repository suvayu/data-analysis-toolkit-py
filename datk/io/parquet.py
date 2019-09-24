# coding=utf-8
"""Data processing utilities for different data sources"""

import logging
from datetime import datetime
from itertools import product, chain
from pathlib import Path
from uuid import uuid4
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from datk.io.fs import dirpath


logger = logging.getLogger(__name__)


METADATA_ATTR = b"_metadata"


def to_parquet_file(df, path, fs=None, metadata=None):
    """Write a dataframe to parquet in a filesystem agnostic manner

    Parameters
    ----------
    df
        `pandas.DataFrame` to write

    path
        Path of the parquet file to write

    fs (optional)
        A filesystem instance conforming to the __fspath__ API (see the doc
        string in read_parquet)

    metadata (optional)
        Dataframe metadata that is stored with the table schema metadata.  If
        None, and `pandas.DataFrame._metadata` is not empty, that is stored
        instead.  Since on read it is restored as `pandas.DataFrame._metadata`,
        this should be a list.

    Returns
    -------
    None

    """
    tbl = pa.Table.from_pandas(df)
    metadata = metadata or df._metadata
    if metadata:
        tbl = tbl.replace_schema_metadata(
            {**tbl.schema.metadata, **{METADATA_ATTR: json.dumps(metadata)}}
        )
    if fs:
        pq.write_table(tbl, path, filesystem=fs)
    else:
        with dirpath(Path(path).parent):
            pq.write_table(tbl, path, filesystem=fs)


# partition name for date columns
DATE_PART = "date_"


def to_parquet(df, root_path, partitions=None, date_col=None, fs=None):
    """Write a partitioned parquet dataset in a filesystem agnostic manner

    FIXME: undefined behaviour if date_col has pandas.NaT (Not a Timestamp)

    Parameters
    ----------
    df
        `pandas.DataFrame` to write

    root_path
        top directory of the Parquet dataset

    partitions (optional)
        list of column names to partition on

    date_col (optional)
        When `pandas.Timestamp` columns are partitioned on, they are treated as
        strings while reading, which makes filtering difficult.  If desired,
        this option identifies the date column, which is then converted to an
        integer of the form YYYYMMDD and saved as the additional column
        `DATE_PART`, making it much simpler to filter on them later.  Note,
        this is in addition to the partitions list above.

    fs (optional)
        A filesystem instance conforming to the __fspath__ API (PEP 519).  If
        not provided, or None, it is assumed to be a native path.  To use this
        method transparently, you may pass the path URI through `dwim_path_fs`
        and pass the (path, fs) tuple onwards.

    Returns
    -------
    None

    """
    if date_col:
        date = df[date_col]
        df[DATE_PART] = date.dt.strftime("%Y%m%d").astype(int)
        if partitions:
            partitions.extend([DATE_PART])
        else:
            partitions = [DATE_PART]
    tbl = pa.Table.from_pandas(df)
    if fs is None:
        with dirpath(root_path):
            pq.write_to_dataset(tbl, root_path, partitions)
    else:
        pq.write_to_dataset(tbl, root_path, partitions, fs)


def read_parquet_file(path, fs=None):
    """Read a parquet file in a filesystem agnostic manner

    Parameters
    ----------
    path
        Path of the parquet file to read

    fs (optional)
        A filesystem instance conforming to the __fspath__ API (see the doc
        string in read_parquet)

    Returns
    -------
    pandas.DataFrame

    """
    fs_open = fs.open if fs else open
    with fs_open(path, mode="rb") as pqfile:
        tbl = pq.read_table(pqfile, use_pandas_metadata=True)
        _metadata = json.loads(tbl.schema.metadata[METADATA_ATTR])
        df = tbl.to_pandas()
        df._metadata += _metadata
        return df


def read_parquet(root_path, filters, fs=None):
    """Read a partitioned parquet dataset with filters

    Parameters
    ----------
    path
        Path of the parquet file to read

    filters

        The list of filters to apply.  They are expected to be in disjunctive
        normal form.  This means, the filters are expressed as a nested list of
        tuples of depth 2, where each tuple describes a column and is a 3-tuple
        of (lhs, comparator, rhs).  The conditions from the inner most list of
        tuples are combined with a boolean AND, where as the outermost list is
        combined with a boolean OR.  For example, the filter below returns all
        rows for july and august for the city of bangalore and hyderabad.

        post_june = ("date_", ">", 20190600)
        pre_sept = ("date_", "<", 20190900)
        in_blr = ("city", "=", "BLR")
        in_hyd = ("city", "=", "HYD")
        [[post_june, pre_sept, in_blr], [post_june, pre_sept, in_hyd]]

        For a more thorough documentation, see the docstring of
        `pyarrow.parquet.ParquetDataset`.

    fs (optional)
        A filesystem instance conforming to the __fspath__ API (see the doc
        string in read_parquet)

    Returns
    -------
    pandas.DataFrame

    """
    dst = pq.ParquetDataset(root_path, filesystem=fs, filters=filters)
    tbl = dst.read_pandas()
    return tbl.to_pandas()


def date2num(date, **kwargs):
    """Convert date to an YYYYMMDD integer

    Parameters
    ----------
    date
        This can be anything that pandas.Timestamp understands

    **kwargs
        Any keyword arguments are passed on to pandas.Timestamp

    Returns
    -------
    int
        Date converted to an integer in YYYYMMDD format

    """
    return int(pd.Timestamp(date, **kwargs).strftime("%Y%m%d"))


def get_daterange_filter(start, end):
    """Convert string date range to daterange filters understood by pyarrow

    (see docstring of `read_parquet` above)

    NOTE: only expected use is reading date partitioned datasets.  The date key
    is hardcoded to `DATE_PART`, and must match the key used in `to_parquet`.

    Parameters
    ----------
    start
        Starting date of the date range. The date can be any timestamp
        understood by pandas.Timestamp.

    end
        End date

    Returns
    -------
    List[List[Tuple]]
        Filters in disjunctive normal form (see docstring of `read_parquet`
        above).  The date column name is hardcoded to `DATE_PART` to be
        consistent with `read_parquet`.

    """
    if pd.Timestamp(start) > pd.Timestamp(end):
        raise ValueError(f"{start} comes after {end}")

    start, end = date2num(start), date2num(end)
    if start == end:
        return [(DATE_PART, "==", start)]
    else:
        return [(DATE_PART, ">=", start), (DATE_PART, "<=", end)]


class PartitionedParquet(object):
    def __init__(self, basepath, partitions):
        path_tmpl = "/".join(["{}={}"] * len(partitions))
        path_tmpl = "{}/{}".format(basepath, path_tmpl)

        parts = [product([part], vals) for part, vals in partitions.items()]
        parts = [i for i in product(*parts)]
        self._dirs = [
            path_tmpl.format(*tuple(chain.from_iterable(i))) for i in parts
        ]
        # store partitions to get the corresponding index later
        self._parts = np.array(parts)[Ellipsis, 1]

    def get_index(self, part):
        """Get the index for the given partition"""
        from functools import reduce

        ret = [
            n
            for n in map(
                lambda i: reduce(lambda p, q: p and q, i), self._parts == part
            )
        ]
        return ret.index(True)

    @classmethod
    def make_dirs(cls, _dirs):
        """Create output directories"""
        import os

        for _dirpath in _dirs:
            os.makedirs(_dirpath, exist_ok=True)

    @property
    def dirs(self):
        return self._dirs


# FIXME: see filters
class PartitionedParquetWriter(PartitionedParquet):
    def __init__(self, basepath, partitions, schema):
        super().__init__(basepath, partitions)
        self.schema = schema

    def get_writers(self):
        """Return the output file writers"""
        self.make_dirs(self._dirs)
        ts = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        self._paths = []
        self._writers = []
        for _dirpath in self.dirs:
            fname = "{}/{}-{}.parquet".format(_dirpath, ts, uuid4())
            self._paths += [fname]
            self._writers += [
                pq.ParquetWriter(fname, self.schema, flavor="spark")
            ]
        return self._writers

    @property
    def paths(self):
        return self._paths

    @property
    def writers(self):
        if hasattr(self, "_writers"):
            return self._writers
        else:
            return self.get_writers()


class PartitionedParquetReader(PartitionedParquet):
    def get_dataset(self):
        """Read a partitioned parquet dataset"""
        from glob import glob

        self._paths = [glob("{}/*".format(dirpath)) for dirpath in self.dirs]
        self._paths = [i for i in chain.from_iterable(self._paths)]
        self._dst = pq.ParquetDataset(self.paths)
        return self._dst

    @property
    def paths(self):
        return self._paths

    @property
    def dataset(self):
        if hasattr(self, "_dst"):
            return self._dst
        else:
            return self.get_dataset()
