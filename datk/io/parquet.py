# coding=utf-8
"""Data processing utilities for different data sources"""

from datetime import datetime
from uuid import uuid4
from itertools import product, chain

import numpy as np
import pyarrow.parquet as pq


def get_parquet_readers(directory):
    dst = pq.ParquetDataset(directory)
    table = dst.read()
    df = table.to_pandas()
    return df, table, dst


class PartitionedParquet(object):
    def __init__(self, basepath, partitions):
        path_tmpl = '/'.join(['{}={}'] * len(partitions))
        path_tmpl = '{}/{}'.format(basepath, path_tmpl)

        parts = [product([part], vals) for part, vals in partitions.items()]
        parts = [i for i in product(*parts)]
        self._dirs = [path_tmpl.format(*tuple(chain.from_iterable(i)))
                      for i in parts]
        # store partitions to get the corresponding index later
        self._parts = np.array(parts)[Ellipsis, 1]

    def get_index(self, part):
        """Get the index for the given partition"""
        from functools import reduce
        ret = [n for n in map(lambda i: reduce(lambda p, q: p and q, i),
                              self._parts == part)]
        return ret.index(True)

    @classmethod
    def make_dirs(cls, _dirs):
        """Create output directories"""
        import os
        for dirpath in _dirs:
            os.makedirs(dirpath, exist_ok=True)

    @property
    def dirs(self):
        return self._dirs


class PartitionedParquetWriter(PartitionedParquet):
    def __init__(self, basepath, partitions, schema):
        super().__init__(basepath, partitions)
        self.schema = schema

    def get_writers(self):
        """Return the output file writers"""
        self.make_dirs(self._dirs)
        ts = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
        self._paths = []
        self._writers = []
        for dirpath in self.dirs:
            fname = '{}/{}-{}.parquet'.format(dirpath, ts, uuid4())
            self._paths += [fname]
            self._writers += [pq.ParquetWriter(fname, self.schema,
                                               flavor='spark')]
        return self._writers

    @property
    def paths(self):
        return self._paths

    @property
    def writers(self):
        if hasattr(self, '_writers'):
            return self._writers
        else:
            return self.get_writers()


class PartitionedParquetReader(PartitionedParquet):
    def get_dataset(self):
        """Read a partitioned parquet dataset"""
        from glob import glob
        self._paths = [glob('{}/*'.format(dirpath)) for dirpath in self.dirs]
        self._paths = [i for i in chain.from_iterable(self._paths)]
        self._dst = pq.ParquetDataset(self.paths)
        return self._dst

    @property
    def paths(self):
        return self._paths

    @property
    def dataset(self):
        if hasattr(self, '_dst'):
            return self._dst
        else:
            return self.get_dataset()


# Spark I/O helpers
def to_parquet(df, output, partitions):
    """Write dataframe to output in Parquet format.

    Partition the output as requested.

    """
    from collections.abc import Iterable
    from datk.spark.utils import get_spark_logger
    logger = get_spark_logger('to_parquet')
    if isinstance(partitions, str):
        df.write.parquet(output, partitionBy=partitions)
    elif isinstance(partitions, Iterable):
        df.write.partitionBy(*partitions).parquet(output)
    else:
        if partitions is not None:
            logger.warn('partitioning request flew over my head, ignoring')
        df.write.parquet(output)


def read_parquet(session, basepath, partitions):
    """Read parquet files and return a Spark dataframe.

    `basepath` points to the parquet data, for partitioned parquet
    data, it is the basepath (hence the keyword name).

    If `partitions` is false (e.g. None, 0, False, etc), read all
    partitions (if any), otherwise `partitions` is expected to be an
    dictionary, where the key is the partition name, and the value is
    a list of values of the partition that are to be read.  A
    dictionary with multiple keys are treated as a hierarchy of
    partitions.  Since the order of the keys in a dictionary is
    indeterminate, use an OrderedDict.

    session    -- Spark SQL session object, typically available as
                  `spark` in the PySpark shell.

    basepath   -- Path to parquet data.

    partitions -- OrderedDict where the key is the partition and the
                  value is a list of values to read be from the
                  partition.

    """
    from itertools import product, chain
    import os.path
    from pyarrow import hdfs
    from pyspark.sql.utils import AnalysisException

    from datk.spark.utils import get_spark_logger
    logger = get_spark_logger('read_parquet')

    if partitions:
        path_tmpl = '/'.join(['{}={}'] * len(partitions))
        path_tmpl = '{}/{}'.format(basepath, path_tmpl)
    else:
        return session.read.parquet(basepath)

    parts = []
    for part, vals in partitions.items():
        parts += [product([part], vals)]
    parts = product(*parts)
    paths = [path_tmpl.format(*tuple(chain.from_iterable(i))) for i in parts]

    # FIXME: check for non-existent paths, but only for HDFS
    if not basepath.startswith('s3://'):
        if os.path.exists(basepath):  # native
            paths = [path for path in paths if os.path.exists(path)]
        else:                         # HDFS
            try:
                fs = hdfs.connect()
            except IOError as err:
                logger.error('read_parquet: {}'.format(err))
                logger.error("read_parquet: can't validate paths")
            else:
                paths = [path for path in paths if fs.exists(path)]
            # FIXME: what happens when both fail!?
    else:
        logger.warn("read_parquet: can't validate S3 paths")

    try:
        res = session.read.option('basePath', basepath).parquet(*paths)
    except AnalysisException as err:
        res = None
        logger.error('read_parquet: {}'.format(err))
    return res
