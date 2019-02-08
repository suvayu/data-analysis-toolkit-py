"""DataFrame tools"""


import os.path
from itertools import product, chain
from collections.abc import Iterable

from pyarrow import hdfs
from pyspark.sql.utils import AnalysisException


def concat_df(dfs, cols):
    """Concatenate dataframes, optionally only keep specified columns."""
    res = None
    for df in dfs:
        if res is None:
            res = df.select(*cols)
        else:
            res = res.union(df.select(*cols))
    return res


# Spark I/O helpers
def to_parquet(df, output, partitions, logger):
    """Write dataframe to output in Parquet format.

    Partition the output as requested.

    """
    if isinstance(partitions, str):
        df.write.parquet(output, partitionBy=partitions)
    elif isinstance(partitions, Iterable):
        df.write.partitionBy(*partitions).parquet(output)
    else:
        if partitions is not None:
            logger.warn('partitioning request flew over my head, ignoring')
        df.write.parquet(output)


def read_parquet(session, basepath, partitions, logger):
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
