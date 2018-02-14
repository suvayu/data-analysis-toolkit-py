"""CSV utilities"""


def csv2spark(session, csvfile, *args, **kwargs):
    """Read csv files into a pyspark.sql.dataframe.DataFrame.

    session -- PySpark session object

    csvfile -- CSV file, or file glob.  The file(s) maybe compressed.

       args -- Positional arguments passed on to session.read.csv

     kwargs -- Keyword arguments passed on to session.read.csv

    The schema keyword argument to session.read.csv is specially dealt
    with.  Normally Spark accepts a pyspark.sql.types.StructType
    object, however for convenience this function accepts an simpler
    specification (in addition to the usual).

    The column schema can be specified as a list of 3-tuples, where
    the three fields are: name, type, nullable.  `name` is a string,
    `type` is a type from pyspark.sql.types, and `nullable` is a
    boolean saying if null values are allowed for this field.

    The `header` keyword is set to True by default.  If your CSV files
    do not have a header line, ensure to set it to False.

    """
    from collections.abc import Iterable
    from pyspark.sql.types import (StructType, StructField)
    if 'header' not in kwargs:
        kwargs['header'] = True
    if 'schema' in kwargs and isinstance(kwargs['schema'], Iterable):
        kwargs['schema'] = StructType([StructField(i, j, k)
                                       for i, j, k in kwargs['schema']])
    return session.read.csv(csvfile, *args, **kwargs)


def csv2pandas(csvfile, transform=None, aargs=[], akwargs={}, *args, **kwargs):
    """Read CSV file and return a `pandas.DataFrame` object.

    csvfile   -- CSV file to read

    transform -- A function to transform dataframe columns.  It
                 expects a pandas.Series object (a row in the
                 dataframe) as input.

    aargs     -- pass other arguments to DataFrame.apply()

    akwargs   -- pass other keyword arguments to DataFrame.apply()

    args      -- other arguments to pandas.read_table()

    kwargs    -- other keyword arguments to pandas.read_table()

    An example transformation function that transforms text timestamps
    to time tuples and reads embeded json objects is shown below.

    >>> def transform(row):
    ...     \"\"\"col1, col2, col3 -> col1, time tuple, json\"\"\"
    ...     import json
    ...     return (row[0], str2datetime(row[1]), json.loads(row[2]))

    """
    from logix.utils import suppress_warnings
    suppress_warnings()
    import pandas as pd
    with open(csvfile) as csvfile:
        if 'sep' not in kwargs:
            kwargs['sep'] = ','
        if 'quotechar' not in kwargs:
            kwargs['quotechar'] = '"'
        df = pd.read_table(csvfile, *args, **kwargs)
        if transform:
            df = df.apply(transform, *aargs, axis=1, reduce=False, **akwargs)
    return df
