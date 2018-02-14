"""Classes and utilities to work with Spark SQL"""

from sqlalchemy import MetaData, Table
from datk.spark.utils import get_spark_logger


def sql2str(sql, dialect=None):
    """Compile and convert SQL query into string"""
    res = sql.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    return str(res)


class SparkDataFrameQuery(object):
    """A query class that encapsulates the details of SQL statements.

    """

    def __init__(self, session, df, name, schema):
        self._session = session
        self._df = df
        self._tblname = name
        self.metadata = MetaData()  # assume default is okay
        self._tbl = Table(self._tblname, self.metadata, *schema)
        self._df.createOrReplaceTempView(self._tblname)
        self.logger = get_spark_logger(self.__class__.__name__)

    def drop_view(self, name=''):
        # def __del__(self):
        # FIXME: Although destructors are discouraged in Python, cannot find a
        # cleaner way to drop the Spark SQL table created during initialisation
        self.spark.catalog.dropTempView(self._tblname if name == '' else name)

    @property
    def table(self):
        """SQL Alchemy table"""
        return self._tbl

    @property
    def session(self):
        """Spark Session"""
        return self._session

    @property
    def spark(self):
        """Alias for Spark Session"""
        return self._session

    def set_log_level(self, level):
        self.logger.setLevel(level)

    @property
    def df(self):
        """DataFrame"""
        return self._df

    def _execute_sql(self, sql):
        res = sql2str(sql)
        self.logger.debug(res)
        return self.spark.sql(res)
