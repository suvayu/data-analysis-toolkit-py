"""Spark utilities"""


def get_spark_session_ctx(appName):
    """Get or create a Spark Session, and the underlying Context."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(appName).getOrCreate()
    sc = spark.sparkContext
    return (spark, sc)


def sql_function(function_name):
    """Get SQL function from `pyspark.sql.functions`.

    This function throws `AttributeError` on failure.

    """
    from datk.utils.helpers import import_from
    return import_from('pyspark.sql.functions', function_name)


def sql_type(type_name):
    """Get SQL type from `pyspark.sql.types`.

    This function throws `AttributeError` on failure.

    """
    from datk.utils.helpers import import_from
    return import_from('pyspark.sql.types', type_name)


def get_spark_logger(name):
    """Get logger for the driver program.

    NB: This won't work in executors as there's no Python layer.

    """
    from pyspark.context import SparkContext
    log4j = SparkContext.getOrCreate()._jvm.org.apache.log4j
    return log4j.LogManager.getLogger(name)


def get_spark_log_levels(level):
    from pyspark.context import SparkContext
    log4j = SparkContext.getOrCreate()._jvm.org.apache.log4j
    return getattr(log4j.Level, level)
