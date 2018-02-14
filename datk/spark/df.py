"""DataFrame tools"""


def concat_df(dfs, cols):
    """Concatenate dataframes, optionally only keep specified columns."""
    res = None
    for df in dfs:
        if res is None:
            res = df.select(*cols)
        else:
            res = res.union(df.select(*cols))
    return res
