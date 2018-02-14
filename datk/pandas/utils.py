"""Pandas utilities"""


def is_Df_not_empty(df):
    """
    To check if the DataFrame is empty or not

    df -- `pandas.DataFrame` to be checked
    """
    import pandas as pd
    return df is not None and isinstance(df, pd.DataFrame) and not df.empty
