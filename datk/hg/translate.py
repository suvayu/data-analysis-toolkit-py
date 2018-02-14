"""Translate Histogrammar objects to numpy datastructures"""

from datk.hg.utils import get_aggr_val


def container_values(cont, prop='values', filter_expr=None, replace_by=0):
    """Numerical values of container.

    The numerical values are accessed as per aggr_t, if the aggregator type
    is not present, say for a branch, then simply 'values' is returned.

    cont        -- the container object

    prop        -- specify which property is used to access the values.
                   If None, treat cont as a list of values.

    filter_expr -- whether to filter and replace by replace_by

    replace_by  -- replace by value when filtering

    """
    cont = getattr(cont, prop) if prop else cont
    if filter_expr:
        res = []
        for i in cont:
            val = get_aggr_val(i)
            res += [replace_by if filter_expr(val) else val]
    else:
        res = [get_aggr_val(i) for i in cont]
    return res
