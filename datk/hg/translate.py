"""Translate Histogrammar objects to numpy datastructures"""

from datk.hg.utils import eval_aggr


def eval_container(cont, prop='values', filter_expr=None, replace_by=0):
    """Numerical values of container.

    The numerical values are accessed as per aggr_t, if the aggregator type
    is not present, say for a branch, then simply 'values' is returned.

    cont        -- the container object

    prop        -- specify which property is used to access the values.
                   If None, treat cont as a list of values.

    filter_expr -- whether to filter and replace by replace_by

    replace_by  -- replace by value when filtering, if replace_by is None
                   however, skip the entry

    Evaluate the aggregators in the container and return the values as a list.
    If the container is made of other containers or unsupported aggregators,
    the items are returned as is.

    """
    cont = getattr(cont, prop) if prop else cont
    if filter_expr:
        if replace_by is None:
            res = [eval_aggr(i) for i in cont
                   if not filter_expr(eval_aggr(i))]
        else:
            res = [replace_by if filter_expr(eval_aggr(i))
                   else eval_aggr(i) for i in cont]
    else:
        res = [eval_aggr(i) for i in cont]
    return res
