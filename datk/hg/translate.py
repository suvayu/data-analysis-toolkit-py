"""Translate Histogrammar objects to arrays

Developer notes:

- isinstance(obj, ..) does not work as the containers are dynamically
  generated.  Compare with cont.factory or cont.name (str).

- Usually bins are lists, however sparse bins are dictionaries with integer
  indices, so integer indexing works regardless of the type of binning.

- The 'values' property of a binning holds the bin containers (whatever that
  maybe), and all containers including entries, underflow, and overflow (as
  well as bins) are found in 'children'.

"""

import numpy as np

# other types like IrregularlyBin are not supported at the moment
from histogrammar import Bin, SparselyBin

from datk.utils.helpers import import_from, get_property
from datk.hg.utils import sparsebin_props, sparsebin_bounds


# supported aggregator types
aggr_t = {
    import_from('histogrammar', 'SparselyBin'): 'bins',
    import_from('histogrammar', 'Sum'): 'sum',
    import_from('histogrammar', 'Average'): 'mean',
    import_from('histogrammar', 'Maximize'): 'max',
    import_from('histogrammar', 'Minimize'): 'min',
    import_from('histogrammar', 'Count'): 'entries'
}


def eval_aggr(aggregator):
    """Get aggregator value based on aggregator type.

    If the aggregator is unsupported, return the object as is (as returned by
    aggr.values).

    """
    return getattr(aggregator, aggr_t.get(type(aggregator), 'values'))


def eval_container(cont, prop='values', filter_expr=None, replace_by=0):
    """Numerical values of container.

    The numerical values are accessed as per aggr_t, if the aggregator type
    is not present, say for a branch, then simply 'values' is returned.

    cont        -- the container object

    prop        -- specify which property is used to access the values.

    filter_expr -- whether to filter and replace by replace_by

    replace_by  -- replace by value when filtering, if replace_by is None
                   however, skip the entry

    Evaluate the aggregators in the container and return the values as a list.
    If the container is made of other containers or unsupported aggregators,
    the items are returned as is.

    """
    vals = getattr(cont, prop) if prop else cont
    if filter_expr:
        if replace_by is None:
            res = [eval_aggr(i) for i in vals if not filter_expr(eval_aggr(i))]
        else:
            res = [replace_by if filter_expr(eval_aggr(i))
                   else eval_aggr(i) for i in vals]
    else:
        res = [eval_aggr(i) for i in vals]
    return res


def eval_sparse_container(cont):
    _1, offset, _2, _3 = sparsebin_props(cont)
    res = [(n - offset + 1, eval_aggr(cont.bins[n])) for n in cont.bins.keys()]
    return res


def eval_recursively(cont, prop='values', filter_expr=None, replace_by=0,
                     no_metadata=True):
    if cont.factory == Bin:
        res = eval_container(cont, prop, filter_expr, replace_by)
    elif cont.factory == SparselyBin:
        res = eval_sparse_container(cont)
    else:
        raise ValueError('unsupported binning: ' + res[0].name)

    if no_metadata is False:
        if cont.factory == Bin:
            metadata = get_property((cont, cont.underflow, cont.overflow),
                                    'entries')
        else:  # if unsupported, exception is raised above
            metadata = sparsebin_props(cont)

    # contents
    if np.isreal(res[0]) or isinstance(res[0], np.ndarray):
        if no_metadata:
            return np.array(res)
        else:
            return np.array(res), metadata, cont.factory
    elif res[0].factory == Bin:
        vals = [eval_recursively(each, prop, filter_expr, replace_by, i)
                for i, each in enumerate(res)]
        vals[0], vals_meta, vals_factory = vals[0]
        if no_metadata:
            return vals
        else:
            return vals, vals_meta, vals_factory
    elif res[0].factory == SparselyBin:
        vals = [eval_recursively(each, prop, filter_expr, replace_by)
                for each in res]
        vals_meta = sparsebin_bounds(cont, 'values')
        if no_metadata:
            return vals
        else:
            return vals, vals_meta, res[0].factory
    else:
        raise ValueError('unsupported binning: ' + res[0].name)
