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
    if cont.factory == SparselyBin:
        nbins, offset, _, __ = sparsebin_props(cont)
    else:
        nbins, offset = len(vals), 0
    # access by index to support both Bin and SparselyBin
    if filter_expr:
        if replace_by is None:
            res = [eval_aggr(vals[i + offset]) for i in range(nbins)
                   if not filter_expr(eval_aggr(vals[i + offset]))]
        else:
            res = [replace_by if filter_expr(eval_aggr(vals[i + offset]))
                   else eval_aggr(vals[i + offset]) for i in range(nbins)]
    else:
        res = [eval_aggr(vals[i + offset]) for i in range(nbins)]
    return res
