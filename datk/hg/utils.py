"""Histogrammar utilities"""

import numpy as np

from utils.helpers import import_from

# supported aggregator types
aggr_t = {
    import_from('histogrammar', 'Sum'): 'sum',
    import_from('histogrammar', 'Average'): 'mean',
    import_from('histogrammar', 'Maximize'): 'max',
    import_from('histogrammar', 'Minimize'): 'min',
    import_from('histogrammar', 'Count'): 'entries'
}


def get_aggr_val(aggregator):
    """Get aggregator value based on aggregator type

    """
    return getattr(aggregator, aggr_t.get(type(aggregator), 'values'))


def aggregate_sparsebins(cont, prop='values'):
    """Aggregate the boundaries of a binning with sparse bins

    cont        -- the container object

    prop        -- specify which property is used to access the values.
                   If None, treat cont as a list of values.

    """
    cont = getattr(cont, prop) if prop else cont
    data = np.array([(v.low, v.minBin, v.high, v.maxBin)
                     for v in cont if v.low is not None],
                    dtype='f8, i4, f8, i4')
    # FIXME: check if same index
    low, binlo = min(data['f0']), min(data['f1'])
    high, binhi = max(data['f2']), max(data['f3'])
    # simply adding 1 (np+ 1) converts the returned value to np.int64, which
    # subsequently fails when calling the TH2 constructor!
    nbins = binhi - binlo + 1
    return (int(nbins), int(binlo), low, high)
