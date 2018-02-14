# coding=utf-8
"""Working with ROOT histogams"""

import numpy as np


def th1fill(hist, dim=1):
    """Return a TH1.Fill wrapper for use with map(..)."""
    if 1 == dim:
        return lambda i: hist.Fill(i)
    elif 2 == dim:
        return lambda i, j: hist.Fill(i, j)
    elif 3 == dim:
        return lambda i, j, k: hist.Fill(i, j, k)
    else:
        return None


def th1clonereset(hist, name):
    """Clone and reset ROOT histogram"""
    res = hist.Clone(name)
    res.Reset('icesm')
    res.Sumw2()
    return res


def thn2array(hist, overflow=True, err=False, shaped=False, pair=False):
    """Convert ROOT histograms to numpy.array

       hist -- histogram to convert
   overflow -- include underflow and overflow bins
       err  -- include bin errors
     shaped -- return an array with appropriate dimensions, 1-D
               array is returned normally
       pair -- pair bin errors with bin content, by default errors
               are put in a similarly shaped array in res[1]

    """
    # add overflow, underflow bins
    overflow = 2 if overflow else 0
    size, buf = hist.GetSize() - overflow, hist.GetArray()
    if err:
        nerr, errbuf = hist.GetSumw2().GetSize(), hist.GetSumw2().GetArray()
    if shaped:
        xbins = hist.GetNbinsX()
        ybins = hist.GetNbinsY()
        zbins = hist.GetNbinsZ()
        if ybins == 1:
            shape = [xbins + overflow]
        elif zbins == 1:
            shape = [xbins + overflow, ybins + overflow]
        else:
            shape = [xbins + overflow, ybins + overflow, zbins + overflow]
    get_array = lambda data: np.frombuffer(
        data, dtype=type(data[0]), count=size,
        offset=data.itemsize if overflow > 0 else 0)
    val = get_array(buf)
    if err:
        if nerr > 0:  # sum of weights as error (sumw2)
            err = np.sqrt(get_array(errbuf))
        else:
            err = np.sqrt(np.abs(val))
        if not shaped:  # pair = False, True -> axis = 0 , 1
            return np.stack((val, err), axis=int(pair))
        elif shaped and not pair:
            return np.stack((val.reshape(*shape), err.reshape(*shape)), axis=0)
        elif shaped and pair:
            shape += [2]
            np.stack((val, err), axis=1).reshape(*shape)
    elif shaped:
        return val.reshape(*shape)
    else:
        return val


def thnbincontent(hist, x, y=0, z=0, err=False, asym=False):
    """Get histogram bin content.

       hist -- histogram
       x    -- bin x coordinates
       y    -- bin y coordinates (only for 2D)
       z    -- bin z coordinates (only for 3D)
       err  -- also return error
       asym -- return asymmetric error

    """
    dim = hist.GetDimension()
    if dim == 1:
        xyz = [x]
    elif dim == 2:
        xyz = [x, y]
    else:
        xyz = [x, y, z]
    content = hist.GetBinContent(*xyz)
    if err and asym:
        return (content, hist.GetBinErrorUp(*xyz), hist.GetBinErrorLow(*xyz))
    elif err:
        return (content, hist.GetBinError(*xyz))
    else:
        return content


def thn2array_asymerr(hist, asym=False, pair=False, shaped=False,
                      overflow=False):
    """Convert ROOT histograms to numpy.array

       hist -- histogram to convert
       asym -- Asymmetric errors
       pair -- pair bin errors with bin content, by default errors
               are put in a similarly shaped array in res[1]
     shaped -- return an array with appropriate dimensions, 1-D
               array is returned normally
   overflow -- include underflow and overflow bins

    """
    size = hist.GetSize()
    if shaped:
        xbins = hist.GetNbinsX()
        ybins = hist.GetNbinsY()
        zbins = hist.GetNbinsZ()
        # add overflow, underflow bins
        overflow = 2 if overflow else 0
        if ybins == 1:
            shape = [xbins + overflow]
        elif zbins == 1:
            shape = [xbins + overflow, ybins + overflow]
        else:
            shape = [xbins + overflow, ybins + overflow, zbins + overflow]
    else:
        shape = [size]
        if not overflow:
            shape[0] -= 2
    if asym:
        shape.append(3)
    hiter = range(size) if overflow else range(1, size-1)
    # TODO: refactor with buffer from GetArray (see thn2array)
    val = np.array([thnbincontent(hist, i, err=asym, asym=asym)
                    for i in hiter]).reshape(*shape)
    return val if pair else val.transpose()


def taxisbincentre(axis, i, edges=False, width=False):
    """Get histogram bin centre (X-axis).

       axis  -- axis instance
       i     -- bin number
       edges -- also return bin low edges
       width -- also return bin width
    """
    if edges and width:
        return (axis.GetBinCenter(i), axis.GetBinLowEdge(i),
                axis.GetBinWidth(i))
    elif edges:
        return (axis.GetBinCenter(i), axis.GetBinLowEdge(i))
    elif width:
        return (axis.GetBinCenter(i), axis.GetBinWidth(i))
    else:
        return axis.GetBinCenter(i)


def thnbincentre(hist, i, edges=False, width=False):
    """Get histogram bin centre (X, Y, Z).

       hist  -- histogram
       i     -- bin number
       edges -- also return bin low edges
       width -- also return bin width
    """
    dim = hist.GetDimension()
    axes = []
    if dim == 1:
        axes.append(hist.GetXaxis())
    elif dim == 2:
        axes.append(hist.GetYaxis())
    else:
        axes.append(hist.GetZaxis())
    res = [taxisbincentre(ax, i, edges, width) for ax in axes]
    return res[0] if len(res) == 1 else res


def thnbins(hist, edges=False, width=False, pair=False, overflow=False):
    """Return histogram bin centre or edges"""
    hiter = range(len(hist)) if overflow else range(1, len(hist)-1)
    val = np.array([thnbincentre(hist, i, edges=edges, width=width)
                    for i in hiter])
    return val if pair else val.transpose()


def thnprint(hist, err=False, asym=False, pair=False, shaped=True):
    """Print ROOT histograms of any dimention"""
    val = thn2array(hist, err=err, asym=asym, pair=pair, shaped=shaped)
    print('Hist: {}, dim: {}'.format(hist.GetName(), len(np.shape(val))))
    hist.Print()
    print(np.flipud(val))  # flip y axis, FIXME: check what happens for 3D


def th1offset(hist, offset):
    """Offset non-empty histogram bins"""
    # only offset bins with content
    buf = hist.GetArray()
    for i in range(1, hist.GetSize() - 2):  # exclude underflow & overflow
        if buf[i] != 0.:  # FIXME: comparing floats shouldn't work
            buf[i] += offset
    return hist
