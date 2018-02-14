# coding=utf-8
"""RooFit utilities"""


# RooFit utilities
def dst_iter(dst):
    """RooAbsData iterator: generator to iterate over a RooDataSet"""
    argset = dst.get()
    for i in range(dst.numEntries()):
        dst.get(i)
        yield argset


def rf_verbosity(lvl):
    """Set RooFit verbosity level"""
    from ROOT import RooMsgService
    msgsvc = RooMsgService.instance()
    oldlvl = msgsvc.globalKillBelow()
    msgsvc.setGlobalKillBelow(lvl)
    return oldlvl
