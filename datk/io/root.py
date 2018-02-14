# coding=utf-8
"""ROOT I/O utilities"""

from ROOT import TFile


class Rtmpfile(object):
    # NOTE: This is to ignore the security warning from os.tmpnam.
    # There is no risk since I `recreate' the TFile.
    from warnings import filterwarnings
    filterwarnings(action='ignore', category=RuntimeWarning,
                   message='tmpnam is a potential security risk.*')

    def __init__(self, keep=False):
        """Do not remove temporary file if `keep' is `True'"""
        self.keep = keep
        from os import tmpnam
        from time import strftime, localtime
        self.rfile = TFile.Open('{}-{}.root'.format(
            tmpnam(), strftime('%y-%m-%d-%H%M%S%Z', localtime())), 'recreate')

    def __del__(self):
        from os import remove
        self.rfile.Close()
        if not self.keep:
            remove(self.rfile.GetName())

    def __enter__(self):
        """Return the ROOT file when used with `with'"""
        return self.rfile

    def __exit__(self, exc_type, exc_value, traceback):
        """Return True, in case an exception is raised, ignored otherwise."""
        return True
