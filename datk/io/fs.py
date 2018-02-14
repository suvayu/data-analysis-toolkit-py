"""I/O utilities"""


def recursive_delete(path):
    import os.path
    import shutil
    from pyarrow import hdfs
    fs = hdfs.connect()
    if os.path.exists(path):  # if local filesystem
        shutil.rmtree(path)
    elif fs.exists(path):     # if HDFS
        fs.delete(path, recursive=True)
    else:
        print('something went really wrong!')
