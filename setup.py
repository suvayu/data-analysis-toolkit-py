"""Data Analysis Toolkit

@author: Suvayu Ali
@email:  fatkasuvayu (plus) linux (at) gmail (dot) com

"""


from setuptools import setup, find_packages

setup(name='DATk',
      version='0.1-dev0',
      description='Data Analysis Toolkit for Python',
      url='https://github.com/suvayu/data-analysis-toolkit-py',
      author='Suvayu Ali',
      author_email='fatkasuvayu+linux@gmail.com',
      license='GPLv3',
      packages=find_packages(exclude=[
          'doc',
          'tests',
          'tmp'
      ]),
      install_requires=[
          'requests',
          'boto3>=1.4.4',
          'SQLAlchemy>=1.1.11',
          'Cython>=0.25.2',
          'pyarrow>=0.6.0',
          'numpy>=1.11.1',
          'scipy>=0.18',
          'pandas>=0.20.1',
          'histogrammar',
          'bokeh>=0.12',
          'matplotlib>=2'
      ])
