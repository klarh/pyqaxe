.. pyqaxe documentation master file, created by
   sphinx-quickstart on Tue Jul 24 20:39:02 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyqaxe's documentation!
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

`pyqaxe` is a library to facilitate unifying data access from a
variety of sources. The basic idea is to expose data through custom
tables and adapters using python's `sqlite3` module.

::

   cache = pyqaxe.Cache()
   cache.index(pyqaxe.mines.Directory())
   cache.index(pyqaxe.mines.GTAR())

   for (positions,) in cache.query(
       'select data from gtar_records where name = "position"'):
       pass # do something with positions array

Installation
============

Install from PyPI::

  pip install pyqaxe

Alternatively, install from source using the typical distutils
procedure::

  python setup.py install

Examples
========

Usage examples go in the `examples` directory.

Documentation
=============

.. automodule:: pyqaxe
   :members: Cache

.. automodule:: pyqaxe.mines.directory
   :members:

.. automodule:: pyqaxe.mines.gtar
   :members:

.. automodule:: pyqaxe.mines.glotzformats
   :members:

.. automodule:: pyqaxe.mines.tarfile
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
