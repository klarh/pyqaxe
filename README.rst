
========
Overview
========

`pyqaxe` is a library to facilitate unifying data access from a
variety of sources. The basic idea is to expose data through custom
tables and adapters using python's `sqlite3` module.

::

   cache = pyqaxe.Cache()
   cache.index(pyqaxe.data_sources.Directory())
   cache.index(pyqaxe.data_sources.GTAR())

   for (positions,) in cache.query(
       'select data from gtar_records where name = "position"'):
       pass # do something with positions array

Installation
============

Use the typical distutils procedure::

  python setup.py install

Examples
========

Usage examples go in the `examples` directory.
