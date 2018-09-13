#!/usr/bin/env python

from setuptools import setup

long_description="""
Pyqaxe is a library for transparently accessing datasets as they
change forms.

Documentation
=============

Full documentation is available in standard sphinx form::

  $ cd doc
  $ make html

Automatically-built documentation is available at
https://pyqaxe.readthedocs.io .
"""

with open('pyqaxe/version.py') as version_file:
    exec(version_file.read())

setup(name='pyqaxe',
      author='Matthew Spellings',
      author_email='mspells@umich.edu',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: BSD License',
          'Programming Language :: Python :: 3',
          'Topic :: Database :: Front-Ends'
      ],
      description='Dataset indexing and curation tool',
      install_requires=['scandir ; python_version<"3.5"'],
      license='BSD',
      long_description=long_description,
      packages=[
          'pyqaxe',
          'pyqaxe.mines'
      ],
      project_urls={
          'Documentation': 'http://pyqaxe.readthedocs.io/',
          'Source': 'https://bitbucket.org/glotzer/pyqaxe'
          },
      python_requires='>=3',
      version=__version__
      )
