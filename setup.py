#!/usr/bin/env python

from setuptools import setup

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
      install_requires=[],
      license='BSD',
      long_description='',
      packages=[
          'pyqaxe'
      ],
      project_urls={
          'Documentation': 'http://pyqaxe.readthedocs.io/',
          'Source': 'https://bitbucket.org/glotzer/pyqaxe'
          },
      python_requires='>=3',
      version=__version__
      )
