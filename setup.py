#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from setuptools import setup

try:
    from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
    # 2.x
    from distutils.command.build_py import build_py

setup(name="datapyle",
      version="2011.1",
      description="Tools and libraries for gathering and analyzing data",
      long_description=open("README.rst", "rt").read(),
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Intended Audience :: Other Audience',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Programming Language :: Python',
          'Topic :: Scientific/Engineering',
          'Topic :: Scientific/Engineering :: Information Analysis',
          'Topic :: Scientific/Engineering :: Mathematics',
          'Topic :: Scientific/Engineering :: Visualization',
          'Topic :: Software Development :: Libraries',
          'Topic :: Utilities',
          ],

      author="Andreas Kloeckner",
      url="http://pypi.python.org/pypi/datapyle",
      scripts=[
          "bin/logtool",
          "bin/runalyzer-gather",
          "bin/runalyzer",
          "bin/couch-queue"],
      author_email="inform@tiker.net",
      license="MIT",
      packages=["datapyle"],

      install_requires=[
          "pytools>=2011.1",
          ],

      # 2to3 invocation
      cmdclass={'build_py': build_py})
