#!/usr/bin/env python

from setuptools import setup

# don't forget to change it in bsonnetwork.__init__.py
__version__ = '0.3.5'


setup(
  name="bsonnetwork",
  version=__version__,
  description="BsonNetwork python networking library",
  author="Juan Batiz-Benet",
  author_email="jbenet@cs.stanford.com",
  url="http://github.com/jbenet/bsonnetwork",
  keywords=["bsonnetwork", "bson", "networking library"],
  packages=["bsonnetwork"],
  install_requires=["bson", "gevent"],
  license="MIT License"
)
