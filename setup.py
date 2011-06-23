#!/usr/bin/env python

from setuptools import setup

import bsonnetwork
# not customary, but if this doesn't work, then why bother installing?


setup(
  name="bsonnetwork",
  version=bsonnetwork.__version__,
  description="BsonNetwork python networking library",
  author="Juan Batiz-Benet",
  author_email="jbenet@cs.stanford.com",
  url="http://github.com/jbenet/bsonnetwork",
  keywords=["bsonnetwork", "bson", "networking library"],
  packages=["bsonnetwork"],
  install_requires=["bson", "gevent"],
  license="MIT License"
)
