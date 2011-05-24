import os
import sys
import bson
import utils
import socket
import random

from utils import BsonSocket
from test_simple import TestSimpleOne

from subprocess import Popen, PIPE
from nose.tools import *

class TestSimpleCLI(TestSimpleOne):

  def setup(self):
    self.port = 10000
    bson.patch_socket()

    self.socks = {}

  def teardown(self):
    pass

  def waitForOutput(self, output):
    pass


  def receive(self, docs, printall=False):
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      self.waitForOutput('[%(_dst)s] sending document' % doc)

      doc2 = self.socks[doc['_dst']].recvobj()
      if not utils.dicts_equal(doc, doc2):
        print 'Document mismatch!!'
        print doc, docs.index(doc)
        print doc2, docs.index(doc)
        assert(False)


if __name__ == '__main__':

  import sys
  queue = '--no-queue' not in sys.argv

  for member in TestSimpleOne.__dict__:
    if member.startswith('test_'):
      if not queue and 'queue' in member:
        print '----------------- SKIPPED %s -----------------' % member
        continue

      print '----------------- TESTING %s -----------------' % member

      t = TestSimpleCLI()
      t.setup()
      getattr(TestSimpleCLI, member)(t)
      t.teardown()

