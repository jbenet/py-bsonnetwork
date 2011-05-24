import os
import sys
import bson
import utils
import socket
import random

from utils import BsonSocket
from test_simple import TestSimpleOne
from test_cl import TestSimpleCLI

from nose.tools import *

bson.patch_socket()

class TestSimpleStress(TestSimpleCLI):

  NUM_CLIENTS = 100
  NUM_REQUESTS = 1000

  prefix = ''

  def subtest_send_requests_to_clients(self, clients):
    for i in range(0, self.NUM_REQUESTS):
      print 'Request ', i

      elem = utils.random_dict()
      for c1 in clients:
        for c2 in clients:
          elem['_seq'] = i
          elem['_src'] = c1
          elem['_dst'] = c2
          self._TestSimpleOne__send(elem)
          self.receive(elem)


  def test_send_multiple(self):
    def name(extra):
      return 'name_%s_%s' % (self.prefix, str(extra))

    clients = [name(c) for c in range(0, self.NUM_CLIENTS)]
    print 'Connecting ', self.NUM_CLIENTS, 'clients...'
    for c in clients:
      self.connect(c)
      self.identify(c)

    print 'Sending ', self.NUM_REQUESTS, '*',
    print self.NUM_CLIENTS * self.NUM_CLIENTS, 'requests...'

    self.subtest_send_requests_to_clients(clients)

    print 'Disconnecting ', self.NUM_CLIENTS, 'clients...'
    for c in clients:
      self.disconnect(c)

    print 'Done!'

  def test_send_multiple_sequence(self):
    def name(extra):
      return 'name_%s_%s' % (self.prefix, str(extra))

    nextClient = 0
    clients = []
    print 'Connecting ', self.NUM_CLIENTS, 'clients...'

    print 'Cycling over clients...'

    for i in range(0, 50):
      clients = clients.reverse()

      new_clients = [name(c + (i * 50)) for c in range(0, self.NUM_CLIENTS / 2)]
      for c in new_clients:
        self.connect(c)
        self.identify(c)
      clients.extend(new_clients)

      self.subtest_send_requests_to_clients(clients)

      old_clients = clients[self.NUM_CLIENTS / 2 : ]
      for c in old_clients:
        self.disconnect(c)
      clients = clients[ 0 : self.NUM_CLIENTS / 2]

    print 'Disconnecting ', self.NUM_CLIENTS, 'clients...'
    for c in clients:
      self.disconnect(c)

    print 'Done!'


if __name__ == '__main__':

  d = TestSimpleOne.__dict__.copy()
  d.update(TestSimpleStress.__dict__)
  for member in d:
    if member.startswith('test_'):
      print '----------------- TESTING %s -----------------' % member
      t = TestSimpleStress()
      t.prefix = sys.argv[-1]
      t.setup()
      getattr(TestSimpleStress, member)(t)
      t.teardown()

