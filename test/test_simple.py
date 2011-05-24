import os
import sys
import bson
import utils
import socket
import random

from utils import BsonSocket

from subprocess import Popen, PIPE
from nose.tools import *

class TestSimpleOne:

  def setup(self):
    self.port = random.randint(10000, 20000)
    bson.patch_socket()

    self.socks = {}

    cmd = 'twisted/bsonrouter.py -p %d -l debug' % self.port
    self.router = Popen(cmd, shell=True, stderr=PIPE)
    self.waitForOutput('Starting BsonNetwork Router')

  def teardown(self):
    self.router.kill()

  def waitForOutput(self, output):
    print '===> Waiting for: %s' % output
    while True:
      line = self.router.stderr.readline()
      print line.strip()
      if output in line:
        return

  def connect(self, clientid):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('', self.port))
    self.socks[clientid] = sock
    self.waitForOutput('connection made')
    self.waitForOutput('sending identification message')
    self.socks[clientid].recvobj()
    return sock

  def disconnect(self, clientid):
    sock = self.socks[clientid]
    sock.close()
    del self.socks[clientid]
    self.waitForOutput('connection closed')

  def identify(self, clientid):
    self.socks[clientid].sendobj( { '_src' : clientid } )
    self.waitForOutput('client connected: %s' % clientid)

  def __send(self, doc):
    self.socks[doc['_src']].sendobj(doc)

  def receive(self, docs, printall=False):
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      self.waitForOutput('[%(_dst)s] sending document' % doc)
      while printall:
        print self.router.stderr.readline().strip()

      doc2 = self.socks[doc['_dst']].recvobj()
      if not utils.dicts_equal(doc, doc2):
        print 'Document mismatch!!'
        print doc
        print doc2
        assert(False)

  def __send_and_receive(self, src, dst, doc):
    doc['_src'] = src
    doc['_dst'] = dst
    self.__send(doc)
    self.receive(doc)

  def test_connect(self):
    self.connect('A')
    self.disconnect('A')

  def test_identify(self):
    self.connect('A')
    self.identify('A')
    self.disconnect('A')

  def test_connect_two(self):
    self.connect('A')
    self.connect('B')
    self.disconnect('A')
    self.disconnect('B')

  def test_identify_two(self):
    self.connect('A')
    self.connect('B')
    self.identify('B')
    self.identify('A')
    self.disconnect('A')
    self.disconnect('B')

  def test_send_self(self):
    self.connect('A')
    self.identify('A')

    self.__send_and_receive('A', 'A', {'herp' : 'derp'})

    self.disconnect('A')

  def test_send_simple(self):
    self.connect('A')
    self.connect('B')
    self.identify('B')
    self.identify('A')

    self.__send_and_receive('A', 'B', {'herp' : 'derp'})

    self.disconnect('A')
    self.disconnect('B')

  def test_send_more(self):
    self.connect('A')
    self.connect('B')
    self.identify('B')
    self.identify('A')

    seq = map(lambda x: utils.random_dict(), range(0, 10))
    for elem in seq:
      elem['_seq'] = seq.index(elem)
      elem['_src'] = 'A'
      elem['_dst'] = 'B'
      self.__send(elem)
    self.receive(seq)

    self.disconnect('A')
    self.disconnect('B')

  def test_send_much(self):
    self.connect('A')
    self.connect('B')
    self.identify('B')
    self.identify('A')

    for pair in [('A', 'B'), ('B', 'A'), ('A', 'A'), ('B', 'B')]:
      seq = map(lambda x: utils.random_dict(), range(0, 10))
      for elem in seq:
        elem['_seq'] = seq.index(elem)
        elem['_src'] = pair[0]
        elem['_dst'] = pair[1]
        self.__send(elem)
      self.receive(seq)

    self.disconnect('A')
    self.disconnect('B')

  def test_enqueue_simple(self):
    self.connect('A')
    self.identify('A')
    doc = utils.random_dict()
    doc['_src'] = 'A'
    doc['_dst'] = 'B'
    doc['_que'] = True
    self.__send(doc)
    self.disconnect('A')

    self.connect('B')
    self.identify('B')
    self.receive(doc)
    self.disconnect('B')

  def test_enqueue_many(self):
    self.connect('A')
    self.identify('A')
    seq = map(lambda x: utils.random_dict(), range(0, 10))
    for elem in seq:
      elem['_seq'] = seq.index(elem)
      elem['_src'] = 'A'
      elem['_dst'] = 'B'
      elem['_que'] = True
      self.__send(elem)

    self.disconnect('A')

    self.connect('B')
    self.identify('B')
    self.receive(seq)
    self.disconnect('B')

  def test_enqueue_many_more(self):
    self.test_enqueue_many()
    self.test_enqueue_many()
    self.test_enqueue_many()
    self.test_enqueue_many()



if __name__ == '__main__':

  for member in TestSimpleOne.__dict__:
    if member.startswith('test_'):
      print '----------------- TESTING %s -----------------' % member
      t = TestSimpleOne()
      t.setup()
      getattr(TestSimpleOne, member)(t)
      t.teardown()

