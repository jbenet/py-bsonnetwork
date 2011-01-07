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

    cmd = './bsonrouter.py -p %d -l debug' % self.port
    self.router = Popen(cmd, shell=True, stderr=PIPE)
    self.__waitForOutput('Starting BsonNetwork Router')

  def teardown(self):
    self.router.kill()

  def __waitForOutput(self, output):
    print '===> Waiting for: %s' % output
    while True:
      line = self.router.stderr.readline()
      print line.strip()
      if output in line:
        return

  def __connect(self, clientid):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('', self.port))
    self.socks[clientid] = sock
    self.__waitForOutput('connection made')
    self.__waitForOutput('sending identification message')
    self.socks[clientid].recvobj()
    return sock

  def __disconnect(self, clientid):
    sock = self.socks[clientid]
    sock.close()
    del self.socks[clientid]
    self.__waitForOutput('connection closed')

  def __identify(self, clientid):
    self.socks[clientid].sendobj( { '_src' : clientid } )
    self.__waitForOutput('client connected: %s' % clientid)

  def __send(self, doc):
    self.socks[doc['_src']].sendobj(doc)

  def __receive(self, docs, printall=False):
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      self.__waitForOutput('[%(_dst)s] sending document' % doc)
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
    self.__receive(doc)

  def test_connect(self):
    self.__connect('A')
    self.__disconnect('A')

  def test_identify(self):
    self.__connect('A')
    self.__identify('A')
    self.__disconnect('A')

  def test_connect_two(self):
    self.__connect('A')
    self.__connect('B')
    self.__disconnect('A')
    self.__disconnect('B')

  def test_identify_two(self):
    self.__connect('A')
    self.__connect('B')
    self.__identify('B')
    self.__identify('A')
    self.__disconnect('A')
    self.__disconnect('B')

  def test_send_self(self):
    self.__connect('A')
    self.__identify('A')

    self.__send_and_receive('A', 'A', {'herp' : 'derp'})

    self.__disconnect('A')

  def test_send_simple(self):
    self.__connect('A')
    self.__connect('B')
    self.__identify('B')
    self.__identify('A')

    self.__send_and_receive('A', 'B', {'herp' : 'derp'})

    self.__disconnect('A')
    self.__disconnect('B')

  def test_send_more(self):
    self.__connect('A')
    self.__connect('B')
    self.__identify('B')
    self.__identify('A')

    seq = map(lambda x: utils.random_dict(), range(0, 10))
    for elem in seq:
      elem['_seq'] = seq.index(elem)
      elem['_src'] = 'A'
      elem['_dst'] = 'B'
      self.__send(elem)
    self.__receive(seq)

    self.__disconnect('A')
    self.__disconnect('B')

  def test_send_much(self):
    self.__connect('A')
    self.__connect('B')
    self.__identify('B')
    self.__identify('A')

    for pair in [('A', 'B'), ('B', 'A'), ('A', 'A'), ('B', 'B')]:
      seq = map(lambda x: utils.random_dict(), range(0, 10))
      for elem in seq:
        elem['_seq'] = seq.index(elem)
        elem['_src'] = pair[0]
        elem['_dst'] = pair[1]
        self.__send(elem)
      self.__receive(seq)

    self.__disconnect('A')
    self.__disconnect('B')

  def test_enqueue_simple(self):
    self.__connect('A')
    self.__identify('A')
    doc = utils.random_dict()
    doc['_src'] = 'A'
    doc['_dst'] = 'B'
    self.__send(doc)
    self.__disconnect('A')

    self.__connect('B')
    self.__identify('B')
    self.__receive(doc)
    self.__disconnect('B')

  def test_enqueue_many(self):
    self.__connect('A')
    self.__identify('A')
    seq = map(lambda x: utils.random_dict(), range(0, 10))
    for elem in seq:
      elem['_seq'] = seq.index(elem)
      elem['_src'] = 'A'
      elem['_dst'] = 'B'
      self.__send(elem)

    self.__disconnect('A')

    self.__connect('B')
    self.__identify('B')
    self.__receive(seq)
    self.__disconnect('B')

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

