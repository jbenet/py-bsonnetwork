
import unittest
import utils

from bsonnetwork.echo import flipMessage
from bsonnetwork.util.test import BsonNetworkProcess


from subprocess import Popen, PIPE
from nose.tools import *

class TestConnect(unittest.TestCase):

  def test_simple(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py') as d:
      pass

  def test_single(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py') as d:
      d.connect('herp')
      d.identify('herp')
      d.disconnect('herp')

  def test_double(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py') as d:
      d.connect('herp')
      d.connect('derp')
      d.identify('herp')
      d.identify('derp')
      d.disconnect('herp')
      d.disconnect('derp')

  def test_many(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py') as d:
      for i in range(0, 100):
        d.connect('client%d' % i)
      for i in range(0, 100):
        d.identify('client%d' % i)
      for i in range(0, 100):
        d.disconnect('client%d' % i)


class TestEcho(unittest.TestCase):

  def subtest_send_receive(self, d, clientid, msg):
    msg['_src'] = clientid
    msg['_dst'] = 'echoer'
    d.send(msg)
    d.receive(flipMessage(msg))

  def test_single(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py -i echoer') as d:
      d.connect('herp')
      d.identify('herp')
      self.subtest_send_receive(d, 'herp', utils.random_dict())
      d.disconnect('herp')

  def test_single_many(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py -i echoer') as d:
      d.connect('herp')
      d.identify('herp')
      for i in range(0, 100):
        self.subtest_send_receive(d, 'herp', utils.random_dict())
      d.disconnect('herp')

  def test_double(self):
    with BsonNetworkProcess('python bsonnetwork/echo.py -i echoer') as d:
      d.connect('herp')
      d.connect('derp')
      d.identify('herp')
      d.identify('derp')
      for i in range(0, 100):
        self.subtest_send_receive(d, 'herp', utils.random_dict())
        self.subtest_send_receive(d, 'derp', utils.random_dict())
      d.disconnect('herp')
      d.disconnect('derp')

  def test_many(self):
    clients = 100
    with BsonNetworkProcess('python bsonnetwork/echo.py -i echoer') as d:
      clientid = lambda num: 'client%d' % num
      for i in range(0, clients):
        d.connect(clientid(i))
      for i in range(0, clients):
        d.identify(clientid(i))
      for i in range(0, clients):
        for m in range(0, 100):
          self.subtest_send_receive(d, clientid(i), utils.random_dict())
      for i in range(0, clients):
        d.disconnect(clientid(i))




if __name__ == '__main__':
  unittest.main()
