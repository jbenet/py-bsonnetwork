
import unittest

from bsonnetwork.test import BsonNetworkProcess

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




if __name__ == '__main__':
  unittest.main()
