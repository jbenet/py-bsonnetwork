
import unittest
import utils

from bsonnetwork.util.test import BsonNetworkProcess


def clientid(num):
  return 'client%d' % int(num)



class TestRouterStress(unittest.TestCase):

  NUM_CLIENTS = 1000
  NUM_MESSAGES = 10

  clients = [clientid(c) for c in range(0, NUM_CLIENTS)]

  def setUp(self):
    self.r = BsonNetworkProcess('python bsonnetwork/router.py -i router')
    self.r.start()

  def tearDown(self):
    self.r.stop()
    del self.r

  def test_connect(self):
    map(self.r.connect, self.clients)
    map(self.r.identify, self.clients)
    map(self.r.disconnect, self.clients)

  def test_send_self(self):
    map(self.r.connect, self.clients)
    map(self.r.identify, self.clients)
    for client in self.clients:
      self.r.send_and_receive(client, client, {'herp' : 'derp'})
    map(self.r.disconnect, self.clients)

  def test_send_simple(self):
    map(self.r.connect, self.clients)
    map(self.r.identify, self.clients)
    for c in range(0, self.NUM_CLIENTS - 1):
      self.r.send_and_receive(clientid(c), clientid(c + 1), {'herp' : 'derp'})
    map(self.r.disconnect, self.clients)

  def test_send_more(self):
    map(self.r.connect, self.clients)
    map(self.r.identify, self.clients)
    msg = utils.random_dict()
    for c in range(0, self.NUM_CLIENTS - 1):
      self.r.send_and_receive(clientid(c), clientid(c + 1), msg)
    map(self.r.disconnect, self.clients)

  def test_send_much(self):
    map(self.r.connect, self.clients)
    map(self.r.identify, self.clients)
    for i in range(0, self.NUM_MESSAGES):
      msg = utils.random_dict()
      for c in range(0, self.NUM_CLIENTS - 1):
        self.r.send_and_receive(clientid(c), clientid(c + 1), msg)
    map(self.r.disconnect, self.clients)

  def test_churn(self):
    self.test_connect()
    self.test_send_self()
    self.test_send_simple()
    self.test_send_more()

if __name__ == '__main__':
  unittest.main()
