
import unittest
import utils

from bsonnetwork.util.test import BsonNetworkProcess

class TestRouter(unittest.TestCase):

  def test_connect(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.identify('A')
      r.disconnect('A')

  def test_connect_two(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')
      r.disconnect('A')
      r.disconnect('B')

  def test_send_self(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.identify('A')
      r.send_and_receive('A', 'A', {'herp' : 'derp'})
      r.disconnect('A')

  def test_send_simple(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')
      r.send_and_receive('A', 'B', {'herp' : 'derp'})
      r.disconnect('A')
      r.disconnect('B')

  def test_send_more(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')

      seq = [utils.random_dict() for i in range(0, 10)]
      r.send_and_receive('A', 'B', seq)
      r.send_and_receive('B', 'A', seq)

      r.disconnect('A')
      r.disconnect('B')


  def test_send_much(self):
    with BsonNetworkProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')

      for pair in [('A', 'B'), ('B', 'A'), ('A', 'A'), ('B', 'B')]:
        seq = [utils.random_dict() for i in range(0, 50)]
        r.send_and_receive(pair[0], pair[1], seq)
        r.send_and_receive(pair[0], pair[1], seq)
        r.send_and_receive(pair[0], pair[1], seq)
        r.send_and_receive(pair[0], pair[1], seq)

      r.disconnect('A')
      r.disconnect('B')


def clientid(num):
  return 'client%d' % int(num)


if __name__ == '__main__':
  unittest.main()
