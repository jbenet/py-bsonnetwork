
import unittest
import random
import utils

from bsonnetwork.util.test import BsonNetworkProcess as BNProcess

class TestRouter(unittest.TestCase):

  def test_connect(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.identify('A')
      r.disconnect('A')

  def test_connect_two(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')
      r.disconnect('A')
      r.disconnect('B')

  def test_reidentify(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')
      r.send_and_receive('A', 'B', {'herp' : 'derp'})
      r.reidentify('A', 'C')
      r.send_and_receive('C', 'B', {'herp' : 'derp'})
      r.reidentify('B', 'D')
      r.send_and_receive('C', 'D', {'herp' : 'derp'})
      r.disconnect('C')
      r.disconnect('D')

  def test_send_self(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.identify('A')
      r.send_and_receive('A', 'A', {'herp' : 'derp'})
      r.disconnect('A')

  def test_send_simple(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
      r.connect('A')
      r.connect('B')
      r.identify('A')
      r.identify('B')
      r.send_and_receive('A', 'B', {'herp' : 'derp'})
      r.disconnect('A')
      r.disconnect('B')

  def test_send_more(self):
    with BNProcess('python bsonnetwork/router.py -i router') as r:
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
    with BNProcess('python bsonnetwork/router.py -i router') as r:
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

def nodeid(num):
  return 'node%d' % int(num)


class TestTopology(unittest.TestCase):

  NODE_COUNT = 6
  NODE_LINKS = []

  def setUp(self):
    ports = self.randomPorts(self.NODE_COUNT)
    cmds = [self.cmd(i, ports) for i in range(0, self.NODE_COUNT)]
    self.nodes = [BNProcess(cmd) for cmd in cmds]
    self.map(BNProcess.start)

  def tearDown(self):
    self.map(BNProcess.stop)
    del self.nodes


  @staticmethod
  def randomPorts(number):
    return random.sample(xrange(10000, 65000), number)

  def cmd(self, r, ports, filename='bsonnetwork/router.py'):
    '''returns the appropriate command for node r'''
    cmd = 'python %s -i %s -p %d' % (filename, nodeid(r), ports[r])

    if r in self.NODE_LINKS:
      connect = map(lambda r2: 'localhost:%d' % ports[r2], self.NODE_LINKS[r])
      cmd += ' --connect-to ' + ','.join(connect)

    return cmd


  def map(self, function, excepting=None):
    '''Map a function over all nodes, except those in `excepting`'''
    if excepting is None:
      nodes = self.nodes
    else:
      nodes = filter(lambda r: r not in excepting, self.nodes)
    map(function, nodes)

  def pair_map(self, function, distinct=False):
    '''Map a function over every pair of nodes.'''
    if distinct:
      fn = lambda n1: self.map(lambda n2: function(n1, n2), excepting=[n1])
    else:
      fn = lambda n1: self.map(lambda n2: function(n1, n2))
    map(fn, self.nodes)

  def link_map(self, function):
    for r1 in self.NODE_LINKS:
      for r2 in self.NODE_LINKS[r1]:
        function(self.nodes[r1], self.nodes[r2])

  def test_connectivity(self):
    def connectivity(node):
      node.connect('client')
      node.identify('client')
      node.disconnect('client')
    self.map(connectivity)

  def test_links(self):
    def linked(r1, r2):
      r1.connect(r2.clientid, trigger=False)
      r1.identify(r2.clientid, trigger=False)
      r2.connect(r1.clientid, trigger=False)
      r2.identify(r1.clientid, trigger=False)
    self.link_map(linked)

  def test_disconnect(self):
    self.test_links()
    self.map(lambda r: r.proc.kill())

    # Note: wont be observed because processes are killed. catch SIGKILL?
    # def disconnect(r1, r2):
    #   r1.disconnect(r2.clientid, trigger=False)
    #   r2.disconnect(r1.clientid, trigger=False)
    # self.link_map(disconnect)

    self.map(BNProcess.stop)
    self.map(BNProcess.start)
    self.test_links()



class SimpleTopology(TestTopology):
  NODE_COUNT = 2
  NODE_LINKS = { 1:[0] }



class LineTopology(TestTopology):
  NODE_COUNT = 6
  NODE_LINKS = { 1:[0], 2:[1], 3:[2], 4:[3], 5:[4] }



class TestStarTopology(TestTopology):
  NODE_COUNT = 6
  NODE_LINKS = { 5 : range(0, 5) }



class TestMeshTopology(TestTopology):
  NODE_COUNT = 6
  NODE_LINKS = { 1 : range(0, 1), \
                 2 : range(0, 2), \
                 3 : range(0, 3), \
                 4 : range(0, 4), \
                 5 : range(0, 5) }






if __name__ == '__main__':
  unittest.main()
