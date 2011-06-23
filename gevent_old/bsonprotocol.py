#!/usr/bin/python
'''
bson gevent Server
'''

import logging

from protocol import Protocol, Factory
from gevent import socket

import bson
bson.patch_socket(socket.socket)




class BsonProtocol(Protocol):

  def sendBson(self, message):
    self.sock.sendobj(message)

  def receivedBson(self, message):
    '''Override this to handle a message addressed to this service.'''
    raise NotImplementedError





class BsonFactory(Factory):
  '''Twisted-like factory facility.'''

  protocol = BsonProtocol

  def transportReceive(self, connection):
    while True:
      message = None
      message = connection.sock.recvobj()
      if not message:
        break
      connection.receivedBson(message)





class BsonEchoProtocol(BsonProtocol):

  def connectionMade(self):
    logging.info('[BsonEchoProtocol] %s:%d connection made' % self.address)

  def connectionLost(self, reason):
    logging.info('[BsonEchoProtocol] %s:%d connection lost' % self.address)

  def receivedBson(self, message):
    logging.info('[BsonEchoProtocol] %s:%d received message' % self.address)
    logging.debug('[BsonEchoProtocol] %s' % message)
    self.sendBson(message)





if __name__ == '__main__':

  import logging
  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=logging.INFO, format=fmt)

  factory = BsonFactory()
  factory.protocol = BsonEchoProtocol

  from gevent.server import StreamServer
  server = StreamServer(('', 6000), factory.serverHandler)

  print 'Starting bson echo server on port 6000'
  server.start()

  def sendMessage(number):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 6000))

    for i in range(0, 100):
      message = {'line' : ('%d' % i) * 64 }
      s.sendobj(message)
      gevent.sleep(0.001)
      if s.recvobj() != message:
        s.close()
        return False

    s.close()
    gevent.sleep(0.001)
    return True

  import gevent
  jobs = [gevent.spawn(sendMessage, num) for num in range(0, 100)]
  gevent.joinall(jobs, timeout=15)
  vals = [(1 if job.value else 0) for job in jobs]

  print sum(vals), '/', len(vals), 'succeeded'


