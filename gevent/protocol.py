#!/usr/bin/python
'''
bson gevent Server
'''

import logging
from gevent import socket


class Protocol(object):
  '''Twisted-like protocol facility.'''
  def __init__(self, sock, address, factory):
    self.sock = sock
    self.address = address
    self.factory = factory

  def connectionMade(self):
    pass

  def connectionLost(self, reason):
    pass

  def sendData(self, data):
    self.sock.send(data)

  def receivedData(self, data):
    '''Override this to handle data sent to this service.'''
    raise NotImplementedError





class Factory(object):
  '''Twisted-like factory facility.'''

  protocol = Protocol

  def error(self, error):
    logging.error(error)

  def transportReceive(self, connection):
    while True:
      data = None
      data = connection.sock.recv(1024)
      if not data:
        break
      connection.receivedData(data)

  def serverHandler(self, sock, address):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    conn = self.protocol(sock, address, self)
    conn.connectionMade()

    try:
      self.transportReceive(conn)
    except Exception, e:
      self.error(e)

    sock.close()
    conn.connectionLost('Connection Closed')





class EchoProtocol(Protocol):

  def connectionMade(self):
    logging.info('[EchoProtocol] %s:%d connection made' % self.address)

  def connectionLost(self, reason):
    logging.info('[EchoProtocol] %s:%d connection lost' % self.address)

  def receivedData(self, data):
    logging.info('[EchoProtocol] %s:%d received data' % self.address)
    logging.debug('[EchoProtocol] %s' % data)
    self.sendData(data)






if __name__ == '__main__':

  import logging
  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=logging.INFO, format=fmt)

  factory = Factory()
  factory.protocol = EchoProtocol

  from gevent.server import StreamServer
  server = StreamServer(('', 6000), factory.serverHandler)

  print 'Starting echo server on port 6000'
  server.start()

  def sendData(number):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 6000))
#
    for i in range(0, 100):
      line = ('%d' % i) * 64
      s.send(line)
      gevent.sleep(0.001)
      if s.recv(len(line)) != line:
        s.close()
        return False

    s.close()
    gevent.sleep(0.001)
    return True

  import gevent
  jobs = [gevent.spawn(sendData, num) for num in range(0, 100)]
  gevent.joinall(jobs, timeout=15)
  vals = [(1 if job.value else 0) for job in jobs]

  print sum(vals), '/', len(vals), 'succeeded'

