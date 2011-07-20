#!/usr/bin/python
'''
bson gevent Server
'''

import logging
import gevent
import traceback

from gevent import socket
from gevent.server import StreamServer




class Transport(object):
  '''Socket wrapper to emulate twisteds Transport'''
  __slots__ = ('sock')

  def __init__(self, socket):
    self.sock = socket

  def write(self, data):
    self.sock.sendall(data)

  def read(self, bytes):
    return self.sock.recv(bytes)

  def loseConnection(self):
    self.sock.close()





class Protocol(object):
  '''Twisted-like protocol facility.'''

  def __init__(self, transport, address, factory):
    self.transport = transport
    self.address = address
    self.factory = factory

  def connectionMade(self):
    pass

  def connectionLost(self, reason):
    pass

  def sendData(self, data):
    self.transport.write(data)

  def receivedData(self, data):
    '''Override this to handle data sent to this service.'''
    raise NotImplementedError





class Factory(object):
  '''Twisted-like factory facility.'''

  protocol = Protocol

  def error(self, error):
    logging.error(error)

  def transportRead(self, connection):
    while True:
      data = None
      data = connection.transport.read(1024)
      if not data:
        break
      connection.receivedData(data)

  def handler(self, sock, address, client=None):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    transport = Transport(sock)
    conn = self.protocol(transport, address, self)
    conn.connectionMade()

    if client:
      client.connection = conn

    try:
      self.transportRead(conn)
    except Exception, e:
      errstr = 'unknown error: %s \n %s'
      self.error(errstr % (str(e), str(traceback.format_exc())) )

    transport.loseConnection()
    conn.connectionLost('Connection Closed')



class Server(object):
  __slots__ = ('server')
  def __init__(self, address, factory):
    self.server = StreamServer(address, factory.handler)

  def serve_forever(self):
    self.server.serve_forever()

  def serve(self):
    self.server.start()




class Client(object):
  __slots__ = ('factory', 'socket', 'connection')

  def __init__(self, factory):
    self.factory = factory
    self.socket = None
    self.connection = None

  def connect(self, address, family=socket.AF_INET, type=socket.SOCK_STREAM):
    self.socket = socket.socket(family, type)
    try:
      self.socket.connect(address)
      self.factory.handler(self.socket, address, client=self)
    except Exception, e:
      self.factory.error(e)
    self.socket = None

  def disconnect(self):
    if self.socket:
      self.socket.close()
    self.socket = None

  def send(self, data):
    self.socket.send(data)

  @classmethod
  def spawn(cls, factory, *args):
    c = cls(factory)
    gevent.spawn(c.connect, *args)
    return c



if __name__ == '__main__':
  '''Testing'''

  import util
  import logging
  from util.test import testFactory
  from echo import EchoProtocol

  parser = util.arg_parser('usage: %prog [options]', logging=logging.WARNING)
  options, args = parser.parse_args()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  factory = Factory()
  factory.protocol = EchoProtocol

  data = 'TestMessageFEWFSDVFSDR@#R#$@$#@%$Y^U&*&(^%$#@^&IRUYHTGSFDAFVD)'
  data = [data for i in range(0, 100)]
  testFactory(factory, data)


