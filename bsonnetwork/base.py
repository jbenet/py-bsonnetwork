#!/usr/bin/python
'''
bson gevent Server
'''

import logging

from gevent import socket
from gevent.server import StreamServer




class Transport(object):
  '''Socket wrapper to emulate twisteds Transport'''
  __slots__ = ('sock')

  def __init__(self, socket):
    self.sock = socket

  def write(self, data):
    self.sock.send(data)

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

  def transportReceive(self, connection):
    while True:
      data = None
      data = connection.transport.read(1024)
      if not data:
        break
      connection.receivedData(data)

  def serverHandler(self, sock, address):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    transport = Transport(sock)
    conn = self.protocol(transport, address, self)
    conn.connectionMade()

    try:
      self.transportReceive(conn)
    except Exception, e:
      self.error(e)

    transport.loseConnection()
    conn.connectionLost('Connection Closed')






class Server(object):
  __slots__ = ('server')
  def __init__(self, address, factory):
    self.server = StreamServer(address, factory.serverHandler)

  def serve_forever(self):
    self.server.serve_forever()

  def serve(self):
    self.server.start()






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


