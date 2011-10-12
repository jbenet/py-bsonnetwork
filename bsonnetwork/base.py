#!/usr/bin/python
'''
bson gevent Server
'''

import logging
import gevent
import traceback

from gevent import socket
from gevent.server import StreamServer
from gevent.queue import Queue

from util.sockutil import set_tcp_keepalive

class Transport(object):
  '''Greenlet-safe socket wrapper to emulate twisteds Transport'''
  __slots__ = ('sock', 'queue', 'send_greenlet')

  def __init__(self, socket):
    self.sock = socket
    self.queue = Queue(maxsize=1000) # maxsize just in case...
    self.send_greenlet = gevent.spawn(self._sendloop)

  def _sendloop(self):
    '''The need for this sendloop is that multiple greenlets may attempt to
    call `write` simultaneously. Thus, the call to `sock.sendall` must be
    protected. This could instead be achieved with a Semaphore, but that would
    perhaps block high priority greenlets. The approach here, using a send
    queue, seems to be the best way to keep faulty or slow sockets from
    affecting others.
    '''
    while True:
      self.sock.sendall(self.queue.get())


  def write(self, data):
    self.queue.put(data)

  def read(self, bytes):
    return self.sock.recv(bytes)

  def loseConnection(self):
    self.send_greenlet.kill()
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
    self.socket = self.configured_socket(family, type)
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
  def configured_socket(cls, family, type):
    sock = socket.socket(family, type)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    return sock

  @classmethod
  def spawn(cls, factory, *args):
    c = cls(factory)
    gevent.spawn(c.connect, *args)
    return c



class PersistentClient(Client):

  __slots__ = Client.__slots__ + ('persist', )

  def __init__(self, factory):
    super(PersistentClient, self).__init__(factory)
    self.persist = False

  def connect(self, address, family=socket.AF_INET, type=socket.SOCK_STREAM):
    self.persist = True
    while self.persist:
      super(PersistentClient, self).connect(address, family, type)
      gevent.sleep(1)

  def disconnect(self):
    self.persist = False
    super(PersistentClient, self).disconnect()

  @classmethod
  def configured_socket(cls, family, type):
    sock = super(PersistentClient, cls).configured_socket(family, type)
    set_tcp_keepalive(sock, tcp_keepidle=5, tcp_keepcnt=5, tcp_keepintvl=1)
    return sock



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


