#!/usr/bin/env python

import logging

from network import BsonNetworkProtocol, BsonNetworkFactory

__version__ = '0.3.0'

class BsonRouterProtocol(BsonNetworkProtocol):

  def receivedMessage(self, msg):
    self.log('error', 'router received message addressed to it.')

  def receivedControlMessage(self, msg):
    BsonNetworkProtocol.receivedControlMessage(self, msg)
    self.factory.registerClient(self.clientid, self)

  def receivedForwardMessage(self, msg):
    self.factory.forward(msg)

  def close(self):
    self.factory.removeClient(self.clientid)
    BsonNetworkProtocol.close(self)





class BsonRouterFactory(BsonNetworkFactory):

  protocol = BsonRouterProtocol

  def __init__(self, *args, **kwargs):
    super(BsonRouterFactory, self).__init__(*args, **kwargs)
    self.connections_ = {}

  def registerClient(self, clientid, conn):
    connections_open = len(self.connections_)
    if connections_open > self.options.clients:
      self.logging.error( \
        '[router] refused connection to %s (max clients %d)' % \
          (clientid, connections_open)
      conn.close()
      return

    self.logging.info('[router] client connected: %s (%d)' % \
      (clientid, connections_open)
    self.connections_[clientid] = conn

  def removeClient(self, clientid):
    if clientid in self.connections_:
      del self.connections_[clientid]
    self.logging.info('client disconnected: %s' % clientid)

  def forward(self, doc):
    self.logging.info( \
      '[router] forwarding document from %(_src)s to %(_dst)s' % doc)
    clientid = doc['_dst']
    if clientid in self.connections_:
      self.connections_[clientid].sendMessage(doc)
    else:
      self.logging.warning( \
        '[router] dropped document from %(_src)s to %(_dst)s' % doc)





def parseArgs():

  usage = '''usage: %prog [options]

  This the bsonnetwork python router. It accepts and maintains connections
  and routes BSON objects between all connected clients.

  client object must specify:
    _dst : destination client id (string)
    _src : source client id (string)

  client objects should specify:
    _sec : shared secret (must if using -s)
  '''

  from util import arg_parser
  parser = arg_parser(usage)
  return parser.parse_args()


def main():
  options, args = parseArgs()

  fmt='[%(asctime)s][%(levelname)8s][' + options.clientid + '] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  logging.info('options: %s' % options)
  logging.info('Starting BsonNetwork Router v%s on port %d' \
    % (__version__, options.port))

  factory = BsonRouterFactory(options.clientid, options)
  factory.logging = logging

  if options.connect_to:
    logging.info('Starting clients')
    from base import Client
    clients = [Client.spawn(factory, addr) for addr in options.connect_to]

  logging.info('Starting server')
  from base import Server
  server = Server(('', options.port), factory)
  server.serve_forever()


if __name__ == '__main__':
  main()

