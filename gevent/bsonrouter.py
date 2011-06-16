#!/usr/bin/env python

import logging
import optparse

from bsonnetwork import BsonNetworkProtocol, BsonNetworkFactory

version = '0.2.24'

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
    if len(self.connections_) > self.options.clients:
      self.logging.error( \
        '[router] refused connection to %s (max clients)' % clientid)
      conn.close()
      return

    self.logging.info('[router] client connected: %s' % clientid)
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



class defaults(object):
  port = 0
  secret = None
  clients = 10000
  logging = 'error'
  verbose = False



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

  parser = optparse.OptionParser(usage)
  parser.add_option('-v', '--verbose', dest='verbose', metavar='verbose',
    default=defaults.verbose, help='whether to output test urls')
  parser.add_option('-p', '--port', dest='port', metavar='port',
    default=defaults.port, help='the port to listen on')
  parser.add_option('-s', '--secret', dest='secret', metavar='string',
    default=defaults.secret, help='shared secret to authenticate clients')
  parser.add_option('-c', '--clients', dest='clients', metavar='number',
    default=defaults.clients, help='max number of connected clients')
  parser.add_option('-l', '--logging', dest='logging', metavar='log level',
    default=defaults.logging, help='logging level')

  options, args = parser.parse_args()

  options.port = int(options.port)
  options.clients = int(options.clients)

  loglevels = {'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

  options.logging = loglevels.get(options.logging, logging.NOTSET)

  return options, args


def main():

  options, args = parseArgs()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  factory = BsonRouterFactory('$router', options)
  factory.logging = logging

  logging.info('Starting BsonNetwork Router v%s on port %d' \
    % (version, options.port))
  logging.info('options: %s' % options)

  from gevent.server import StreamServer
  server = StreamServer(('', options.port), factory.serverHandler)
  server.serve_forever()


if __name__ == '__main__':
  main()

