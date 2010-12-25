#!/usr/bin/env python

import os
import sys
import bson
import logging
import optparse

from bsonprotocol import BsonProtocol

from twisted.protocols.basic import IntNStringReceiver
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ServerFactory

LOG = logging


class BsonRouterQueue(object):

  def __init__(self, maxElements = None):
    self.docs_ = {}
    self.evict_ = []
    self.maxElements = maxElements
    self.numElements = 0

  def __len__(self):
    return self.numElements

  def length(self, clientid):
    if clientid not in self.docs_:
      return 0
    return len(self.docs_[clientid])

  def evict(self, evictions):
    LOG.info('[queue] evicting first %d:' % evictions)
    for e in range(0, evictions):
      LOG.debug('[queue]   evicting %s' % self.evict_[e])
      self.dequeueDoc(self.evict_.pop(e))

  def enqueue(self, clientid, doc):
    if clientid not in self.docs_:
      LOG.info('[queue] %s queue added' % clientid)
      self.docs_[clientid] = []

    LOG.info('[queue] %s queue enqueue' % clientid)
    self.docs_[clientid].append(doc)
    self.evict_.append(clientid)
    self.numElements += 1

    if self.maxElements and self.numElements > self.maxElements:
      self.evict(self.maxElements - self.numElements)

  def dequeue(self, clientid):
    if clientid not in self.docs_:
      LOG.debug('[queue] %s queue dequeue (EMPTY)' % clientid)
      return None

    self.evict_.remove(clientid)
    doc = self.docs_[clientid].pop(0)
    length = len(self.docs_[clientid])
    LOG.info('[queue] %s queue dequeue (%d left)' % (clientid, length))

    if length == 0:
      LOG.info('[queue] %s queue removed' % clientid)
      del self.docs_[clientid]

    self.numElements -= 1
    return doc


class BsonRouterProtocol(BsonProtocol):
  def connectionMade(self):
    self.clientid = None
    LOG.info('[%s] connection made' % self.clientid)

  def connectionLost(self, reason):
    self.close()
    LOG.info('[%s] connection closed' % self.clientid)

  def close(self):
    self.transport.loseConnection()
    if self.clientid:
      self.factory.removeClient(self.clientid)

  def handleDoc(self, doc):
    LOG.info('[%s] handling control message' % self.clientid)
    self.clientid = doc['_src'] # set client id as this source.
    self.factory.registerClient(self.clientid, self)

  def bsonReceived(self, doc):
    LOG.debug('[%s] bson document received' % self.clientid)
    if not self.validDocument(doc):
      LOG.info('[%s] data discarded (invalid document)' % self.clientid)
      return

    LOG.debug('[%s] document parsed %s' % (self.clientid, str(doc)))
    if '_dst' not in doc:
      self.handleDoc(doc)
      return

    self.factory.forward(doc)

  def sendDocument(self, doc):
    LOG.debug('[%s] sending document %s' % (self.clientid, str(doc)))
    try:
      self.sendBson(doc)
    except Exception, e:
      LOG.error('[%s] sending bson document error: %s' % (self.clientid, e))


  def bsonDecodingError(self, error):
    LOG.error('[%s] received bson parsing error: %s' % (self.clientid, e))

  def validDocument(self, doc):
    if '_src' not in doc:
      LOG.error('[%s] bson document invalid: no source id.' % self.clientid)
      return False

    if self.clientid and doc['_src'] != self.clientid:
      LOG.error('[%s] bson document invalid: source id mismatch (%s)'
        % (self.clientid, doc['_src']))
      return False

    if not self.factory.options.secret:
      return True

    if '_sec' not in doc:
      LOG.error('[%s] bson document invalid: no secret' % self.clientid)
      return False

    if doc['_sec'] != self.factory.options.secret:
      LOG.error('[%s] bson document invalid: secret mismatch (%s)'
        % (self.clientid, doc['_sec']))
      return False

    return True


class BsonRouterFactory(ServerFactory):

  protocol = BsonRouterProtocol

  def __init__(self, options):
    self.connections_ = {}
    self.queue_ = BsonRouterQueue(options.queue)
    self.options = options

  def registerClient(self, clientid, conn):
    if len(self.connections_) > self.options.clients:
      LOG.error('[router] refused connection to %s (max clients)' % clientid)
      conn.close()
      return

    LOG.info('[router] client connected: %s' % clientid)
    self.connections_[clientid] = conn
    queued = self.queue_.length(clientid)
    while queued > 0:
      conn.sendDocument(self.queue_.dequeue(clientid))
      queued -= 1

  def removeClient(self, clientid):
    if clientid in self.connections_:
      del self.connections_[clientid]
    LOG.info('client disconnected: %s' % clientid)

  def forward(self, doc):
    LOG.info('[router] forwarding document from %(_src)s to %(_dst)s' % doc)
    clientid = doc['_dst']
    if clientid in self.connections_:
      self.connections_[clientid].sendDocument(doc)
    else:
      self.queue_.enqueue(clientid, doc)


def setupLogger(level=logging.ERROR):

  logger = logging.getLogger("BsonRouter")
  logger.setLevel(level)

  fmt = '[%(asctime)s][%(levelname)8s] %(message)s'
  formatter = logging.Formatter(fmt)

  ch = logging.StreamHandler()
  ch.setLevel(level)
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  global LOG
  LOG = logger
  return logger


class defaults(object):
  port = 0
  queue = 1000
  secret = None
  clients = 100
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
  parser.add_option('-q', '--queue', dest='queue', metavar='number',
    default=defaults.queue, help='max number of queued documents')
  parser.add_option('-l', '--logging', dest='logging', metavar='log level',
    default=defaults.logging, help='logging level')

  options, args = parser.parse_args()

  options.port = int(options.port)
  options.clients = int(options.clients)
  options.queue = int(options.queue)

  loglevels = {'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

  options.logging = loglevels.get(options.logging, logging.NOTSET)

  return options, args


def main():
  options, args = parseArgs()

  setupLogger(options.logging)
  factory = BsonRouterFactory(options)

  from twisted.internet import reactor

  port = reactor.listenTCP(options.port, factory)

  LOG.info('Starting BsonNetwork Router on %s' % port.getHost())
  reactor.run()


if __name__ == '__main__':
  main()

