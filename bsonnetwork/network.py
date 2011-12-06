#!/usr/bin/python
'''
bson gevent bson network
'''

__version__ = '0.2'

import logging
import nanotime

from base import PersistentClient
from protocol import BsonProtocol, BsonFactory

class BsonNetworkProtocol(BsonProtocol):
  '''BsonNetwork Protocol'''

  def log(self, level, message):
    if not self.factory.logging or not hasattr(self.factory.logging, level):
      return

    logfn = getattr(self.factory.logging, level)
    logfn('[BN] [%s] %s' % (self.clientid, message))

  def connectionMade(self):
    self.clientid = None
    self.log('debug', 'connection made')

    # send identification message
    self.log('debug', 'sending identification message')
    self.forwardMessage({'_src' : self.factory.clientid})

  def connectionLost(self, reason):
    self.close()

  def close(self):
    self.log('info', 'connection closed')
    self.transport.loseConnection()

  def receivedMessage(self, msg):
    '''Override this to handle a packet addressed to this service.'''
    raise NotImplementedError

  def receivedControlMessage(self, msg):
    '''Identify sender by default. Also, respond to echoaddress.'''

    if '_dst' not in msg:
      self.clientid = msg['_src']
      self.log('debug', 'connection identified')

    response = {}
    if 'echoaddress' in msg:
      response['echoaddress'] = '%s:%d' % self.address

    if len(response) > 0:
      response['_dst'] = msg['_src']
      self.sendMessage(response)


  def receivedForwardMessage(self, msg):
    '''Drop bson docs not for us by default.'''
    pass

  def receivedBson(self, doc):
    self.log('debug', 'bson document received')
    if not self.validMessage(doc):
      self.log('debug', 'data discarded (invalid document)')
      return

    self.log('debug', 'document parsed %s' % str(doc))
    self.lastRecvTime = nanotime.nanotime.now()

    if '_dst' not in doc:
      self.log('debug', 'handling identification message')
      self.receivedControlMessage(doc)
      return

    if doc['_dst'] == self.factory.clientid and '_ctl' in doc:
      self.log('debug', 'handling control message')
      self.receivedControlMessage(doc)
      return

    if doc['_dst'] != self.factory.clientid:
      self.receivedForwardMessage(doc)
      return

    self.receivedMessage(doc)

  def forwardMessage(self, doc):
    self.log('debug', 'sending document %s' % str(doc))
    try:
      self.sendBson(doc)
      self.lastSendTime = nanotime.nanotime.now()
    except Exception, e:
      self.log('error', 'sending bson document error: %s' % e)

  def sendMessage(self, doc, src=None, dst=None):
    if '_src' not in doc or src:
      doc['_src'] = src or self.factory.clientid
    if '_dst' not in doc or dst:
      doc['_dst'] = dst or self.clientid
    self.forwardMessage(doc)

  def bsonDecodingError(self, error):
    errstr ='received bson parsing error: %s (bson data length: %d)'
    self.log('error', errstr % (error, len(error.bsonData)))

  def validMessage(self, doc):
    invalid = 'bson document invalid: %s'
    if '_src' not in doc:
      self.log('error', invalid % 'no source id.')
      return False

    # if self.clientid and doc['_src'] != self.clientid:
    #   self.log('error', invalid % 'source id mismatch (%s)' % doc['_src'])
    #   return False

    opts = self.factory.options
    if not opts or not hasattr(opts, 'secret') or not opts.secret:
      return True

    if '_sec' not in doc:
      self.log('error', invalid % 'no secret')
      return False

    if doc['_sec'] != opts.secret:
      self.log('error', invalid % 'secret mismatch (%s)' % doc['_sec'])
      return False

    return True




class BsonNetworkFactory(BsonFactory):

  def __init__(self, clientid, options):
    logging.info('Starting BsonNetwork v%s process with id %s on port %d' % \
      (__version__, clientid, options.port))
    self.clientid = clientid
    self.options = options
    if hasattr(options, 'logging'):
      self.logging = options.logging



class BsonNetworkPersistentClient(PersistentClient):

  def connect(self, *args, **kwargs):
    self.keepalive_greenlet = gevent.spawn(self._keepalive_loop)
    super(BsonNetworkPersistentClient, self).connect(*args, **kwargs)

  def disconnect(self):
    self.keepalive_greenlet.kill()
    del self.keepalive_greenlet
    super(BsonNetworkPersistentClient, self).disconnect()

  keepalive_timeout = nanotime.seconds(1)
  def _keepalive_loop(self):
    while self.persist:
      gevent.sleep(self.keepalive_timeout / 5.0)

      # only send keepalive if we havent gotten data for keepalive_timeout
      timediff = nanotime.nanotime.now() - self.connection.lastSendTime
      if timediff > self.keepalive_timeout:
        self.connection.sendMessage({'_ctl':'keepalive', 'echoaddress':True})
        logging.info('[BsonNetworkPersistentClient] sent keepalive')






def test():
  '''Testing BsonNetwork Protocol'''

  import sys
  import util
  import bson
  import logging

  from util.test import testFactory
  from echo import BsonNetworkEchoProtocol, flipMessage

  parser = util.arg_parser('usage: %prog [options]', logging=logging.WARNING)
  options, args = parser.parse_args()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  factory = BsonNetworkFactory('echoer', options)
  factory.logging = logging
  factory.protocol = BsonNetworkEchoProtocol


  send = [{'_src':'client', '_dst':'echoer', 'h':'d'} for i in range(0, 64)]
  recv = map(flipMessage, send)

  send.insert(0, {'_src':'client'})
  recv.insert(0, {'_src':'echoer'})

  data = []
  for i in range(0, len(send)):
    data.append((bson.dumps(send[i]), bson.dumps(recv[i])))

  testFactory(factory, data)


if __name__ == '__main__':
  test()
