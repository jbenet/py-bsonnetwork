#!/usr/bin/python
'''
bson gevent bson network
'''

import logging
from bsonprotocol import BsonProtocol
from bsonprotocol import BsonFactory

class BsonNetworkProtocol(BsonProtocol):

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
    self.sendBson({'_src' : self.factory.clientid})

  def connectionLost(self, reason):
    self.close()

  def close(self):
    self.log('info', 'connection closed')
    self.sock.close()

  def receivedMessage(self, msg):
    '''Override this to handle a packet addressed to this service.'''
    raise NotImplementedError

  def receivedControlMessage(self, msg):
    '''Identify sender by default.'''
    self.clientid = msg['_src']
    self.log('debug', 'connection identified')

  def receivedForwardMessage(self, msg):
    '''Drop bson docs not for us by default.'''
    pass

  def receivedBson(self, doc):
    self.log('debug', 'bson document received')
    if not self.validMessage(doc):
      self.log('debug', 'data discarded (invalid document)')
      return

    self.log('debug', 'document parsed %s' % str(doc))
    if '_dst' not in doc:
      self.log('debug', 'handling identification message')
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
    except Exception, e:
      self.log('error', 'sending bson document error: %s' % e)

  def sendMessage(self, doc, src=None, dst=None):
    if '_src' not in doc or src:
      doc['_src'] = src or self.factory.clientid
    if '_dst' not in doc or dst:
      doc['_dst'] = dst or self.clientid
    self.forwardMessage(doc)

  def bsonDecodingError(self, error):
    self.log('error', 'received bson parsing error: %s' % e)

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

  def __init__(self, clientid='None', options=None):
    self.clientid = clientid
    self.options = options






class BsonNetworkEchoProtocol(BsonNetworkProtocol):

  def receivedMessage(self, msg):
    msg['_dst'] = msg['_src']
    msg['_src'] = self.factory.clientid
    self.sendMessage(msg)
    self.log('info', 'echoed message %s' % msg)



if __name__ == '__main__':
  import sys
  import logging

  loglevel = logging.DEBUG if '-v' in sys.argv else logging.WARNING
  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=loglevel, format=fmt)

  factory = BsonNetworkFactory()
  factory.logging = logging
  factory.clientid = 'echoer'
  factory.protocol = BsonNetworkEchoProtocol

  import random
  port = random.randint(10000, 65000)

  from gevent import socket
  from gevent.server import StreamServer
  server = StreamServer(('', port), factory.serverHandler)

  print 'Starting bson network server on port', port
  server.start()

  def sendMessages(number):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', port))

    def send(message):
      message['_src'] = 'client_%d' % number
      s.sendobj(message)

    def receive(message):
      if s.recvobj() != message:
        s.close()
        return False
      return True

    send({})

    if not receive({'_src' : 'echoer'}):
      return False

    for i in range(0, 100):
      message = {'_dst' : 'echoer', 'line' : ('%d' % i) * 64 }
      send(message)
      gevent.sleep(0.001)

      message['_src'] = 'echoer'
      message['_dst'] = 'client_%d' % number
      if not receive(message):
        return False

    s.close()
    gevent.sleep(0.001)
    return True

  import gevent
  jobs = [gevent.spawn(sendMessages, num) for num in range(0, 100)]
  gevent.joinall(jobs, timeout=15)
  vals = [(1 if job.value else 0) for job in jobs]

  print sum(vals), '/', len(vals), 'succeeded'


