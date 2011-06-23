
import logging

from bsonprotocol import BsonProtocol

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
    self.transport.loseConnection()

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
