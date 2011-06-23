#!/usr/bin/python
'''
bson gevent Server
'''

import logging
import bson
import struct

from base import Protocol, Factory



class BsonError(Exception):
  pass

class LengthExceededError(BsonError):
  pass




class BsonProtocol(Protocol):
  '''
  Generic class for bson protocols.
  Heavily based on twisted.protocols.basic.IntNReceiver
  It needed to be rewritten (instead of deriving) because of a fundamental
  difference: bson counts the length of the prefix in the doc length. hacking
  around that is unclean.

  WARNING: this class assumes correctly formed bson docs. There is a bit of
           checking, in case the doc doesnt quite convert. However, the first
           int32 read (bson doc length) is taken at face value. If it happens
           to be wrong, it will hose all the data that it covers, as the bson
           doc won't parse properly.

  @author Juan Batiz-Benet

  @ivar recvd: buffer holding received data.
  @type recvd: C{str}
  '''

  recv_buffer = ''

  max_bytes = 16777216 # 16 MB (current bson limit is 4, +proposed inc to 16)

  length_fmt = '<i' # this may change in the future.
  length_size = struct.calcsize(length_fmt)

  def bsonDecodingError(self, error):
    ''' For potential error checking. '''
    pass

  def lengthLimitExceeded(self, length):
    '''
    Callback invoked when a length prefix greater than max_bytes is
    received.  The default implementation disconnects the transport.
    '''
    self.transport.loseConnection()

  def receivedBson(self, message):
    '''Override this to handle a message addressed to this service.'''
    raise NotImplementedError

  def sendBsonData(self, bsonData):
    '''
    Send bson document to the other end of the connection.
    This is useful if you are sending an unparsed bson doc
    '''
    if len(bsonData) >= self.max_bytes:
      errorStr = 'Trying to send %d bytes whereas maximum is %d'
      raise LengthExceededError(errorStr % (len(bsonData), self.max_bytes))
    self.transport.write(bsonData)

  def sendBson(self, message):
    '''Send a bson document to the other end of the connection.'''
    self.sendBsonData(bson.dumps(message)) # let exceptions propagate up

  def receivedData(self, received):
    '''Receive int prefixed data until full bson doc.'''

    self.recv_buffer = self.recv_buffer + received

    while len(self.recv_buffer) >= self.length_size:
      length ,= struct.unpack(
        self.length_fmt, self.recv_buffer[:self.length_size])

      if length > self.max_bytes:
        self.lengthLimitExceeded(length)
        return # bad news bears.

      if len(self.recv_buffer) < length:
        break # not enough for full bson doc yet.

      bsonData = self.recv_buffer[:length]
      self.recv_buffer = self.recv_buffer[length:]

      try:
        bsonDoc = bson.loads(bsonData)
      except Exception, e:
        self.bsonDecodingError(e)
        # Note: at this point, we may be off sync (warranting disconnect)
        #       but let's attempt to keep going!
      else:
        self.receivedBson(bsonDoc)




class BsonFactory(Factory):
  '''Twisted-like factory facility.'''

  protocol = BsonProtocol




if __name__ == '__main__':
  '''Testing'''

  import util
  import logging
  from echo import BsonEchoProtocol

  parser = util.arg_parser('usage: %prog [options]', logging=logging.WARNING)
  options, args = parser.parse_args()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  factory = BsonFactory()
  factory.protocol = BsonEchoProtocol

  data = {'TestMessage' : 'dfedwfewfrewR#@!$#@!$@T%$EBRDFG', 'Herp' : 'Derp'}
  data = bson.dumps(data)
  data = [data for i in range(0, 100)]

  util.test.testFactory(factory, data)


