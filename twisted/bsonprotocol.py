'''
Basic bson twisted protocol.
'''

import bson
import struct

from twisted.internet.protocol import Protocol
from twisted.protocols.basic import StringTooLongError
from twisted.protocols.basic import _PauseableMixin

class BsonProtocol(Protocol, _PauseableMixin):
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

    @param length: The length prefix which was received.
    @type length: C{int}
    '''
    self.transport.loseConnection()

  def receivedBson(self, msg):
    ''' For Subclasses to implement. '''
    raise NotImplementedError

  def sendBsonData(self, bsonData):
    '''
    Send bson data to the other end of the connection.
    This is useful if you are sending an unparsed bson doc
    '''
    if len(bsonData) >= self.max_bytes:
      errorStr = 'Trying to send %d bytes whereas maximum is %d'
      raise StringTooLongError(errorStr % (len(bsonData), self.max_bytes))
    self.transport.write(bsonData)

  def sendBson(self, bsonDoc):
    '''
    Send a bson document to the other end of the connection.

    @type data: C{str}
    '''
    self.sendBsonData(bson.dumps(bsonDoc)) # let exceptions propagate up


  def dataReceived(self, received):
    '''Receive int prefixed data until full bson doc.'''

    self.recv_buffer = self.recv_buffer + received

    while len(self.recv_buffer) >= self.length_size and not self.paused:
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
