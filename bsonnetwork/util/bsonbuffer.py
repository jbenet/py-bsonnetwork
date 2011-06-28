

import bson
import struct



class BsonException(Exception):
  pass

class BsonLengthExceeded(BsonException):
  pass

class BsonDecodeError(BsonException):
  pass



class BsonReceiveBuffer(object):
  '''Buffer to receive BSON documents.'''

  __slots__ = ('buffer')

  len_fmt = '<i'
  len_size = struct.calcsize(len_fmt)
  max_bytes = 2 ** (8 * len_size)

  def __init__(self):
    self.buffer = ''

  def nextLength(self):
    '''Returns the length of the next document.'''
    if len(self.buffer) < self.len_size:
      return 0

    length ,= struct.unpack(self.len_fmt, self.buffer[:self.len_size])
    if length > self.max_bytes:
      raise BsonLengthExceeded('max bson length %d exceeded' % self.max_bytes)
    return length

  def missingLength(self):
    '''Returns the number of bytes missing from the next document.'''
    missing = self.nextLength() - len(self.buffer)
    return missing if missing > 0 else 0

  def hasNext(self):
    '''Return wether this receiver buffer has a next element.'''
    length = self.nextLength()
    return length > self.len_size and length <= len(self.buffer)

  def next(self):
    '''Returns the next element or None if one is not available yet.'''
    length = self.nextLength()
    if length is 0 or len(self.buffer) < length:
      return None

    bsonData = self.buffer[:length]
    self.buffer = self.buffer[length:]

    try:
      bsonDoc = bson.loads(bsonData)
    except Exception, e:
      raise
      raise BsonDecodeError
    return bsonDoc

  def append(self, string):
    self.buffer += string




def _sendobj(self, obj):
  '''Send a bson object from socket `self`.'''
  self.sendall(bson.dumps(obj))

def _recvobj(self):
  '''Receive the next bson object from the socket `self`'''
  buf = BsonReceiveBuffer()
  buf.append(self.recv(buf.len_size))
  while not buf.hasNext():
    bytes = min(4096, buf.missingLength())
    buf.append(self.recv(bytes))
  return buf.next()

def patch_socket(socket):
  '''Setup the methods here to be available on `socket` class'''
  socket.recvobj = _recvobj
  socket.sendobj = _sendobj


