

import bson
import socket
import struct

class BsonSocket(socket.socket):

  def sendBson(self, doc):
    bsonData = bson.dumps(doc)
    self.send(bsonData)

  def recvBson(self):
    data = self.recv(4)
    if not data:
      return None

    length = struct.unpack("<i", data)[0]

    while len(data) < length:
      bytes_left = length - len(data)
      partialData = self.recv(min(bytes_left, 2048))
      if not partialData:
        return None
      data = data + partialData

    if len(data) > length:
      print 'OH NOES TOOK TOOO MUCH!?'

    return bson.loads(data)

import random

def random_object():
  fns = [random_dict, \
         random_list, \
         random_int, random_int, random_int, \
         random_int, random_int, random_int, \
         random.random, random.random, random.random, \
         random_string, random_string]
  return random.choice(fns)()

def random_dict():
  def random_key():
    key = ''
    for i in range(0, 10):
      key += chr(random.randint(ord('A'), ord('Z')))
    return key

  elems = random.randint(1, 7)
  dictionary = {}
  for r in range(0, elems):
    dictionary[random_key()] = random_object()
  return dictionary

def random_list():
  elems = random.randint(1, 7)
  seq = []
  for r in range(0, elems):
    seq.append(random_object())
  return seq

def random_string():
  length = random.randint(1, 256)

  string = ''
  for i in range(0, length):
    string += chr(random.randint(1, 126))
  return string

def random_int():
  return random.randint(0, 10000)

def dicts_equal(doc, doc2):
  return not any(True for k in doc if str(k) not in doc2) \
    and not any(True for k in doc2 if str(k) not in doc) \
    and not any(True for v in doc.values() if v not in doc2.values()) \
    and not any(True for v in doc2.values() if v not in doc.values())
