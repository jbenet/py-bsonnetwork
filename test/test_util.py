
import bson
import unittest
import socket

from bsonnetwork.util import test
from bsonnetwork.util import BsonReceiveBuffer

from bsonnetwork.protocol import BsonProtocol

import utils

class TestUtil(unittest.TestCase):

  def test_bsonbuffer(self):

    num_docs = 20
    docs = [utils.random_dict() for i in range(0, num_docs)]
    data = [bson.dumps(doc) for doc in docs]
    lens = [len(doc) for doc in data]

    buf = BsonReceiveBuffer()

    # byte by byte
    self.assertFalse(buf.hasNext())
    for i in range(0, num_docs):
      self.assertEqual(buf.nextLength(), 0)
      for b in range(0, lens[i]):
        self.assertFalse(buf.hasNext())
        buf.append(data[i][b])
      self.assertTrue(buf.hasNext())
      self.assertEqual(buf.nextLength(), lens[i])
      doc = buf.next()
      self.assertEqual(doc, docs[i])
      self.assertTrue(test.dicts_equal(doc, docs[i]))

    # whole docs.
    self.assertFalse(buf.hasNext())
    for i in range(0, num_docs):
      self.assertFalse(buf.hasNext())
      self.assertEqual(buf.nextLength(), 0)
      buf.append(data[i])
      self.assertEqual(buf.nextLength(), lens[i])
      self.assertTrue(buf.hasNext())
      doc = buf.next()
      self.assertEqual(doc, docs[i])
      self.assertTrue(test.dicts_equal(doc, docs[i]))

    # all docs
    self.assertFalse(buf.hasNext())
    self.assertEqual(buf.nextLength(), 0)
    for i in range(0, num_docs):
      buf.append(data[i])
      self.assertTrue(buf.hasNext())
      self.assertTrue(buf.nextLength(), lens[0])
    for i in range(0, num_docs):
      self.assertTrue(buf.hasNext())
      self.assertTrue(buf.nextLength(), lens[i])
      doc = buf.next()
      self.assertEqual(doc, docs[i])
      self.assertTrue(test.dicts_equal(doc, docs[i]))
    self.assertFalse(buf.hasNext())


  def test_bsonrecv(self):

    class ProtocolTest(BsonProtocol):

      def __init__(self, dictionaries):
        self.match = dictionaries
        send = [bson.dumps(d) for d in dictionaries]
        print [len(s) for s in send]

        chunkize = lambda d, s: [d[i:i+s] for i in range(0, len(d), s)]


        for rep in send:
          print len(rep)
          chunks = chunkize(rep, 1024)
          for chunk in chunks:
            self.receivedData(chunk)

      def receivedBson(self, bsonDoc):
        assert test.dicts_equal(bsonDoc, self.match[0])
        del self.match[0]

    ProtocolTest([utils.random_dict() for i in range(0, 20)])
    ProtocolTest([utils.random_dict() for i in range(0, 20)])
    ProtocolTest([utils.random_dict() for i in range(0, 20)])
    ProtocolTest([utils.random_dict() for i in range(0, 20)])

    mf = {}
    for i in range(0, 30):
      mf['%d' % i] = utils.random_dict()

    ProtocolTest([mf, mf, mf, mf, mf])
    ProtocolTest([mf, mf, mf, mf, mf])
    ProtocolTest([mf, mf, mf, mf, mf])

    mf = {}
    for i in range(0, 5000):
      mf['%d' % i] = utils.random_dict()

    ProtocolTest([mf, mf, mf, mf, mf])
    ProtocolTest([mf, mf, mf, mf, mf])
    ProtocolTest([mf, mf, mf, mf, mf])

if __name__ == '__main__':
  unittest.main()
