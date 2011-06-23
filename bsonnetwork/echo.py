#!/usr/bin/env python

import copy
import logging

from base import Protocol
from protocol import BsonProtocol
from network import BsonNetworkFactory, BsonNetworkProtocol


__version__ = '1.1'


def flipMessage(message):
  newMessage = copy.copy(message)
  newMessage['_src'] = message['_dst']
  newMessage['_dst'] = message['_src']
  return newMessage



class EchoProtocol(Protocol):
  '''Simple echo protocol. Just sends back any received data.'''

  def connectionMade(self):
    logging.info('[EchoProtocol] %s:%d connection made' % self.address)

  def connectionLost(self, reason):
    logging.info('[EchoProtocol] %s:%d connection lost' % self.address)

  def receivedData(self, data):
    logging.info('[EchoProtocol] %s:%d received data' % self.address)
    logging.debug('[EchoProtocol] %s' % data)
    self.sendData(data)





class BsonEchoProtocol(BsonProtocol):
  '''Simple echo bsonprotocol. Just sends back any received documents.'''

  def connectionMade(self):
    logging.info('[BsonEchoProtocol] %s:%d connection made' % self.address)

  def connectionLost(self, reason):
    logging.info('[BsonEchoProtocol] %s:%d connection lost' % self.address)

  def receivedBson(self, message):
    logging.info('[BsonEchoProtocol] %s:%d received message' % self.address)
    logging.debug('[BsonEchoProtocol] %s' % message)
    self.sendBson(message)




class BsonNetworkEchoProtocol(BsonNetworkProtocol):

  def receivedMessage(self, msg):
    msg['_dst'] = msg['_src']
    msg['_src'] = self.factory.clientid
    self.sendMessage(msg)
    self.log('info', 'echoed message %s' % msg)




def parseArgs():

  usage = '''usage: %prog [options]

  This the bsonnetwork echo server. It accepts and maintains connections
  and echoes any received BSON objects.
  '''

  from util import arg_parser
  parser = arg_parser(usage)
  return parser.parse_args()



def main():

  options, args = parseArgs()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  logging.info('options: %s' % options)
  logging.info('Starting BsonNetwork Echoer v%s on port %d' \
    % (__version__, options.port))

  factory = BsonNetworkFactory(options.clientid, options)
  factory.logging = logging
  factory.protocol = BsonNetworkEchoProtocol

  from base import Server
  server = Server(('', options.port), factory)
  server.serve_forever()


if __name__ == '__main__':
  main()

