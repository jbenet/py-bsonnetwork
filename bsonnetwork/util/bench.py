#!/usr/bin/env python

import sys
import gevent
import time
import logging
import bson

from gevent import socket

try:
  bson.patch_socket(socket.socket)
except:
  import bsonbuffer
  bsonbuffer.patch_socket(socket.socket)

def router_sock(clientid, addr):
  logging.debug('opening router socket at %s:%d' % addr)
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect(addr)
  sock.sendobj({'_src':clientid})
  sock.recvobj()
  return sock


class Connection(object):
  def __init__(self, clientid, sockaddr):
    self.clientid = clientid
    self.sockaddr = sockaddr
    self.stats = {'sent': 0, 'recv': 0}

    self._socket = router_sock(clientid, sockaddr)
    logging.debug(self.clientid + ' connected')

  def send(self, msg):
    msg['_src'] = self.clientid
    self._socket.sendobj(msg)
    self.stats['sent'] += 1
    logging.debug(self.clientid + ' send ' + str(msg))

  def recv(self):
    msg = self._socket.recvobj()
    self.stats['recv'] += 1
    logging.debug(self.clientid + ' recv ' + str(msg))
    return msg



class ConnectionPair(object):
  def __init__(self, pairid, sockaddr):
    self.pairid = pairid
    self.c2 = Connection(pairid + '2', sockaddr)
    self.c1 = Connection(pairid + '1', sockaddr)
    self.stats = {'flight_time' : 0.0, 'sent' : 0}

  def _sendMessage(self, msg, frm, to):
    tic = time.time()
    msg['_dst'] = to.clientid
    frm.send(msg)
    to.recv()
    toc = time.time()

    self.stats['flight_time'] += (toc - tic)
    self.stats['sent'] += 1

  def sendMessage(self, msg):
    self._sendMessage(msg, self.c1, self.c2)
    self._sendMessage(msg, self.c2, self.c1)

  def avgRTT(self):
    return self.stats['flight_time'] / self.stats['sent']





def sendSimpleMessagesToPair(pairid, sockaddr, messages):
  pair = ConnectionPair(pairid, sockaddr)
  msg = {'herp':'derp'}
  for i in range(0, messages):
    pair.sendMessage(msg)
  return pair.stats['flight_time'], pair.stats['sent']


def sockaddrFromHost(host):
  sockaddr = host.split(':')
  sockaddr[1] = int(sockaddr[1])
  return tuple(sockaddr)



def runBenchmark(host, options):
  from gevent import queue

  sockaddr = sockaddrFromHost(host)

  print 'setting up worker queue...'
  in_queue = queue.JoinableQueue()
  results = []

  def print_progress():
    print '\rpairs left to finish:', options.pairs - len(results),
    sys.stdout.flush()

  def work():
    pairid = in_queue.get()
    result = sendSimpleMessagesToPair(pairid, sockaddr, options.messages)
    results.append(result)
    print_progress()

  def worker():
    logging.debug('greenlet initialized')
    while True:
      try:
        work()
      finally:
        in_queue.task_done()

  for i in range(options.concurrency):
     gevent.spawn(worker)
     #FIXME(jbenet): are these spawned greenlets actually deallocated?

  print 'setting up jobs...'
  for i in range(options.pairs):
    in_queue.put('$test-pair-%d-' % i)

  print 'running...'
  print ''
  print_progress()

  tic = time.time()
  in_queue.join()  # block until all tasks are done
  toc = time.time()
  print '\rdone.                             '

  tup = reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]), results)
  avgrtt = tup[0] / tup[1]

  print ''
  print 'Messages Sent:', tup[1]
  print 'Average RTT:', avgrtt
  print 'Running Time:', (toc - tic)





def parseOptions():
  import optparse

  usage = 'usage: %prog [options] hostname:port'
  parser = optparse.OptionParser(usage=usage)

  loglevels = {'DEBUG': logging.DEBUG,
               'INFO': logging.INFO,
               'WARNING': logging.WARNING,
               'ERROR': logging.ERROR,
               'CRITICAL': logging.CRITICAL}

  parser.add_option('-c', '--concurrency', dest='concurrency',metavar='INTEGER',
    default=1, help='number of concurrent jobs to run')
  parser.add_option('-p', '--pairs', dest='pairs', metavar='INTEGER',
    default=1, help='number of connection pairs to make')
  parser.add_option('-m', '--messages', dest='messages', metavar='INTEGER',
    default=1, help='number of messages to send per connection')
  parser.add_option('-l', '--logging', dest='logging', metavar='loglevel',
    default='INFO', help='one of %s' % str(loglevels.keys()))

  options, args = parser.parse_args()

  clipi = lambda n, minimum, maximum: max(min(int(n), maximum), minimum)
  options.concurrency = clipi(options.concurrency, 1, 1000)
  options.messages = clipi(options.messages, 0, 1000)
  options.pairs = clipi(options.pairs, 0, 10000)

  options.logging = options.logging.upper()
  if options.logging not in loglevels:
    print 'Error: logging level specified not supported (%s)' % options.logging
    exit(-1)

  options.logging = loglevels[options.logging]

  if len(args) == 0:
    print 'Error: no host specified'
    exit(-1)

  if args[0].count(':') is not 1:
    print 'Error: host argument must indicate one port, e.g: hostname:12345'
    exit(-1)

  return options, args



def main():
  options, args = parseOptions()

  fmt='[%(asctime)s][%(levelname)8s] %(message)s'
  logging.basicConfig(level=options.logging, format=fmt)

  print '----- BsonRouter Bench -----'
  print 'Host:', args[0]
  print 'Concurrency:', options.concurrency
  print 'Connection Pairs:', options.pairs
  print 'Messages Per Pair:', options.messages
  print 'Total Messages:', options.messages * options.pairs * 2

  runBenchmark(args[0], options)

if __name__ == '__main__':
  main()