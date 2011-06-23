

import util
import random
import optparse
from optparse import OptionValueError

import logging

from base import Factory, Server

import gevent
from gevent import socket


def testSendData(dataSet, port):
  '''Test sending message `dataSet` to a server listening on `port`'''

  if not isinstance(dataSet, list):
    dataSet = [dataSet]

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect(('localhost', port))

  for data in dataSet:
    if not isinstance(data, tuple):
      data = (data, data)

    s.send(data[0])
    gevent.sleep(0.001)
    if s.recv(len(data[1])) != data[1]:
      s.close()
      return False

  s.close()
  gevent.sleep(0.001)
  return True




def testFactory(factory, data, clients=100):
  '''Tests the given factory with the given data packets.'''

  port = randomPort()

  print 'Starting', factory, 'with', factory.protocol, 'on port', port
  server = Server(('', port), factory)
  server.serve()

  jobs = []
  for c in range(0, clients):
    jobs.append(gevent.spawn(testSendData, data, port))

  gevent.joinall(jobs, timeout=15)
  vals = [(1 if job.value else 0) for job in jobs]
  print sum(vals), '/', len(vals), 'succeeded'

  return sum(vals) * 1.0 / len(vals)





def randomPort(low=10000, high=65000):
  '''Returns a random port'''
  return random.randint(low, high)




# Useful Argument Parsing

def store_int_range(min, max):
  '''Returns an optparse store callback that validates an int within a range'''
  def store_int(option, ops, value, parser):
    if value is None:
      return

    value = int(value)
    if value > max:
      raise OptionValueError("value for %s too large (max: %d)" % (ops, max))
    if value < min:
      raise OptionValueError("value for %s too small (min: %d)" % (ops, min))
    setattr(parser.values, option.dest, value)

  return store_int



def store_loglevel(option, ops, value, parser):
  '''optparse store callback that validates loglevel'''
  loglevels = {'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

  try:
    if value.lower() in loglevels:
      setattr(parser.values, option.dest, loglevels[value.lower()])
    elif int(value) in loglevels.values():
      setattr(parser.values, option.dest, int(value))
    else:
      raise OptionValueError("unrecognized value for %s: %s" % (ops, value))
  except:
    raise OptionValueError("invalid value for %s: %s" % (ops, value))



def arg_parser(usage, **defaults):
  '''Returns an argument parser with common bsonnetwork arguments.'''

  if 'port' not in defaults:
    defaults['port'] = randomPort()
  if 'clients' not in defaults:
    defaults['clients'] = 10000
  if 'logging' not in defaults:
    defaults['logging'] = logging.NOTSET
  if 'clientid' not in defaults:
    defaults['clientid'] = '$process'


  parser = optparse.OptionParser(usage)

  parser.add_option('-p', '--port',  dest='port',  metavar='PORT', type='int',
    action='callback', callback=store_int_range(1024, 65535),
    default=defaults['port'],
    help='the port to listen on')

  # parser.add_option('-s', '--secret', dest='secret', metavar='string',
  #   default=defaults['secret'], help='shared secret to authenticate clients')

  parser.add_option('-c', '--clients', dest='clients', metavar='NUMBER',
    type='int', action="callback", callback=store_int_range(1, 100000),
    default=defaults['clients'],
    help='maximum number of simultaneously connected clients (1, 100000)')

  parser.add_option('-l', '--logging', dest='logging', metavar='log level',
    action="callback", callback=store_loglevel, type='string',
    default=defaults['logging'],
    help='logging level. one of (debug, info, warning, error, critical)')

  parser.add_option('-i', '--client-id', dest='clientid', metavar='STRING',
    default=defaults['clientid'], help='bsonnetwork client id')

  return parser

