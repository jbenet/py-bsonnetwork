#!/usr/bin/env python

import os
import re
import sys
import datetime
import optparse

LINE_FORMAT = r'^\[([^\]]+)\]\[([^\]]+)\] (.+)$'
LINE_RE = re.compile(LINE_FORMAT)


class UserCounters(object):
  OUTPUT_FMT = '%(name)20s \t %(times_connected)3d \t\t%(num_connections)d\t' \
    '%(docs_sent)3d/%(docs_received)3d\t\t' \
    '%(docs_sent_dropped)3d/%(docs_received_dropped)3d'

  def __init__(self, name):
    self.name = name
    self.docs_sent = 0
    self.docs_sent_dropped = 0
    self.docs_received = 0
    self.docs_received_dropped = 0
    self.times_connected = 0
    self.connections = {}
    self.logLevels = {}

  def logConnection(self, from_user, to_user, dropped=False):
    other = None
    if from_user == self.name:
      if dropped:
        self.docs_sent_dropped += 1
      else:
        self.docs_sent += 1
      other = to_user
    elif to_user == self.name:
      if dropped:
        self.docs_received_dropped += 1
      else:
        self.docs_received += 1
      other = from_user

    if other not in self.connections:
      self.connections[other] = 0
    self.connections[other] += 1


  def log(self, log):
    if log.level not in self.logLevels:
      self.logLevels[log.level] = 0
    self.logLevels[log.level] += 1

    if 'client connected:' in log.msg:
      self.times_connected += 1

    elif 'forwarding document from' in log.msg:
      from_user, to, to_user = log.msg.split(' ')[-3:]
      self.logConnection(from_user, to_user)

    elif 'dropped document' in log.msg:
      from_user, to, to_user = log.msg.split(' ')[-3:]
      self.logConnection(from_user, to_user, dropped=True)

  def __str__(self):
    info = self.__dict__
    info['num_connections'] = len(self.connections)
    return UserCounters.OUTPUT_FMT % info

class Counters(object):
  OUTPUT_FMT = """
    ========================= %(identifier)s =========================
    Unique Users Connected: %(unique_user_connected)s
    Users Connected: %(user_connected)s
    Docs Forwarded: %(docs_forwarded)s
    Docs Dropped: %(docs_dropped)s
    Logs: %(logLevels)s

           USERNAME \tCONNECT      FRIENDS \tSENT/RECV \tDROPPED
       ---------------------------------------------------------
"""

  def __init__(self, identifier):
    self.identifier = identifier
    self.unique_user_connected = 0
    self.user_connected = 0
    self.docs_forwarded = 0
    self.docs_dropped = 0
    self.users = {}
    self.logLevels = {}

  def __ensureUserExists(self, user):
    if user not in self.users:
      self.unique_user_connected += 1
      self.users[user] = UserCounters(user)

  def log(self, log):
    if log.level not in self.logLevels:
      self.logLevels[log.level] = 0
    self.logLevels[log.level] += 1

    if 'client connected:' in log.msg:
      self.user_connected += 1

      user = log.msg.strip().split(' ')[-1]
      self.__ensureUserExists(user)
      self.users[user].log(log)

    elif 'forwarding document from' in log.msg:
      self.docs_forwarded += 1

      from_user, to, to_user = log.msg.split(' ')[-3:]
      self.__ensureUserExists(from_user)
      self.__ensureUserExists(to_user)
      self.users[from_user].log(log)
      self.users[to_user].log(log)

    elif 'dropped document' in log.msg:
      self.docs_dropped += 1

      from_user, to, to_user = log.msg.split(' ')[-3:]
      self.__ensureUserExists(from_user)
      self.__ensureUserExists(to_user)
      self.users[from_user].log(log)
      self.users[to_user].log(log)


  def __str__(self):
    out = Counters.OUTPUT_FMT % self.__dict__
    for user in sorted(self.users):
      out += str(self.users[user]) + '\n'
    return out

class Stats(object):
  def __init__(self):
    self.counters = {}
    self.counters['global'] = Counters('_GLOBAL')

  def log(self, log):
    day = log.day()
    if day not in self.counters:
      self.counters[day] = Counters(day)

    self.counters[day].log(log)
    self.counters['global'].log(log)


class LogMessage(object):
  def __init__(self, line):
    date, level, msg = LINE_RE.match(line).groups()
    self.date = parseDate(date)
    self.level = level
    self.msg = msg.strip()

  def isBefore(self, date):
    return self.date < date

  def isAfter(self, date):
    return self.date >= date

  def day(self):
    return self.date.strftime('%Y-%m-%d')


def printStats(stats):
  for counter in sorted(stats.counters):
    print str(stats.counters[counter])


def parseLog(infile, opts):
  stats = Stats()

  for line in infile:
    log = LogMessage(line.strip())
    if opts.until and not log.isBefore(opts.until):
      break
    if opts.since and log.isBefore(opts.since):
      continue

    stats.log(log)

  return stats


DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
def parseDate(datestr, fmt=DATE_FORMAT):
  if ',' not in datestr:
    return datetime.datetime.strptime(datestr, fmt)

  nofrag, frag = datestr.split(",")
  date = datetime.datetime.strptime(nofrag, fmt)

  frag = frag[:6]  # truncate to microseconds
  frag += (6 - len(frag)) * '0'  # add 0s
  return date.replace(microsecond=int(frag))


def parseArgs():

  usage = '''BsonRouter Log Parser
  usage: %prog [options]
  '''

  parser = optparse.OptionParser(usage)
  parser.add_option('-v', '--verbose', dest='verbose', metavar='verbose',
    default=False, help='whether to output test urls')
  parser.add_option('-f', '--file', dest='file', metavar='logfile',
    default=None, help='read from a log file')
  parser.add_option('-s', '--since', dest='since', metavar='DATE',
    default=None, help='parse logs after given date')
  parser.add_option('-u', '--until', dest='until', metavar='DATE',
    default=None, help='parse logs until given date')

  options, args = parser.parse_args()

  if options.file:
    if not os.path.exists(options.file):
      print 'Error: filename', options.file, 'does not exist.'
      exit(-1)
    elif not os.path.isfile(options.file):
      print 'Error: filename', options.file, 'is not a file.'
      exit(-1)

  if options.since:
    options.since = parseDate(options.since, '%Y-%m-%d')
  if options.until:
    options.until = parseDate(options.until, '%Y-%m-%d')

  if options.until and options.since and options.until < options.since:
    print 'Error: since date is later than until date.'
    exit(-1)

  return options, args


def main():
  opts, args = parseArgs()

  readFrom = open(opts.file) if opts.file else sys.stdin
  stats = parseLog(readFrom, opts)
  if opts.file:
    readFrom.close()

  printStats(stats)

if __name__ == '__main__':
  main()