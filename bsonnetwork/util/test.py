
import os
import bson
import time
import socket
import signal
import gevent
import process
import tempfile

from subprocess import Popen, STDOUT

try:
  bson.patch_socket(socket.socket)
except:
  import bsonbuffer
  bsonbuffer.patch_socket(socket.socket)


class DocumentMismatchError(Exception):
  pass

class Alarm(Exception):
  @staticmethod
  def handler(signum, frame):
    raise Alarm

class OutputTimeout(Exception):
  pass


def dicts_equal(doc, doc2):
  '''Utility function that checks documents for equivalence.'''
  return not any(True for k in doc if str(k) not in doc2) \
    and not any(True for k in doc2 if str(k) not in doc) \
    and not any(True for v in doc.values() if v not in doc2.values()) \
    and not any(True for v in doc2.values() if v not in doc.values())





def testSendData(dataSet, port):
  '''Test sending message `dataSet` to a server listening on `port`'''

  if not isinstance(dataSet, list):
    dataSet = [dataSet]

  s = gevent.socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

  from base import Server

  port = process.randomPort()

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






class BsonNetworkProcess(object):
  '''Utility class in order to test BsoNetwork processes.'''

  def __init__(self, cmd, **kwargs):
    '''Initialize with the given command and arguments.'''
    self._parse_arguments(cmd, **kwargs)
    self.proc = None

  def _parse_arguments(self, cmd, port=None, clientid=None, **kwargs):
    '''Parse all the arguments and add them to the command.'''
    if isinstance(cmd, str):
      cmd = cmd.split(' ')

    if '-p' not in cmd and '--port' not in cmd:
      if port is None:
        port = process.randomPort()
      cmd.append('-p')
      cmd.append(str(port))
    self.port = int(cmd[cmd.index('-p') + 1])

    if '-i' not in cmd and '--client-id' not in cmd:
      if clientid is None:
        clientid = 'server'
      cmd.append('-i')
      cmd.append(clientid)
    self.clientid = cmd[cmd.index('-i') + 1]

    self.cmd = ' '.join(cmd)


  def start(self):
    '''Launch and wait for the process to initialize.'''
    print 'starting', self.cmd

    # here, using own pipes for subprocess.
    r, w = os.pipe()
    tf = os.fdopen(w, 'w', 1048576)
    self.tf = os.fdopen(r, 'r', 1048576)

    self.proc = Popen(self.cmd, shell=True, stderr=STDOUT, stdout=tf.fileno())
    self.socks = {}
    self._output_buffer = []
    self.waitForOutput('Starting BsonNetwork v')
    time.sleep(1.0)

  def stop(self):
    '''Stop the process, and print out all remaining output.'''
    for clientid in self.socks.keys():
      self.disconnect(clientid)
    self.socks = {}

    self.proc.terminate()
    if self.proc.poll() is None:
      self.proc.kill()

    for line in self.tf.readlines():
      line = line.strip()
      if len(line) > 1:
        print line

    self.proc = None

  def __enter__(self):
    '''This object can be used in a with clause. starts the process.'''
    self.start()
    return self

  def __exit__(self, type, value, traceback):
    '''This object can be used in a with clause. stops the process.'''
    self.stop()


  def waitForOutput(self, output):
    '''Waits for `output` to appear in the stdout or stderr of the process.'''
    print '=====> Waiting for: %s' % output

    # check backlog first.
    for line in self._output_buffer:
      if output in line:
        self._output_buffer.remove(line) # found it! remove it.
        return

    signal.signal(signal.SIGALRM, Alarm.handler)
    signal.alarm(5)

    try:
      line = ''
      while output not in line:
        if len(line) > 1:
          self._output_buffer.append(line)
        line = self.tf.readline().strip()
        if len(line) > 1:
          print line
    except Alarm:
      raise OutputTimeout('Timed out waiting for output: %s' % output)

    signal.alarm(0)

  def _sendobj(self, clientid, doc):
    '''Sends bson document `doc` via socket with `clientid`.'''
    self.socks[clientid].send(bson.dumps(doc))

  def _recvobj(self, clientid, doc):
    '''Receives bson document `doc` via socket `clientid`.'''
    print len(bson.dumps(doc))
    res = self.socks[clientid].recvobj()
    #res = self.socks[clientid].recv(len(bson.dumps(doc)))
    print res
    return res


  def send(self, docs):
    '''Sends a collection of bson docs (with _src and _dst)'''
    print 'SENDING', docs
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      self._sendobj(doc['_src'], doc)

  def receive(self, docs, printall=False):
    '''Receives a collection of bson docs (with _src and _dst)'''
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      self.waitForOutput('[%(_dst)s] sending document' % doc)
      while printall:
        print self.tf.readline().strip()

      doc2 = self._recvobj(doc['_dst'], doc)
      if not dicts_equal(doc, doc2):
        raise DocumentMismatchError('Document mismatch:\n%s\n%s' % (doc, doc2))

  def send_and_receive(self, src, dst, docs):
    '''Sends and Receives a collection of bson docs (with _src and _dst)'''
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      doc['_src'] = src
      doc['_dst'] = dst
      self.send(doc)
      self.receive(doc)

  def connect(self, clientid, trigger=True):
    '''Connects a socket with `clientid` to the process server.'''
    sock = None
    if trigger:
      print '=====> Connecting', clientid, 'to', self.port
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect(('', self.port))
      self.socks[clientid] = sock
    self.waitForOutput('connection made')
    return sock

  def disconnect(self, clientid, trigger=True):
    '''Disconnects the socket with `clientid` from the process server.'''
    if trigger:
      sock = self.socks[clientid]
      sock.close()
      del self.socks[clientid]
    self.waitForOutput('[%s] connection closed' % clientid)

  def identify(self, clientid, trigger=True):
    '''Identifies the socket with `clientid` with the process server.'''
    self.waitForOutput('sending identification message')
    if trigger:
      self._recvobj(clientid, { '_src' : self.clientid } )
      self._sendobj(clientid, { '_src' : clientid } )
    self.waitForOutput('[%s] connection identified' % clientid)


