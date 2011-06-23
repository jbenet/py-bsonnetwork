import bson
import util
import time
import socket
import signal

from subprocess import Popen, PIPE, STDOUT


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






class BsonNetworkProcess(object):
  '''Utility class in order to test BsoNetwork processes.'''

  def __init__(self, cmd, **kwargs):
    '''Initialize with the given command and arguments.'''
    self._parse_arguments(cmd, **kwargs)

  def _parse_arguments(self, cmd, port=None, clientid=None, **kwargs):
    '''Parse all the arguments and add them to the command.'''
    if isinstance(cmd, str):
      cmd = cmd.split(' ')

    if '-p' not in cmd:
      if port is None:
        port = util.randomPort()
      cmd.append('-p')
      cmd.append(str(port))
    self.port = int(cmd[cmd.index('-p') + 1])

    if '-i' not in cmd:
      if clientid is None:
        clientid = 'server'
      cmd.append('-i')
      cmd.append(clientid)
    self.proc_clientid = clientid

    self.cmd = ' '.join(cmd)


  def start(self):
    '''Launch and wait for the process to initialize.'''
    print 'starting', self.cmd
    self.proc = Popen(self.cmd, shell=True, stderr=STDOUT, stdout=PIPE)
    self.socks = {}
    self._output_buffer = []
    self.waitForOutput('Starting BsonNetwork v')
    time.sleep(1.0)

  def stop(self):
    '''Stop the process, and print out all remaining output.'''
    for clientid in self.socks.keys():
      self.disconnect(clientid)
    del self.socks

    self.proc.kill()
    print self.proc.stdout.read()

    del self.proc


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
    signal.alarm(3.0)

    try:
      line = ''
      while output not in line:
        if len(line) > 1:
          self._output_buffer.append(line)
        line = self.proc.stdout.readline()
        print line.strip()
    except Alarm:
      raise OutputTimeout('Timed out waiting for output: %s' % output)


  def _sendobj(self, clientid, doc):
    '''Sends bson document `doc` via socket with `clientid`.'''
    self.socks[clientid].send(bson.dumps(doc))

  def _recvobj(self, clientid, doc):
    '''Receives bson document `doc` via socket `clientid`.'''
    return self.socks[clientid].recv(len(bson.dumps(doc)))


  def send(self, docs):
    '''Sends a collection of bson docs (with _src and _dst)'''
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
        print self.proc.stdout.readline().strip()

      doc2 = self.socks[doc['_dst']].recv(len(doc))
      if not dicts_equal(doc, doc2):
        raise DocumentMismatchError('Document mismatch:\n%s\n%s' % doc, doc2)

  def send_and_receive(self, src, dst, docs):
    '''Sends and Receives a collection of bson docs (with _src and _dst)'''
    if not isinstance(docs, list):
      docs = [docs]

    for doc in docs:
      doc['_src'] = src
      doc['_dst'] = dst
      self.send(doc)
      self.receive(doc)


  def connect(self, clientid):
    '''Connects a socket with `clientid` to the process server.'''
    print '=====> Connecting', clientid, 'to', self.port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('', self.port))
    self.socks[clientid] = sock
    self.waitForOutput('connection made')
    return sock

  def disconnect(self, clientid):
    '''Disconnects the socket with `clientid` from the process server.'''
    sock = self.socks[clientid]
    sock.close()
    del self.socks[clientid]
    self.waitForOutput('[%s] connection closed' % clientid)

  def identify(self, clientid):
    '''Identifies the socket with `clientid` with the process server.'''
    self.waitForOutput('sending identification message')
    self._recvobj(clientid, { '_src' : self.proc_clientid } )
    self._sendobj(clientid, { '_src' : clientid } )
    self.waitForOutput('[%s] connection identified' % clientid)



