import gevent
from gevent import socket

import subprocess
import errno
import sys
import fcntl, os

from subprocess import Popen as _Popen, PIPE, STDOUT


class GeventFile(object):

    def __init__(self, fobj):
        self._obj = fobj
        fcntl.fcntl(self._obj, fcntl.F_SETFL, os.O_NONBLOCK)

    def __getattr__(self, item):
        assert item != '_obj'
        return getattr(self._obj, item)

    def write(self, data):
        # use buffer
        bytes_total = len(data)
        bytes_written = 0
        fileno = self.fileno()
        while bytes_written < bytes_total:
            try:
                # fileobj.write() doesn't return anything, so use os.write.
                bytes_written += os.write(fileno, data[bytes_written:])
            except IOError, ex:
                if ex[0] != errno.EAGAIN:
                    raise
                sys.exc_clear()
            socket.wait_write(fileno)

    def read(self, size=-1, chunksize=1024):
        chunks = []
        bytes_read = 0
        fileno = self.fileno()
        while size < 0 or bytes_read < size:
            try:
                if size < 0:
                    chunk = self._obj.read()
                else:
                    chunk = self._obj.read(min(chunksize, size - bytes_read))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_read += len(chunk)
            except IOError, ex:
                if ex[0] != errno.EAGAIN:
                    raise
                sys.exc_clear()
            socket.wait_read(fileno)
        return ''.join(chunks)


    def readline(self, size=-1, chunksize=1024):
      chunks = []
      bytes_read = 0
      fileno = self.fileno()
      while size < 0 or bytes_read < size:
          try:
              if size < 0:
                  chunk = self._obj.readline()
              else:
                  chunk = self._obj.readline(min(chunksize, size - bytes_read))
              if not chunk:
                  break
              chunks.append(chunk)
              if '\n' in chunk:
                  break
              bytes_read += len(chunk)
          except IOError, ex:
              if ex[0] != errno.EAGAIN:
                  raise
              sys.exc_clear()
          socket.wait_read(fileno)
      return ''.join(chunks)

class Popen(_Popen):

    def __init__(self, *args, **kwargs):
        _Popen.__init__(self, *args, **kwargs)
        if self.stdin is not None:
            self.stdin = GeventFile(self.stdin)
        if self.stdout is not None:
            self.stdout = GeventFile(self.stdout)

    def wait(self):
        while self.returncode is None:
            if self.poll() is None:
                gevent.sleep(0.1)
        # XXX come up with something better
        return self.returncode


def popen_communicate(args, data=''):
    """Communicate with the process non-blockingly."""
    p = Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    p.stdin.write(data)
    p.stdin.close()
    result = p.stdout.read()
    p.stdout.close()
    return result


if __name__ == '__main__':
    # run 2 jobs in parallel
#     job1 = gevent.spawn(popen_communicate, 'finger')
#     job2 = gevent.spawn(popen_communicate, 'netstat')
#
#     # wait for them to complete. stop waiting after 2 seconds
#     gevent.joinall([job1, job2], timeout=2)
#
#     # print the results (if available)
#     if job1.ready():
#         print 'finger: %s bytes: %s' % (len(job1.value), repr(job1.value)[:50])
#     else:
#         print 'finger: job is still running'
#     if job2.ready():
#         print 'netstat: %s bytes: %s' % (len(job2.value), repr(job2.value)[:50])
#     else:
#         print 'netstat: job is still running'

    def printer():
        sys.stderr.write('.')
        gevent.core.timer(0.5, printer)

    printer()

    p = Popen(['python', '-c', 'import time; time.sleep(3)'], stdout=subprocess.PIPE)

    def stdout_reader():
        print p.stdout.read()

    job1 = gevent.spawn(stdout_reader)

    print p.wait()

    try:
        job1.join()
    except KeyboardInterrupt:
        p.kill()

