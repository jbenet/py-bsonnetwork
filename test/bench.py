import socket
import bson
from time import time

def sock(clientid, addr=('celebdil', 9194)):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect(addr)
  sock.sendobj({"_src":clientid})
  print sock.recvobj()
  return sock


def bench(times=21):
  start = time()
  def toc(start, s = ''):
    us = int((time() - start) * 1000000)
    print us, s

  def tic(s = ''):
    toc(start, s)

  tic('start')
  s1 = ('$test1', sock('$test1'))
  tic('connected 1')
  s2 = ('$test2', sock('$test2'))
  tic('connected 2')

  def send(s1, s2, dict):
    dict['_src'] = s1[0]
    dict['_dst'] = s2[0]

    sd = str(dict)
    tic('sending: %s' % sd)
    start = time()
    s1[1].sendobj(dict)
    tic('sent: %s' % sd)
    tic('recv: %s' % str(s2[1].recvobj()))
    toc(start, 'diff')

  for i in range(1, times):
    send(s1, s2, {'herp' : 'derp', 'num' : i })
    send(s2, s1, {'herp' : 'derp', 'num' : i })


