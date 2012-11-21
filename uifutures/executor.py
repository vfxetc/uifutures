import subprocess
import platform
import os
import tempfile
from multiprocessing import connection
from concurrent.futures import _base
import threading
import time
import select
import pprint

from .future import Future
from .utils import debug


class Executor(_base.Executor):
    
    def __init__(self):
        
        address = tempfile.mktemp(prefix='uifutures.', suffix='.sock')
        self._listener = connection.Listener(address)
        
        cmd = ['python', '-m', 'uifutures.host', address]
        proc = subprocess.Popen(cmd)
        
        self._listener._listener._socket.settimeout(3)
        self._conn = self._listener.accept()
        
        msg = self._conn.recv()
        if msg.get('type') != 'handshake' or msg.get('pid') != proc.pid:
            raise RuntimeError('could not shake hands with host: %r' % msg)
        
        self._futures = {}
        self._host_alive = True
        self._listener_thread = threading.Thread(target=self._host_listener)
        self._listener_thread.start()
        self._listener_sleep = 0
        
    def _host_listener(self):

        while self._host_alive:
            try:
                rlist, _, _ = select.select([self._conn], [], [])
                debug('rlist: %r', rlist)
                msg = self._conn.recv()
                debug('Executor: new message of type %r:\n%s', msg.get('type'), pprint.pformat(msg))
            except IOError as e:
                if e.errno == 35:
                    debug('Executor: socket temporarily unavailable; sleeping')
                    time.sleep(0.25)
                else:
                    raise
            except EOFError:
                debug('Executor: EOF')
                self._host_alive = False
            
    def submit(self, func, *args, **kwargs):
        
        uuid = os.urandom(16).encode('hex')
        
        self._conn.send(dict(
            type='job',
            uuid=uuid,
            func=func,
            args=args,
            kwargs=kwargs,
        ))
        
        future = Future()
        self._futures[uuid] = future
        return future
        
        

