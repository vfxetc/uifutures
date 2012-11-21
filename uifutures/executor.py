from concurrent.futures import _base
from multiprocessing import connection
import os
import platform
import pprint
import select
import subprocess
import tempfile
import threading
import time

from .utils import debug


class HostShutdown(RuntimeError):
    pass


class Executor(_base.Executor):
    
    def __init__(self):
        
        # Launch a host, and tell it to connect to us.
        self._conn, child_conn = connection.Pipe()
        cmd = ['python', '-m', 'uifutures.host', str(child_conn.fileno())]
        proc = subprocess.Popen(cmd)
        
        # Wait for the handshake.
        # TODO: Make this non-blocking, or have a timeout.
        msg = self._conn.recv()
        if msg.get('type') != 'handshake' or msg.get('pid') != proc.pid:
            raise RuntimeError('could not shake hands with host: %r' % msg)
        
        self._futures = {}
        
        self._host_alive = True
        self._host_listener_thread = threading.Thread(target=self._host_listener)
        self._host_listener_thread.daemon = True
        self._host_listener_thread.start()
        
    def _host_listener(self):
        try:
            while self._host_alive:
                try:
                    rlist, _, _ = select.select([self._conn], [], [])
                    msg = self._conn.recv()
                    type_ = msg.pop('type', None)
                    debug('Executor: new message of type %r:\n%s', type_, pprint.pformat(msg))
                    handler = getattr(self, '_do_' + (type_ or 'missing'), None)
                    if not handler:
                        debug('Executor: no handler for %r', type_)
                        continue
                    handler(**msg)
                except IOError as e:
                    if e.errno == 35:
                        debug('Executor: socket temporarily unavailable; sleeping')
                        time.sleep(0.25)
                    else:
                        raise
        except EOFError:
            debug('Executor: EOF')
        finally:
            self._do_shutdown()
    
    def _do_shutdown(self):
        self._host_alive = False
        debug('Executor: host shutdown')
        for future in self._futures.itervalues():
            future.set_exception(HostShutdown('host shutdown'))
    
    def _do_result(self, uuid, result):
        debug('Executor: %s finished', uuid)
        future = self._futures.pop(uuid)
        future.set_result(result)
        
    def _do_exception(self, uuid, exception):
        debug('Executor: %s errored', uuid)
        future = self._futures.pop(uuid)
        future.set_exception(exception)
    
    def submit(self, func, *args, **kwargs):
        
        uuid = os.urandom(16).encode('hex')
        
        self._conn.send(dict(
            type='job',
            uuid=uuid,
            func=func,
            args=args,
            kwargs=kwargs,
        ))
        
        future = _base.Future()
        self._futures[uuid] = future
        return future
        
        

