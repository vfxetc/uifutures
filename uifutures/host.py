import sys
import os
import traceback
from multiprocessing import connection
import _multiprocessing
import pprint
import select
import subprocess

from PyQt4 import QtCore, QtGui
Qt = QtCore.Qt

from .utils import debug


class MessageProcessor(QtCore.QThread):
    
    def __init__(self, conn):
        super(MessageProcessor, self).__init__()
        self.conn = conn
        self.workers = []
        self.open_jobs = set()
    
    def run(self):
        try:
            while True:
                
                rlist = [worker.conn for worker in self.workers]
                rlist.append(self.conn)
                rlist, _, _ = select.select(rlist, [], [])
                for conn in rlist:
                    
                    if conn is self.conn:
                        owner_type = 'executor'
                    else:
                        owner_type = 'worker'
                        worker = [x for x in self.workers if x.conn is conn][0]
                
                    try:
                        msg = conn.recv()
                    except EOFError:
                        if owner_type == 'worker':
                            self.do_worker_shutdown(worker)
                            continue
                        else:
                            return
                    
                    type_ = msg.pop('type', None)
                    debug('Host: %r send %r:\n%s', owner_type, type_, pprint.pformat(msg))
                    
                    handler = getattr(self, 'do_%s_%s' % (owner_type, type_ or 'unknown'), None)
                    if not handler:
                        debug('Host: no %s handler for %r', owner_type, type_)
                        continue
                    
                    if owner_type == 'executor':
                        handler(**msg)
                    else:
                        handler(worker, **msg)
        
        except:
            traceback.print_exc()
            debug('HOST SHUTTING DOWN')
            self.conn.send(dict(type='shutdown'))
            exit()
    
    def do_executor_submit(self, uuid, func, args, kwargs):
        worker = Worker(uuid)
        worker.conn.send(dict(
            type='submit',
            uuid=uuid,
            func=func,
            args=args,
            kwargs=kwargs,
        ))
        self.workers.append(worker)
        self.open_jobs.add(uuid)
    
    def do_worker_result(self, worker, result):
        self.open_jobs.remove(worker.uuid)
        self.conn.send(dict(
            type='result',
            uuid=worker.uuid,
            result=result,
        ))
    
    def do_worker_exception(self, worker, exception):
        self.open_jobs.remove(worker.uuid)
        self.conn.send(dict(
            type='exception',
            uuid=worker.uuid,
            exception=exception,
        ))
        
    def do_worker_shutdown(self, worker):
        
        # Remove it from the list.
        self.workers = [x for x in self.workers if x is not worker]
        
        # It wasn't done it's job.
        if worker.uuid in self.open_jobs:
            self.conn.send(dict(type='exception', exception=RuntimeError('worker shutdown unexpectedly')))
        else:
            pass
            # debug('Host: worker shutdown after job')


class Worker(object):
    
    def __init__(self, uuid):
    
        self.uuid = uuid
        
        # Launch a worker, and tell it to connect to us.
        self.conn, child_conn = connection.Pipe()
        cmd = ['python', '-m', 'uifutures.worker', str(child_conn.fileno())]
        proc = subprocess.Popen(cmd)
        child_conn.close()
        
        # Wait for the handshake.
        # TODO: Make this non-blocking, or have a timeout.
        msg = self.conn.recv()
        if msg.get('type') != 'handshake' or msg.get('pid') != proc.pid:
            raise RuntimeError('could not shake hands with worker: %r' % msg)
        


class Dialog(QtGui.QDialog):
    
    def __init__(self):
        super(Dialog, self).__init__()
        self._setup_ui()
    
    def _setup_ui(self):
        
        self.setWindowTitle("UI Futures")
        self.setLayout(QtGui.QVBoxLayout())
        
        button = QtGui.QPushButton('Exit')
        button.clicked.connect(lambda *args: exit())
        self.layout().addWidget(button)
    


def main():
    
    # Connect to the executor, and start the listener.
    fd = int(sys.argv[1])
    conn = _multiprocessing.Connection(fd)
    conn.send(dict(
        type='handshake',
        pid=os.getpid(),
    ))
    message_processor = MessageProcessor(conn)
    message_processor.start()
    
    app = QtGui.QApplication([])
    app.setApplicationName('Futures Host')
    
    dialog = Dialog()
    dialog.show()
    
    app.exec_()


if __name__ == '__main__':
    main()
