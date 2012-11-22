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
from . import utils
from .executor import DependencyFailed


class Host(QtCore.QThread):

    executor_message = QtCore.pyqtSignal([object])
    new_worker = QtCore.pyqtSignal([object, object])
    worker_message = QtCore.pyqtSignal([object, object, object])
    
    def __init__(self, conn):
        super(Host, self).__init__()
        
        # Will be set to None if the connection is closed.
        self.conn = conn
        
        self.active_workers = []
        self.blocked_workers = []
        
        self.active_jobs = set()
        self.failed_jobs = set()
    
    def run(self):
        try:
            while True:
                
                # Poke all the blocked workers.
                blocked_workers = self.blocked_workers
                self.blocked_workers = []
                for worker in blocked_workers:
                    worker.poke()
                    if worker.started:
                        self.active_workers.append(worker)
                    else:
                        self.blocked_workers.append(worker)
                
                rlist = [worker.conn for worker in self.active_workers]
                if self.conn is not None:
                    rlist.append(self.conn)
                    
                if not rlist:
                    break
                
                rlist, _, _ = select.select(rlist, [], [])
                for conn in rlist:
                    
                    if conn is self.conn:
                        owner_type = 'executor'
                    else:
                        owner_type = 'worker'
                        worker = [x for x in self.active_workers if x.conn is conn][0]
                    
                    # Get a message, turning EOFs into shutdown messages.
                    try:
                        msg = conn.recv()
                    except EOFError:
                        msg = {'type': 'shutdown'}
                    
                    type_ = msg.pop('type', None)
                    # debug('Host: %r sent %r:\n%s', owner_type, type_, pprint.pformat(msg))
                    
                    handler = getattr(self, 'do_%s_%s' % (owner_type, type_ or 'unknown'), None)
                    
                    if owner_type == 'executor':
                        if handler:
                            handler(**msg)
                        self.executor_message.emit(msg)
                    else:
                        if handler:
                            handler(worker, **msg)
                        self.worker_message.emit(worker, type_, msg)
        
        except:
            traceback.print_exc()
            QtGui.QApplication.exit(1)
        
        finally:
            if self.conn is not None:
                self.conn.send(dict(type='shutdown'))
        
        if not self.failed_jobs:
            QtGui.QApplication.exit(0)
    
    def send(self, msg):
        if self.conn is not None:
            self.conn.send(msg)
    
    def do_executor_submit(self, uuid, **msg):
        self.active_jobs.add(uuid)
        worker = Worker(self, uuid, **msg)
        self.new_worker.emit(worker, msg)
        
        if worker.started:
            self.active_workers.append(worker)
        else:
            self.blocked_workers.append(worker)
            self.worker_message.emit(worker, 'blocked', {})
    
    def do_executor_shutdown(self, **msg):
        # debug('Host: executor shut down')
        self.conn = None
    
    def do_worker_notify(self, worker, **msg):
        msg.setdefault('icon', worker.icon)
        msg.setdefault('title', worker.name)
        utils.notify(**msg)
    
    def do_worker_result(self, worker, **msg):
        self.active_jobs.remove(worker.uuid)
        
        # Forward the message.
        msg['type'] = 'result'
        msg['uuid'] = worker.uuid
        self.send(msg)
    
    def do_worker_exception(self, worker, **msg):
        
        self.failed_jobs.add(worker.uuid)
        self.active_jobs.remove(worker.uuid)
        
        # Forward the message.
        msg['type'] = 'exception'
        msg['uuid'] = worker.uuid
        self.send(msg)
        utils.notify(
            title="Job Errored",
            message=worker.name or 'Untitled',
            sticky=True,
            icon=utils.icon(worker.icon) if worker.icon else None,
        )
        
    def do_worker_shutdown(self, worker):
        
        # Remove it from the list.
        self.active_workers = [x for x in self.active_workers if x is not worker]
        
        # It wasn't done it's job.
        if worker.uuid in self.active_jobs:
            self.do_worker_exception(worker, dict(
                type='exception',
                exception=RuntimeError('worker shutdown unexpectedly'),
            ))


class Worker(object):
    
    def __init__(self, host, uuid, **submit_msg):
        
        self.host = host
        self.uuid = uuid
        self.name = submit_msg.get('name') or submit_msg.get('func_name') or uuid
        self.icon = utils.icon(submit_msg.get('icon') or 'fatcow/gear_in')

        submit_msg['type'] = 'submit'
        submit_msg['uuid'] = uuid
        self.submit_msg = submit_msg
        
        self.started = False
        self.depends_on = submit_msg['depends_on']
        self.poke()
    
    def poke(self):
        
        if self.started:
            return
        
        if any(x in self.host.active_jobs or x in self.host.failed_jobs for x in self.depends_on):
            return
        
        self.started = True
        
        # Launch a worker, and tell it to connect to us.
        self.conn, child_conn = connection.Pipe()
        cmd = ['python', '-m', 'uifutures.sandbox.the_corner', str(child_conn.fileno())]
        proc = subprocess.Popen(cmd)
        child_conn.close()
        
        # Forward the submission.
        self.conn.send(self.submit_msg)
        


class WorkerWidget(QtGui.QFrame):
    
    def __init__(self, worker, **extra):
        super(WorkerWidget, self).__init__()
        self._worker = worker
        self._setup_ui()
    
    def _setup_ui(self):
        self.setLayout(QtGui.QHBoxLayout())
        
        # TODO: Apply this to all but the last one.
        self.setStyleSheet('''
            WorkerWidget {
                border-bottom: 1px dotted rgb(170, 170, 170);
                border-top: none;
            }
        ''')
        
        self._icon = QtGui.QLabel()
        self.layout().addWidget(self._icon)
        pixmap = QtGui.QPixmap(self._worker.icon)
        self._icon.setPixmap(pixmap)
        self._icon.setFixedSize(pixmap.size())
        
        main_layout = QtGui.QVBoxLayout()
        self.layout().addLayout(main_layout)
        
        self._name = QtGui.QLabel(self._worker.name)
        main_layout.addWidget(self._name)
        
        self._progress = QtGui.QProgressBar()
        self._progress.setFixedHeight(12)
        self._progress.setRange(0, 0)
        main_layout.addWidget(self._progress)
                
        self._status = QtGui.QLabel('Starting...')
        font = self._status.font()
        font.setPointSize(10)
        self._status.setFont(font)
        main_layout.addWidget(self._status)
    
    def _handle_message(self, type_, **msg):
        handler = getattr(self, '_do_worker_%s' % type_, None)
        if handler is None:
            return
        handler(**msg)
    
    def _do_worker_blocked(self, **msg):
        self._status.setText('Waiting for another job...')
    
    def _do_worker_handshake(self, pid, **msg):
        self._status.setText('Running as PID %d' % pid)
        
    def _do_worker_result(self, **msg):
        self._status.setText('Done.')
        self._progress.setRange(0, 1)
        self._progress.setValue(1)
    
    def _do_worker_exception(self, **msg):
        self._status.setText('Error.')
        self._progress.setRange(0, 1)
        self._progress.setValue(0)
    
    def _do_worker_progress(self, value=None, maximum=None, status=None, **msg):
        if maximum is not None:
            self._progress.setMaximum(maximum)
        if value is not None:
            self._progress.setValue(value)
        if status is not None:
            self._status.setText(str(status))
            
        


class Dialog(QtGui.QDialog):
    
    def __init__(self, message_processor):
        super(Dialog, self).__init__()
        self._setup_ui()
        self._uuid_to_widget = {}
        
        message_processor.new_worker.connect(self._on_new_worker)
        message_processor.worker_message.connect(self._on_worker_message)
    
    def _setup_ui(self):
        
        self.setWindowTitle("Job Queue")
        self.setMinimumWidth(400)
        
        self.setLayout(QtGui.QVBoxLayout())
        self.layout().setSpacing(0)
        self.layout().setContentsMargins(0, 0, 0, 0)
    
    def _on_new_worker(self, worker, msg):
        widget = WorkerWidget(worker, **msg)
        self._uuid_to_widget[worker.uuid] = widget
        self.layout().addWidget(widget)
    
    def _on_worker_message(self, worker, type_, msg):
        self._uuid_to_widget[worker.uuid]._handle_message(type_, **msg)
            
    


def main():
    
    # Connect to the executor, and start the listener.
    fd = int(os.environ.get('UIFUTURES_HOST_FD') or sys.argv[1])
    conn = _multiprocessing.Connection(fd)
    conn.send(dict(
        type='handshake',
        pid=os.getpid(),
    ))

    message_processor = Host(conn)
    
    app = QtGui.QApplication([])
    app.setApplicationName('Futures Host')
    app.setWindowIcon(QtGui.QIcon(utils.icon('fatcow/road_sign')))
    
    dialog = Dialog(message_processor)
    
    message_processor.start()
    
    dialog.show()
    
    exit(app.exec_())


if __name__ == '__main__':
    main()
