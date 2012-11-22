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


class MessageProcessor(QtCore.QThread):

    executor_message = QtCore.pyqtSignal([object])
    new_worker = QtCore.pyqtSignal([object, object])
    worker_message = QtCore.pyqtSignal([object, object, object])
    
    def __init__(self, conn):
        super(MessageProcessor, self).__init__()
        self.conn = conn
        self.workers = []
        self.open_jobs = set()
        self.failed_jobs = set()
        self.executor_is_alive = True
    
    def run(self):
        try:
            while True:
                
                rlist = [worker.conn for worker in self.workers]
                if self.executor_is_alive:
                    rlist.append(self.conn)
                    
                if not rlist:
                    break
                
                rlist, _, _ = select.select(rlist, [], [])
                for conn in rlist:
                    
                    if conn is self.conn:
                        owner_type = 'executor'
                    else:
                        owner_type = 'worker'
                        worker = [x for x in self.workers if x.conn is conn][0]
                    
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
            if self.executor_is_alive:
                self.conn.send(dict(type='shutdown'))
        
        if not self.failed_jobs:
            QtGui.QApplication.exit(0)
    
    def do_executor_submit(self, uuid, **msg):
        self.open_jobs.add(uuid)
        worker = Worker(uuid, **msg)
        self.new_worker.emit(worker, msg)
        self.workers.append(worker)
    
    def do_executor_shutdown(self, **msg):
        # debug('Host: executor shut down')
        self.executor_is_alive = False
    
    def do_worker_notify(self, worker, **msg):
        msg.setdefault('icon', worker.icon)
        msg.setdefault('title', worker.name)
        utils.notify(**msg)
    
    def do_worker_result(self, worker, **msg):
        self.open_jobs.remove(worker.uuid)
        
        # Forward the message.
        msg['type'] = 'result'
        msg['uuid'] = worker.uuid
        self.conn.send(msg)
    
    def do_worker_exception(self, worker, **msg):
        
        self.failed_jobs.add(worker.uuid)
        self.open_jobs.remove(worker.uuid)
        
        # Forward the message.
        msg['type'] = 'exception'
        msg['uuid'] = worker.uuid
        self.conn.send(msg)
        utils.notify(
            title="Job Errored",
            message=worker.name or 'Untitled',
            sticky=True,
            icon=worker.icon,
        )
        
    def do_worker_shutdown(self, worker):
        
        # Remove it from the list.
        self.workers = [x for x in self.workers if x is not worker]
        
        # It wasn't done it's job.
        if worker.uuid in self.open_jobs:
            self.do_worker_exception(worker, dict(
                type='exception',
                exception=RuntimeError('worker shutdown unexpectedly'),
            ))


class Worker(object):
    
    def __init__(self, uuid, **submit_msg):
        
        self.uuid = uuid
        self.name = submit_msg.get('name') or submit_msg.get('func_name') or uuid
        self.icon = submit_msg.get('icon') or '/home/mboers/Documents/icons/fatcow/32x32/gear_in.png'

        submit_msg['type'] = 'submit'
        submit_msg['uuid'] = uuid
        self.submit_msg = submit_msg
        
        self.started = False
        self.depends_on = submit_msg['depends_on']
        self.poke()
    
    def poke(self):
        
        if self.started:
            return
        self.started = True
        
        # Launch a worker, and tell it to connect to us.
        self.conn, child_conn = connection.Pipe()
        cmd = ['python', '-m', 'uifutures.sandbox.the_corner', str(child_conn.fileno())]
        proc = subprocess.Popen(cmd)
        child_conn.close()
        
        # Wait for the handshake.
        # TODO: Make this non-blocking, or have a timeout.
        msg = self.conn.recv()
        if msg.get('type') != 'handshake' or msg.get('pid') != proc.pid:
            raise RuntimeError('could not shake hands with worker: %r' % msg)
        
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
                
        self._status = QtGui.QLabel('starting...')
        font = self._status.font()
        font.setPointSize(10)
        self._status.setFont(font)
        main_layout.addWidget(self._status)
    
    def _handle_message(self, type_, **msg):
        
        if type_ == 'result':
            self._status.setText('Done.')
            self._progress.setRange(0, 1)
            self._progress.setValue(1)
            
        if type_ == 'exception':
            self._status.setText('Error.')
            self._progress.setRange(0, 1)
            self._progress.setValue(0)
        
        if type_ == 'progress':
            
            maximum = msg.get('maximum')
            if maximum is not None:
                self._progress.setMaximum(maximum)
            
            value = msg.get('value')
            if value is not None:
                self._progress.setValue(value)
            
            status = msg.get('status')
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

    message_processor = MessageProcessor(conn)
    
    app = QtGui.QApplication([])
    app.setApplicationName('Futures Host')
    app.setWindowIcon(QtGui.QIcon('/home/mboers/Documents/icons/fatcow/32x32/road_sign.png'))
    
    dialog = Dialog(message_processor)
    # dialog.setWindowIcon(QtGui.QIcon('/home/mboers/Documents/icons/fatcow/32x32/road_sign.png'))
    # dialog.setWindowIconText("Testing")
    
    message_processor.start()
    
    dialog.show()
    
    exit(app.exec_())


if __name__ == '__main__':
    main()
