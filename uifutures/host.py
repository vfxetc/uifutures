import sys
import os
import traceback
from multiprocessing import connection
import _multiprocessing
import select
import subprocess
import time

from uitools.qt import Qt, QtCore, QtGui

from . import utils


NotSet = object()

INITED = 'INITED'
QUEUED = 'QUEUED'
BLOCKED = 'BLOCKED'
DEPENDENCY_FAILED = 'DEPENDENCY_FAILED'
ACTIVE = 'ACTIVE'
COMPLETE = 'COMPLETE'
FAILED = 'FAILED'

waiting_states = set((INITED, QUEUED, BLOCKED))
failed_states = set((FAILED, DEPENDENCY_FAILED))
finished_states = set((COMPLETE, FAILED, DEPENDENCY_FAILED))

# This will contain the single Host instance.
host = None

class Host(QtCore.QThread):
    
    # (message)
    executor_message = QtCore.pyqtSignal([object])
    
    # (worker, type, message)
    worker_message = QtCore.pyqtSignal([object, object, object])
    
    
    def __init__(self, conn):
        super(Host, self).__init__()
        
        # Will be set to None if the connection is closed.
        self.conn = conn
        
        # Set by a "config" message from the executor.
        self.max_workers = None
        
        # All workers we ever see, by uuid.
        self.workers = {}
        
        # All workers with are non of ACTIVE, FAILED, or DEPENDENCY_FAILED.
        self.unfinished_workers = []
        
    def run(self):
        try:
            
            while True:
                
                # Trigger state changes across all workers. We don't need to
                # bother cascading state changes to earlier workers since
                # dependencies can only be to previous workers.
                
                active_count = sum(int(w.state == ACTIVE) for w in self.unfinished_workers)
                for worker in self.unfinished_workers:
                        
                    # Don't bother poking anyone who is waiting if we have
                    # already reached max_workers.
                    allow_start = (
                        self.max_workers is None or
                        active_count < self.max_workers
                    )
                        
                    old_state = worker.state
                    worker.poke(allow_start)
                    if worker.state != old_state:
                        
                        # Adjust active count.
                        active_count -= int(old_state == ACTIVE)
                        active_count += int(worker.state == ACTIVE)
                        
                        # Send state transition message.
                        self.worker_message.emit(worker, "state_changed", dict(
                            old=old_state,
                            new=worker.state,
                        ))
                
                # Prune all complete workers.
                self.unfinished_workers = [w for w in self.unfinished_workers if w.state not in finished_states]
                
                rlist = [worker.conn for worker in self.unfinished_workers if worker.conn is not None]
                if self.conn is not None:
                    rlist.append(self.conn)
                
                if not rlist:
                    
                    # Wait for changes if there is something that failed, as
                    # the user may hit "Retry".
                    if any(x.state in failed_states for x in self.workers.itervalues()):
                        time.sleep(0.25)
                        continue
                    
                    # There is nothing left to do, and the executor is closed.
                    break
                
                rlist, _, _ = select.select(rlist, [], [])
                for conn in rlist:
                    
                    if conn is self.conn:
                        owner_type = 'executor'
                    else:
                        owner_type = 'worker'
                        worker = [x for x in self.unfinished_workers if x.conn is conn][0]
                    
                    # Get a message, turning EOFs into shutdown messages.
                    # TODO: should these actually be "eof"?
                    try:
                        msg = conn.recv()
                    except EOFError:
                        msg = {'type': 'shutdown'}
                    
                    type_ = msg.pop('type', None)
                    # debug('Host: %r sent %r:\n%s', owner_type, type_, pprint.pformat(msg))
                    
                    # Send the message to methods on ourself, as well as to
                    # the workers.
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
        
        # debug("AT THE END")
        if not any(w.state in failed_states for w in self.workers.itervalues()):
            QtGui.QApplication.exit(0)
    
    def send(self, msg):
        if self.conn is not None:
            self.conn.send(msg)
    
    def do_executor_config(self, max_workers=NotSet, **msg):
        # debug('config: max_workers=%r', max_workers)
        if max_workers is not NotSet:
            self.max_workers = max_workers
    
    def do_executor_submit(self, uuid, **msg):
        
        worker = Worker(self, uuid, **msg)
        self.workers[uuid] = worker
        self.unfinished_workers.append(worker)
        self.worker_message.emit(worker, "new", msg)
    
    def do_executor_shutdown(self, **msg):
        # debug('Host: executor shut down')
        self.conn = None
    
    def do_worker_notify(self, worker, **msg):
        msg.setdefault('icon', worker.icon)
        msg.setdefault('title', worker.name)
        utils.notify(**msg)
    
    def do_worker_result(self, worker, **msg):
        
        self.worker_message.emit(worker, 'state_changed', dict(
            old=worker.state,
            new=COMPLETE
        ))
        worker.state = COMPLETE
        
        # Forward the message.
        msg['type'] = 'result'
        msg['uuid'] = worker.uuid
        self.send(msg)
    
    def do_worker_exception(self, worker, **msg):
        
        self.worker_message.emit(worker, 'state_changed', dict(
            old=worker.state,
            new=FAILED
        ))
        worker.state = FAILED
        
        # Forward the message.
        msg['type'] = 'exception'
        msg['uuid'] = worker.uuid
        self.send(msg)
        msg.setdefault('exception_name', 'Unknown')
        msg.setdefault('exception_message', 'unknown')
        msg.setdefault('exception_traceback', '')
        utils.notify(
            title='Job Failed: %s' % (worker.name or 'Untitled'),
            message='{exception_name}: {exception_message}\n{exception_traceback}'.format(**msg),
            sticky=True,
            icon=utils.icon(worker.icon) if worker.icon else None,
        )
        
    def do_worker_shutdown(self, worker):
        
        # It wasn't done it's job.
        if worker.state not in finished_states:
            self.do_worker_exception(worker, **dict(
                type='exception',
                exception_name='RuntimeError',
                exception_message='worker shutdown unexpectedly; was %r' % worker.state,
                exception=RuntimeError('worker shutdown unexpectedly; was %r' % worker.state),
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
        
        self.state = INITED
        self.conn = None
        self.depends_on = submit_msg['depends_on']
        
        self.retry_count = 0
    
    def poke(self, allow_start):
                    
        if self.state not in waiting_states:
            return
        
        if any(self.host.workers[x].state in failed_states for x in self.depends_on):
            self.state = DEPENDENCY_FAILED
            return
        
        if any(self.host.workers[x].state not in finished_states for x in self.depends_on):
            self.state = BLOCKED
            return
        
        if not allow_start:
            self.state = QUEUED
            return
        
        # Running! Finally...
        self.state = ACTIVE
        
        # Launch a worker, and tell it to connect to us.
        self.conn, child_conn = connection.Pipe()
        cmd = ['python', '-m', 'uifutures.sandbox.the_corner', str(child_conn.fileno())]
        self.proc = subprocess.Popen(cmd)
        child_conn.close()
        
        # Forward the submission.
        self.conn.send(self.submit_msg)
    
    def retry(self):
        
        self.retry_count += 1
        
        # Reset our connection.
        self.conn = None
        
        # Back to the front of the line for us.
        self.state = INITED
        self.host.unfinished_workers.insert(0, self)
        
        for worker in self.host.workers.values():
            if worker.state == DEPENDENCY_FAILED and self.uuid in worker.depends_on:
                
                # To the end of the line for anything that depended on us.
                # We must maintain the ordering of dependencies in the queue.
                worker.state = INITED
                self.host.unfinished_workers.append(worker)


class WorkerWidget(QtGui.QFrame):
    
    def __init__(self, worker, **extra):
        super(WorkerWidget, self).__init__()
        self._worker = worker
        self._setup_ui()
        
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._on_context_menu)
    
    def _setup_ui(self):
        self.setLayout(QtGui.QHBoxLayout())
        
        # TODO: Apply this to all but the last one.
        self.setStyleSheet('''
            WorkerWidget {
                border-bottom: 1px dotted rgb(170, 170, 170);
                border-top: none;
            }
        ''')
        
        self._thumbnail = None
        
        self._icon = QtGui.QLabel()
        self.layout().addWidget(self._icon)
        pixmap = QtGui.QPixmap(self._worker.icon)
        self._icon.setPixmap(pixmap)
        self._icon.setFixedSize(pixmap.size())
        
        self._main_layout = main_layout = QtGui.QVBoxLayout()
        self.layout().addLayout(main_layout)
        
        self._name = QtGui.QLabel(self._worker.name)
        self._name_layout = QtGui.QHBoxLayout()
        self._name_layout.addWidget(self._name)
        main_layout.addLayout(self._name_layout)
        
        self._progress = QtGui.QProgressBar()
        self._progress.setFixedHeight(12)
        self._progress.setRange(0, 0)
        main_layout.addWidget(self._progress)
                
        self._status = QtGui.QLabel('Initializing...')
        font = self._status.font()
        font.setPointSize(10)
        self._status.setFont(font)
        main_layout.addWidget(self._status)
        
        self._button_layout = QtGui.QVBoxLayout()
        main_layout.addLayout(self._button_layout)
        self._buttons = []
    
    def _empty_buttons(self):
        for x in self._buttons:
            x.hide()
            x.destroy()
        self._main_layout.removeItem(self._button_layout)
        self._button_layout = QtGui.QHBoxLayout()
        self._main_layout.addLayout(self._button_layout)
        self._buttons = []
    
    def _add_button(self, x):
        self._button_layout.addWidget(x)
        self._buttons.append(x)
    
    def _on_context_menu(self, point):
        menu = QtGui.QMenu()
        menu.addAction("Test Context Menu")
        menu.exec_(self.mapToGlobal(point))
        
    def _handle_message(self, type_, **msg):
        handler = getattr(self, '_do_%s' % type_, None)
        if handler is None:
            return
        handler(**msg)
    
    def _do_state_changed(self, **msg):
        # old = msg['old'].lower()
        new = msg['new'].lower()
        # debug('Host: %s transition from %s to %s', self._worker.uuid, old, new)
        handlers = [
            # getattr(self, '_do_transition_from_{old}'.format(old=old), None),
            # getattr(self, '_do_transition_from_{old}_to_{new}'.format(old=old, new=new), None),
            getattr(self, '_do_transition_to_{new}'.format(new=new), None),
        ]
        for handler in handlers:
            if handler:
                handler(**msg)
    
    def _do_transition_to_queued(self, **msg):
        self._status.setText('Waiting to start...')
        
    def _do_transition_to_blocked(self, **msg):
        self._status.setText('Waiting for another job...')
        
        # This may have come from a failed state.
        self._status.setStyleSheet('')
        
    def _do_transition_to_active(self, **msg):
        self._status.setText('Starting...')
    
    def _do_handshake(self, pid, **msg):
        self._status.setText('Running as PID %d' % pid)
        
    def _do_result(self, **msg):
        self._status.setText('Done.')
        self._progress.setRange(0, 1)
        self._progress.setValue(1)
    
    def _do_exception(self, exception_name, exception_message, **msg):
        self._set_failure('%s: %s' % (exception_name, exception_message))
        
        self._empty_buttons()
        
        button = QtGui.QToolButton()
        button.setText("Try Again")
        button.setStyleSheet('font-size: 9px;')
        button.clicked.connect(self._retry)
        self._add_button(button)
        
        button = QtGui.QToolButton()
        button.setText("Report Bug")
        button.setStyleSheet('font-size: 9px;')
        # self._add_button(button)
        button.setEnabled(False)
        
        self._button_layout.addStretch()
    
    def _retry(self):
        self._status.setText('Resubmitting...')
        self._status.setStyleSheet('')
        self._worker.retry()
        self._empty_buttons()
        
    def _do_transition_to_dependency_failed(self, **msg):
        self._set_failure('Dependency failed.')
    
    def _set_failure(self, message):
        self._status.setText(message)
        self._status.setStyleSheet('color: darkRed;')
        self._progress.setRange(0, 1)
        self._progress.setValue(0)
        
    def _do_progress(self, value=None, maximum=None, status=None, **msg):
        if maximum is not None:
            self._progress.setMaximum(maximum)
        if value is not None:
            self._progress.setValue(value)
        if status is not None:
            self._status.setText(str(status))
    
    def _do_thumbnail(self, path):
        
        return
        
        if self._thumbnail is None:
            self._icon.setPixmap(self._icon.pixmap().scaled(16, 16, Qt.KeepAspectRatio, Qt.SmoothTransformation))
            self._icon.setFixedSize(self._icon.pixmap().size())
            self._name_layout.insertWidget(0, self._icon)
            self._thumbnail = QtGui.QLabel()
            self.layout().insertWidget(0, self._thumbnail)
            
        pixmap = QtGui.QPixmap(path)
        pixmap = pixmap.scaled(120, 80, Qt.KeepAspectRatio)
        self._thumbnail.setPixmap(pixmap)
            
        


class Dialog(QtGui.QDialog):
    
    def __init__(self, host):
        super(Dialog, self).__init__()
        self._setup_ui()
        self._uuid_to_widget = {}
        
        host.worker_message.connect(self._on_worker_message)
    
    def _setup_ui(self):
        
        self.setWindowTitle("Job Queue")
        self.setMinimumWidth(400)
        
        self.setLayout(QtGui.QVBoxLayout())
        self.layout().setSpacing(0)
        self.layout().setContentsMargins(0, 0, 0, 0)
        
    def _on_worker_message(self, worker, type_, msg):
        
        # This is a new worker.
        if worker.uuid not in self._uuid_to_widget:
            widget = WorkerWidget(worker, type=type_, **msg)
            self._uuid_to_widget[worker.uuid] = widget
            self.layout().addWidget(widget)
        
        self._uuid_to_widget[worker.uuid]._handle_message(type_, **msg)
            
    


def main():
    
    global host
    
    # Connect to the executor, and start the listener.
    fd = int(os.environ.get('UIFUTURES_HOST_FD') or sys.argv[1])
    conn = _multiprocessing.Connection(fd)
    conn.send(dict(
        type='handshake',
        pid=os.getpid(),
    ))

    host = Host(conn)
    
    app = QtGui.QApplication([])
    app.setApplicationName('Futures Host')
    app.setWindowIcon(QtGui.QIcon(utils.icon('fatcow/road_sign')))
    
    dialog = Dialog(host)
    
    host.start()
    
    dialog.show()
    
    exit(app.exec_())


if __name__ == '__main__':
    main()
