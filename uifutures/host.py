import sys
import os
import traceback
from multiprocessing import connection
import _multiprocessing
import pprint

from PyQt4 import QtCore, QtGui
Qt = QtCore.Qt

from .utils import debug


class Listener(QtCore.QThread):
    
    def __init__(self, conn):
        super(Listener, self).__init__()
        self.conn = conn
    
    def run(self):
        try:
            while True:
                msg = self.conn.recv()
                type_ = msg.pop('type', None)
                debug('Host: new message of type %r:\n%s',type_, pprint.pformat(msg))
                handler = getattr(self, 'do_' + (type_ or 'missing'), None)
                if not handler:
                    debug('Host: no handler for %r', msg.get('type'))
                    continue
                handler(**msg)
        except EOFError:
            pass
        except:
            traceback.print_exc()
            debug('HOST SHUTTING DOWN')
            self.conn.send(dict(type='shutdown'))
            exit()
    
    def do_job(self, uuid, func, args, kwargs):
        try:
            res = func(*args, **kwargs)
        except Exception as e:
            debug('Host: exception: %r', e)
            self.conn.send(dict(
                type='exception',
                uuid=uuid,
                exception=e,
            ))
        else:
            debug('Host: result: %r', res)
            self.conn.send(dict(
                type='result',
                uuid=uuid,
                result=res
            ))


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
    listener = Listener(conn)
    listener.start()
    
    app = QtGui.QApplication([])
    app.setApplicationName('Futures Host')
    
    dialog = Dialog()
    dialog.show()
    
    app.exec_()


if __name__ == '__main__':
    main()
