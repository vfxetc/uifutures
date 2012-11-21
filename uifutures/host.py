import sys
import os
import traceback
from multiprocessing import connection
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
                debug('Host: new message of type %r:\n%s', msg.get('type'), pprint.pformat(msg))
                handler = getattr(self, 'do_' + msg.get('type', 'missing'), None)
                if not handler:
                    debug('Host: no handler for %r', msg.get('type'))
                    continue
                msg.pop('type')
                handler(**msg)
        except EOFError:
            pass
        except:
            traceback.print_exc()
            debug('HOST SHUTTING DOWN')
            self.conn.send(dict(type='shutdown'))
            exit()
    
    def do_job(self, uuid, func, args, kwargs):
        res = func(*args, **kwargs)
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
    address = sys.argv[1]
    conn = connection.Client(address)
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
