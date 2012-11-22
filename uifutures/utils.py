import sys
import time
import thread
import re
from subprocess import call
import platform


_debug_start = time.time()
_debug_last = _debug_start
_debug_thread_ids = {}
def debug(msg, *args):
    global _debug_last
    if args:
        msg = msg % args
    ident = _debug_thread_ids.setdefault(thread.get_ident(), len(_debug_thread_ids))
    current_time = time.time()
    sys.stdout.write('# %8.3f (%8.3f) %3d %s\n' % ((current_time - _debug_start) * 1000, (current_time - _debug_last) * 1000, ident, msg))
    sys.stdout.flush()
    _debug_last = current_time


def get_func(spec):
    if not isinstance(spec, basestring):
        return spec
    
    m = re.match(r'([\w\.]+):([\w]+)$', spec)
    if not m:
        raise ValueError('string funcs must be for form "package.module:function"')
    mod_name, func_name = m.groups()
    mod = __import__(mod_name, fromlist=['.'])
    return getattr(mod, func_name)


def get_func_name(spec):
    if isinstance(spec, basestring):
        return spec
    return '%s:%s' % (getattr(spec, '__module__', '__module__'), getattr(spec, '__name__', str(spec)))


def notify(message, title=None, app_name=None, sticky=False, icon=None):
    
    if title is None:
        title = 'Job Queue'
    
    if platform.system() == 'Darwin':
        argv = ['growlnotify', '--message', message]
        if app_name:
            argv.extend(('--name', app_name))
        if title:
            argv.extend(('--title', title))
        if sticky:
            argv.append('--sticky')
        if icon:
            argv.extend(('--image', icon))
    else:
        argv = ['notify-send']
        if sticky:
            argv.extend(['-t', '3600000'])
        if icon:
            argv.extend(('--icon', icon))
        argv.extend([title, message])
    
    call(argv)

