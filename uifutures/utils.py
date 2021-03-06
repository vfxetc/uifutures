from subprocess import call
import os
import re
import sys
import thread
import time


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


def icon(name):
    base, ext = os.path.splitext(name)
    return os.path.abspath(os.path.join(
        __file__, '..', 'art', 'icons', base + (ext or '.png')
    ))


def notify(message, title=None, app_name=None, sticky=False, icon=None):
    
    if title is None:
        title = 'Job Queue'
    
    from uitools.notifications import Notification
    Notification(title, message).send()

