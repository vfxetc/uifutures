import time

from uifutures import Executor

def worker():
    for i in xrange(5):
        print '%d...' % (5 - i)
        time.sleep(0.2)
    print 'DONE!'
    return 'you slept well'

if __name__ == '__main__':
    
    import uifutures.examples.sleep
    
    executor = Executor()
    for i in range(5):
        future = executor.submit(uifutures.examples.sleep.worker)
    print future
    print 'RESULT: %r' % future.result()
    
