import time

from uifutures import Executor

def worker():
    time.sleep(5)

if __name__ == '__main__':
    
    import uifutures.examples.sleep
    
    executor = Executor()
    future = executor.submit(uifutures.examples.sleep.worker)
    print future
    print 'RESULT: %r' % future.result()
    
