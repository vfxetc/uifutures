import time
import random

from uifutures import Executor
from uifutures.worker import set_progress


def worker():
    for i in xrange(5):
        print '%d...' % (5 - i)
        for j in xrange(10):
            time.sleep(0.01 + 0.1 * random.random())
            set_progress(i * 10 + j + 1, maximum=50, status='Working...')
    # print 'DONE!'
    return 'you slept well'
    

if __name__ == '__main__':
    
    import uifutures.examples.sleep
    
    executor = Executor()
    for i in range(5):
        future = executor.submit(uifutures.examples.sleep.worker)
    res = future.result()
    
