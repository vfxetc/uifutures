import time
import random

from uifutures import Executor
from uifutures.worker import set_progress, notify


icons = [
    'add',
    'delete',
    'edit',
    'error',
    'go',
    'link',
]
icons = ['/home/mboers/Documents/icons/fatcow/32x32/brick_%s.png' % x for x in icons]


def worker():
    for i in xrange(5):
        for j in xrange(10):
            time.sleep(0.01 + 0.1 * random.random())
            set_progress(i * 10 + j + 1, maximum=50, status='Working... %d of 50' % (i * 10 + j + 1))
    notify(message='Sleeping is complete.')

if __name__ == '__main__':
    
    import uifutures.examples.sleep
    
    with Executor() as executor:
        
        futures = []
        for i in range(5):
            future = executor.submit_ext(uifutures.examples.sleep.worker, name='Sleeper', icon=random.choice(icons))
            futures.append(future)
    
        final = executor.submit_ext(uifutures.examples.sleep.worker, name='Waiter', depends_on=futures)
    
    # res = final.result()
    
