import time
import random
import glob

from uifutures import Executor
from uifutures.worker import set_progress, notify, set_thumbnail


icons = [
    'add',
    'delete',
    'edit',
    'error',
    'go',
    'link',
]
icons = ['/home/mboers/Documents/icons/fatcow/32x32/brick_%s.png' % x for x in icons]


def worker(die_at=None, thumbnails=[]):
    for i in xrange(5):
        if die_at == i:
            raise ValueError('we died')
        for j in xrange(10):
            time.sleep(0.01 + 0.1 * random.random())
            set_progress(i * 10 + j + 1, maximum=50, status='Working... %d of 50' % (i * 10 + j + 1))
            if thumbnails:
                set_thumbnail(random.choice(thumbnails))
    notify(message='Sleeping is complete.')


def main():
    
    import uifutures.examples.sleep
    
    with Executor(max_workers=3) as executor:
        
        thumbs = glob.glob('/var/tmp/srv_playblast/GB_007_001_Anim_mike_testing_v0002_r0002_0000/*.jpg')
        # show_thumbs = executor.submit(uifutures.examples.sleep.worker, thumbnails=thumbs)
        dies = executor.submit_ext(uifutures.examples.sleep.worker, args=(3, ), name="Dies at 3")
        wait_for_death = executor.submit_ext(uifutures.examples.sleep.worker, name='Wait for Death', depends_on=[dies])
        
        futures = []
        for i in range(3):
            future = executor.submit_ext(uifutures.examples.sleep.worker, name='Job #%d' % (i + 1), icon=random.choice(icons))
            futures.append(future)
    
        final = executor.submit_ext(uifutures.examples.sleep.worker, name='Reducer', depends_on=futures)
    
    # res = final.result()

if __name__ == '__main__':
    main()
