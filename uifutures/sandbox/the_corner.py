# Nobody puts us in the corner... except us.

# This is as clean of an execution environment as we can muster for apps like
# Maya which will execute arbitrary code in the __main__ module.

from uifutures.worker import main as _uifutures_main

_uifutures_main()
