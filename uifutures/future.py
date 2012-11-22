from concurrent.futures import _base


class Future(_base.Future):
    
    def __init__(self, uuid):
        super(Future, self).__init__()
        self.uuid = uuid

