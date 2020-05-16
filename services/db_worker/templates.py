import threading

class WorkerClass(threading.Thread):

    def __init__(self, **kwargs):
        self.thread = threading.Thread.__init__(self)
        self._is_interrupted = False
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

    def run(self):
        while not self._is_interrupted:
            pass

    def stop(self):
        self._is_interrupted = True