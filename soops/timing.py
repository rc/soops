"""
Elapsed time measurement utilities.
"""
import time
from datetime import datetime

from soops.base import Struct

class Timer(Struct):

    def __init__(self, name='timer'):
        Struct.__init__(self, name=name)
        self.time_function = time.perf_counter
        self.reset()

    def reset(self):
        self.t0 = self.t1 = None
        self.total = self.dt = 0.0

        return self

    def start(self):
        self.t1 = None
        self.t0 = self.time_function()

        return self

    def stop(self):
        self.t1 = self.time_function()
        if self.t0 is None:
            raise ValueError('timer "%s" was not started!' % self.name)

        self.dt = self.t1 - self.t0
        self.total += self.dt
        return self.dt

def get_timestamp(fmt='%Y-%m-%d-%H-%M-%S', dtime=None):
    if dtime is None:
        dtime = datetime.now()
    return dtime.strftime(fmt)
