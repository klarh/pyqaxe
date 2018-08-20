
LEFT = -1
RIGHT = 1

class LRU_Cache:
    def __init__(self, generator, finalizer, max_size=16):
        self.max_size = max_size
        self.usage_history_ = []
        self.ticks_ = 0
        self.generator = generator
        self.finalizer = finalizer
        self.results_ = {}
        self.last_param_ticks_ = {}
        self.tick_params_ = {}

    def __call__(self, *args, **kwargs):
        params = (args, tuple((k, kwargs[k]) for k in sorted(kwargs)))

        if params not in self.results_:
            self.results_[params] = self.generator(*args, **kwargs)
            self.last_param_ticks_[params] = self.ticks_
            self.tick_params_[self.ticks_] = params
        else: # update tick <-> parameter tracking machinery
            self.tick_params_.pop(self.last_param_ticks_[params])
            self.tick_params_[self.ticks_] = params
            self.last_param_ticks_[params] = self.ticks_

        while len(self) > self.max_size:
            self.popleft()

        self.ticks_ += 1
        return self.results_[params]

    def __len__(self):
        return len(self.results_)

    def popleft(self):
        return self.pop_(LEFT)

    def pop(self):
        return self.pop_(RIGHT)

    def pop_(self, side=LEFT):
        if len(self) == 0:
            name = {LEFT: 'popleft', RIGHT: 'pop'}[side]
            raise IndexError('{} from empty LRU_Cache'.format(name))

        extremum = max if side == RIGHT else min
        oldest_tick = extremum(self.tick_params_)
        oldest_params = self.tick_params_[oldest_tick]
        self.last_param_ticks_.pop(oldest_params)
        self.tick_params_.pop(oldest_tick)

        self.finalizer(self.results_.pop(oldest_params))
        return oldest_params

    def clear(self):
        while len(self):
            self.pop()

    def __del__(self):
        self.clear()
