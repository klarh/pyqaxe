
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
        params = (args, tuple((k, kwargs[v]) for k in sorted(kwargs)))

        if params not in self.results_:
            self.results_[params] = self.generator(*args, **kwargs)
            self.last_param_ticks_[params] = self.ticks_
            self.tick_params_[self.ticks_] = params
        else: # update tick <-> parameter tracking machinery
            self.tick_params_.pop(self.last_param_ticks_[params])
            self.tick_params_[self.ticks_] = params
            self.last_param_ticks_[params] = self.ticks_

        while len(self.results_) > self.max_size:
            oldest_tick = min(self.tick_params_)
            oldest_params = self.tick_params_[oldest_tick]
            self.last_param_ticks_.pop(oldest_params)
            self.tick_params_.pop(oldest_tick)

            self.finalizer(self.results_.pop(oldest_params))

        self.ticks_ += 1
        return self.results_[params]
