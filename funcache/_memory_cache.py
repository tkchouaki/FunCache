from funcache._cache import Cache


class MemoryCache(Cache):
    # These attributes are redefined here to prevent having the same values of the class variables of the Base class
    # (& so the siblings)
    _cached_functions = dict()
    _cache = dict()
    _is_init = False
    # Unique to the memory cache
    _accesses = dict()
    # Attributes used for multiprocessing
    _manager = None
    _locks = None
    _running_queries = list()
    _multiprocessing = False

    def __init__(self, cache_size=1000, *args):
        super(MemoryCache).__init__(*args)
        self.cache_size = cache_size

    @classmethod
    def post_cache_init(cls):
        for cached_function in cls._cached_functions:
            MemoryCache._accesses[cached_function] = list()

    def post_get(self, result, *args):
        args_key = self.get_args_key(args)
        if self._multiprocessing:
            self._locks[self.func_key].acquire()
            accesses = self._accesses[self.func_key]
            if args_key in accesses:
                accesses.remove(args_key)
            accesses.append(args_key)
            if len(accesses) > self.cache_size:
                cache = self._cache[self.func_key]
                to_delete = accesses[0]
                if (self.func_key, to_delete) not in self._running_queries:
                    accesses.pop(0)
                    del cache[to_delete]
                    self._cache[self.func_key] = cache
            self._accesses[self.func_key] = accesses
            self._locks[self.func_key].release()
        else:
            if len(self._accesses[self.func_key]) > self.cache_size:
                del self._cache[self.func_key][self._accesses[self.func_key].pop(0)]
        return result

    @classmethod
    def pre_share_context(cls):
        cls._accesses = cls._manager.dict(cls._accesses)

    @classmethod
    def build_context(cls):
        return [cls._accesses]

    @classmethod
    def _receive_context(cls, accesses, *args):
        cls._accesses = accesses
