from funcache._cache import Cache


class MemoryCache(Cache):
    """
    This class provides caching functionality for functions results.
    Its usage is simple, the functions just have to be decorated with @MemoryCache(cache_size=size)
    """
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
        """
        Meant to be used as a decorator.
        Enables to cache a function's results in a file
        :param cache_size: The maximum size of the memory cache. ie, the number of results to store
                            When the capacity is exceeded, the  result with the least recent access is removed
        :param args: Just here to ensure possible extensions in sub classes
        """
        super(MemoryCache).__init__(*args)
        self.cache_size = cache_size

    @classmethod
    def post_cache_init(cls):
        """
        Called after the memory caching service is initialized.
        Initializes the lists of accesses to each cache
        """
        for cached_function in cls._cached_functions:
            MemoryCache._accesses[cached_function] = list()

    def post_get(self, result, *args):
        """
        Called after the cache is accessed.
        To ensure the cache sizes do not exceed their limits
        :param result: The result that was retrieved from the cache
        :param args: The arguments that were passed to the function
        """
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
        """
        Converts the accesses lists to ones that are shareable across process
        """
        cls._accesses = cls._manager.dict(cls._accesses)

    @classmethod
    def build_context(cls):
        """
        Adds the list of accesses to the context to share among processes
        :return: The context to share among processes
        """
        return [cls._accesses]

    @classmethod
    def _receive_context(cls, accesses, *args):
        """
        Handles the context reception
        :param accesses: The accesses to the caches
        :param args: Just to ensure compatibility with possible sub classes
        """
        cls._accesses = accesses
