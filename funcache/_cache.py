import collections
from multiprocessing import Manager
import time


class Cache(object):
    """
    This class provides the basic functionality for caching functions results
    This cache supports multiprocessing
    """
    # A dictionary : function_key -> callable
    _cached_functions = dict()
    # The cache, represented by a dictionary of the form function_key -> dict(args_key -> result)
    _cache = dict()
    # A boolean specifying if the Cache was initialized or not
    _is_init = False
    # A boolean specifying if the Cache is active or not. True by default
    _is_active = True

    # Attributes used for multiprocessing
    # A manager for the sharable objects
    _manager = None
    # Locks for securing accesses, a dictionary of the form function_key -> lock object
    _locks = None
    # A list of the running queries
    _running_queries = list()
    # A boolean specifying whether multiprocessing is set or not
    _multiprocessing = False

    def __init__(self, *args):
        """
        This constructor is meant to be used as a decorator
        Has to be redefined by subclasses to process eventual arguments
        :param args:
        """
        pass

    def __call__(self, original_function):
        """
        Returns the wrapper of the function
        :param original_function:
        :return:
        """
        self.original_function = original_function
        self.func_key = self.cache_function(original_function)
        if self.func_key is None:
            return self.original_function
        temp_self = self

        def wrapper(*args):
            return temp_self._get(*args)

        self.post_wrap()
        return wrapper

    def post_wrap(self):
        """
        This method is meant to be redefined by subclasses.
        It is called just after the function is wrapped & func_key attribute is set.
        """
        pass

    @classmethod
    def init(cls, *args):
        """
        Initializes the cache by setting entries for the cached functions
        If this method is not called explicitly by the user, The cache is automatically initialized at its first use
        :return:
        """
        for cached_function in cls._cached_functions:
            cls._cache[cached_function] = dict()
        cls.post_cache_init()
        cls._is_init = True

    @classmethod
    def post_cache_init(cls):
        """
        This method is meant to be redefined by subclasses
        It is called at the end of the ':func:Cache.init' but before _is_init is set to True
        """
        pass

    @classmethod
    def cache_function(cls, func):
        """
        Adds a function to the cache
        :param func: The function to add
        :return: The key of the function in the cache. if None, it means that the desired key was already occupied
        """
        if func.__qualname__ not in cls._cached_functions:
            cls._cached_functions[func.__qualname__] = func
            return func.__qualname__
        return None

    @staticmethod
    def get_args_key(args):
        """
        This method serves to convert the functions args to hashable objects (because lists are not hashable)
        We make sure to convert them to tuples, (& also their contents).
        :param args: an object representing the arguments to be passed to a function
        :return: The key corresponding to args in the cache
        """
        if isinstance(args, collections.Iterable):
            args = tuple([Cache.get_args_key(arg) for arg in args])
        return args

    @classmethod
    def activate(cls):
        """
        Reactivates the cache service (it is activated by default, so this method is only useful after deactivation).
        Note that this method affects only current process. You should call it separately for each process.
        """
        cls._is_active = True

    @classmethod
    def deactivate(cls):
        """
        Deactivates the cache service (it is activated by default)
        Note that this method affects only current process. You should call it separately for each process.
        :return:
        """
        cls._is_active = False

    def _get(self, *args):
        """
        Computes the function's result for the given args only if it's not already in the cache.
        If the cache is not active, the function is just called
        :param args: The arguments to pass to the function
        :return: The function's result for the given arguments
        """
        if not self._is_active:
            return self.original_function(*args)
        # Initialize the Cache if it's not already done
        if not self._is_init:
            self.init()
        # Retrieve the key for the given arguments
        args_key = self.get_args_key(args)
        # Check if the same function for the same arguments is already being computed in another process
        while (self.func_key, args_key) in self._running_queries:
            time.sleep(0.01)
        # Check if the function's result for the given args is already in the cache
        if args_key not in self._cache[self.func_key].keys():
            # If not, we will compute it, so we have to prevent other process from doing the same thing
            self._running_queries.append((self.func_key, args_key))
            # Compute the result with the original function
            result = self.original_function(*args)
            if self._multiprocessing:
                # If we are using multiprocessing, we have to lock the access to the function's entry in the cache
                self._locks[self.func_key].acquire()
                # We also have to do a little trick to update the cache
                entry = self._cache[self.func_key]
                entry[args_key] = result
                self._cache[self.func_key] = entry
                # We release the lock
                self._locks[self.func_key].release()
            else:
                # If we are not using multiprocessing, the update is straightforward
                self._cache[self.func_key][args_key] = result
            self._running_queries.remove((self.func_key, args_key))
        else:
            # If the result is already present, we just retrieve
            result = self._cache[self.func_key][args_key]
        self.post_get(result, *args)
        return result

    def post_get(self, result, *args):
        """
        This method is meant to be implemented by subclasses.
        It is called after the cache is accessed.
        Note that this method is not called if the cache is not active
        :param result: The result retrieved by the cache
        :param args: The args that were passed to the function
        """
        pass

    @classmethod
    def enable_multiprocessing(cls, pool, pool_size):
        """
        Enables the cache to multiprocessing with a given pool.
        Note the calling the function will convert the cache objects to ones that are shareable across processes.
        Which could slower the access
        :param pool: The pool of processes for which we enable the Cache
        :type pool: multiprocessing.Pool
        :param pool_size: The size of the pool
        """
        if not cls._is_init:
            cls.init()
        cls._multiprocessing = True
        cls._manager = Manager()
        cls._cache = cls._manager.dict(cls._cache)
        cls._locks = dict()
        for cached_function in cls._cached_functions:
            cls._locks[cached_function] = cls._manager.Lock()
        cls._running_queries = cls._manager.list(cls._running_queries)
        # Send the shared objects to the processes
        cls.pre_share_context()
        context = [cls._is_active, cls._cache, cls._locks, cls._running_queries]
        context.extend(cls.build_context())
        pool.starmap(cls.post_spawn, [tuple(context)] * pool_size)

    @classmethod
    def pre_share_context(cls):
        '''
        This method is meant to be redefines by subclasses.
        It is before building the context & sharing it with other processes.
        It could basically be used to convert some local objects to ones that are sharable across processes
        '''
        pass

    @classmethod
    def build_context(cls):
        '''
        This method is meant to be redefined by subclasses
        It should return the objects that are desired to share across processes
        These objects are send as args to :func:~Cache._receive_context() when received by other processes
        :return:
        '''
        return []

    @classmethod
    def _receive_context(cls, *args):
        '''
        This method is meant to be redefined by subclasses
        It should handle the objects sent the main process via the :func:Cache.~build_context() method
        :param args: The objects sent by the main process
        :return:
        '''
        pass

    @classmethod
    def post_spawn(cls, is_active, cache, locks, running_queries, *args):
        """
        This method receives the shared cache objects from the main process
        :param is_active: The activity/inactivity of the Cache
        :param cache: The cache
        :param locks: The locks
        :param running_queries: The running queries
        :param args: Other objects that may have been added by subclasses
        """
        cls._is_init = True
        cls._multiprocessing = True
        cls._is_active = is_active
        cls._cache = cache
        cls._locks = locks
        cls._running_queries = running_queries
        cls._receive_context(*args)




