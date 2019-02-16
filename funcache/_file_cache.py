from funcache._cache import Cache
import pickle
import atexit


class FileCache(Cache):
    """
    This classed provides functions results file caching functionality.
    It's simple to use as you just have to decorate your function with *@FileCache(cache_file="cache_file.pkl")
    """
    # These attributes are redefined here to prevent having the same values of the class variables of the Base class
    # (& so the siblings)
    _cached_functions = dict()
    _cache = dict()
    _is_init = False
    # Unique to the file cache
    _default_cache_root = "resources/database/"
    _default_cache_files_extension = ".pkl"
    _cache_files = dict()
    _at_exit_set = False
    # Attributes used for multiprocessing
    _manager = None
    _locks = None
    _running_queries = list()
    _multiprocessing = False

    def __init__(self, cache_file=None, *args):
        """
        Meant to be used as a decorator.
        Enables to cache a function's results in a file
        :param cache_file: The file to be used as a cache.
                            By default, the file path is deduced from the function's qualname & the default cache root
                            & cache files extension
        :param args: Just here to ensure possible extensions in sub classes
        """
        super(FileCache, self).__init__(*args)
        self.cache_file = cache_file

    def post_wrap(self):
        """
        Stores the cache file path
        :return:
        """
        self._cache_files[self.func_key] = self.cache_file

    @classmethod
    def init(cls, default_cache_root="resources/database/", default_cache_files_extension=".pkl"):
        """
        Initializes the FileCache service.
        Note that these arguments won't be taken into accounts for the functions that are cached by specifying the cache file path
        :param default_cache_root: The path of the default folder where to store cache files.
                                   The folder path should include the slash at the end
        :param default_cache_files_extension: The default extension for cache files.
        """
        cls._is_init = True
        cls._default_cache_root = default_cache_root
        cls._default_cache_files_extension = default_cache_files_extension
        for cached_function in cls._cached_functions:
            cls.set_at_exit()
            if cls._cache_files[cached_function] is None:
                cls._cache_files[cached_function] = cls.get_default_cache_file_path(cached_function)
            cache_file_path = cls._cache_files[cached_function]
            try:
                with open(cache_file_path, "rb") as f:
                    cls._cache[cached_function] = pickle.load(f)
                    f.close()
            except FileNotFoundError:
                # If the file doesn't exist, we initialize an empty dictionary for it
                cls._cache[cached_function] = dict()

    @classmethod
    def pre_share_context(cls):
        """
        Transform the cache files paths dictionary to another one that is shareable across processes
        :return:
        """
        cls._cache_files = cls._manager.dict(cls._cache_files)

    @classmethod
    def build_context(cls):
        """
        Adds the cache files paths dictionary to the context to share among other processes
        :return:
        """
        return [cls._cache_files]

    @classmethod
    def _receive_context(cls, cache_files, *args):
        """
        Handles the context reception (which consists of the cache files paths
        :param cache_files: The cache files paths
        :param args: Just to ensure compatibility with possible subclasses
        :return:
        """
        cls._cache_files = cache_files

    @classmethod
    def get_default_cache_file_path(cls, func_key):
        """
        This method isn't used for functions that are decorated using the cache_file attribute
        :param func_key: The function's key
        :return: The default path for a cache file using cache files root and cache files extension
        """
        return cls._default_cache_root + func_key + cls._default_cache_files_extension

    @classmethod
    def set_at_exit(cls):
        """
        Sets the handler to save the cache files when the program ends
        """
        if not cls._at_exit_set:
            cls._at_exit_set = True
            atexit.register(cls.save_files)

    @classmethod
    def save_files(cls):
        """
        Called when the program ends. Saves the cache files
        """
        for cached_function in cls._cache.keys():
            try:
                f = open(cls._cache_files[cached_function], "rb+")
                changed = pickle.load(f)
                if changed is not None and isinstance(changed, dict):
                    for key in changed:
                        if key not in cls._cache[cached_function]:
                            if cls._multiprocessing:
                                cls._locks[cached_function].acquire()
                                entry = cls._cache[cached_function]
                                entry[key] = changed[key]
                                cls._cache[cached_function] = entry
                                cls._locks[cached_function].release()
                            else:
                                cls._cache[cached_function][key] = changed[key]
                f.close()
            except FileNotFoundError:
                pass
            f = open(cls._cache_files[cached_function], "wb")
            pickle.dump(cls._cache[cached_function], f)
            f.close()
