from funcache import FileCache, MemoryCache
from multiprocessing import Pool, cpu_count
import time


@FileCache()
def test_memory(x):
    time.sleep(x/10000)
    return x**2


@MemoryCache()
def test_file(x):
    time.sleep(x/10000)
    return x**3


def compute(x):
    return test_file(x) + test_memory(x)


if __name__ == "__main__":
    begin_time = time.time()
    values = list(range(10)) * 100
    use_pool = True
    # MemoryCache.deactivate()
    # FileCache.deactivate()
    if use_pool:
        pool_size = cpu_count()
        pool = Pool(pool_size)
        MemoryCache.enable_multiprocessing(pool, pool_size)
        FileCache.enable_multiprocessing(pool, pool_size)
        pool.map(compute, values)
    else:
        for i in values:
            compute(i)
    elapsed_time = time.time() - begin_time
    print(elapsed_time)
