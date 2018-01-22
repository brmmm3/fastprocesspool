
import sys
import time
import fastprocesspool_debug as fastprocesspool
from multiprocessing.pool import Pool
if sys.version_info[0] > 2:
    from concurrent.futures import ProcessPoolExecutor
import zstd
import msgpack


class TestValues(object):

    def __init__(self):
        self.result = 0
        self.worker = None
        self.worker_gen = None

    def worker_cb(self, data):
        #print("worker_cb", data)
        return data

    def worker_gen_cb(self, data):
        yield data

    def failed_cb(self, exc):
        print(exc)

    def result_cb(self, result):
        #print("result_cb", result)
        self.result += result

    def results_cb(self, results):
        self.result += sum(results)

    def result_future_cb(self, result):
        self.result += result.result()

    def map(self, data):
        pool = fastprocesspool.Pool()
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum(pool.done)

    def map_no_done(self, data):
        pool = fastprocesspool.Pool()
        pool.map(self.worker, data, False)
        pool.shutdown()

    def map_done_cb(self, data):
        with fastprocesspool.Pool(done_callback = self.result_cb) as pool:
            pool.map(self.worker, data)

    def map_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum(pool.done)

    def imap(self, data):
        pool = fastprocesspool.Pool()
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = sum(pool.done)

    def imap_done_cb(self, data):
        with fastprocesspool.Pool(done_callback = self.result_cb) as pool:
            pool.imap(self.worker_gen, data)

    def imap_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = sum(pool.done)

    def submit(self, data):
        pool = fastprocesspool.Pool()
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = sum(pool.done)

    def submit_pool_done_cb(self, data):
        with fastprocesspool.Pool(done_callback = self.result_cb) as pool:
            for value in data:
                pool.submit(self.worker, value)

    def submit_pool_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = sum(pool.done)

    def submit_done_cb(self, data):
        with fastprocesspool.Pool() as pool:
            for value in data:
                pool.submit_done(self.worker, self.result_cb, value)

    def Pool_map(self, data):
        pool = Pool()
        results = pool.map(self.worker, data)
        pool.close()
        pool.join()
        self.result = sum(results)

    def Pool_map_async_done_cb(self, data):
        pool = Pool()
        pool.map_async(self.worker, data, callback = self.results_cb)
        pool.close()
        pool.join()

    def Pool_apply_async_done_cb(self, data):
        pool = Pool()
        for value in data:
            pool.apply_async(self.worker, ( value, ), callback = self.result_cb)
        pool.close()
        pool.join()

    def ProcessPoolExecutor_map(self, data):
        pool = ProcessPoolExecutor()
        results = pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum(results)

    def ProcessPoolExecutor_submit_done_cb(self, data):
        pool = ProcessPoolExecutor()
        for value in data:
            future = pool.submit(self.worker, value)
            future.add_done_callback(self.result_future_cb)
        pool.shutdown()

    def test(self, test_cb, data):
        self.result = 0
        self.worker = self.worker_cb
        self.worker_gen = self.worker_gen_cb
        t = time.time()
        getattr(self, test_cb)(data)
        print("%6.3f %12d %s" % (time.time() - t, self.result, test_cb))

    def run(self, cnt):
        print("\n%d values:" % cnt)
        values = list(range(cnt))
        self.result = 0
        t = time.time()
        for value in values:
            self.result_cb(self.worker_cb(value))
        print("%6.3f %12d single threaded" % (time.time() - t, self.result))
        t = time.time()
        self.result = sum([ self.worker_cb(value) for value in values ])
        print("%6.3f %12d sum list" % (time.time() - t, self.result))
        print("fastprocesspool:")
        #self.test("map", values)
        #self.test("map_no_done", values)
        #self.test("map_done_cb", values)
        #self.test("map_failed_cb", values)
        #self.test("imap", values)
        #self.test("imap_done_cb", values)
        #self.test("imap_failed_cb", values)
        self.test("submit", values)
        self.test("submit_pool_done_cb", values)
        self.test("submit_pool_failed_cb", values)
        self.test("submit_done_cb", values)
        print("multiprocessing.pool.Pool:")
        #self.test("Pool_map", values)
        #self.test("Pool_map_async_done_cb", values)
        #self.test("Pool_apply_async_done_cb", values)
        print("concurrent.futures.ProcessPoolExecutor:")
        #self.test("ProcessPoolExecutor_map", values)
        #self.test("ProcessPoolExecutor_submit_done_cb", values)


class TestLists(object):

    def __init__(self):
        self.result = 0
        self.worker = None
        self.worker_gen = None

    def worker_cb(self, data):
        return data

    def worker_gen_cb(self, data):
        yield data

    def failed_cb(self, exc):
        print(exc)

    def result_cb(self, result):
        self.result += sum(result)

    def results_cb(self, results):
        self.result += sum([ sum(result) for result in results ])

    def result_future_cb(self, result):
        self.result += sum(result.result())

    def map(self, data):
        pool = fastprocesspool.Pool()
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def map_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        pool.map(self.worker, data)
        pool.shutdown()

    def map_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def imap(self, data):
        pool = fastprocesspool.Pool()
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def imap_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        pool.imap(self.worker_gen, data)
        pool.shutdown()

    def imap_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def submit(self, data):
        pool = fastprocesspool.Pool()
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def submit_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()

    def submit_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = sum([ sum(result) for result in pool.done ])

    def Pool_map(self, data):
        pool = Pool()
        results = pool.map(self.worker, data)
        pool.close()
        pool.join()
        self.result = sum([ sum(result) for result in results ])

    def Pool_map_async_done_cb(self, data):
        pool = Pool()
        pool.map_async(self.worker, data, callback = self.results_cb)
        pool.close()
        pool.join()

    def Pool_apply_async_done_cb(self, data):
        pool = Pool()
        for value in data:
            pool.apply_async(self.worker, ( value, ), callback = self.result_cb)
        pool.close()
        pool.join()

    def ProcessPoolExecutor_map(self, data):
        pool = ProcessPoolExecutor()
        results = pool.map(self.worker, data)
        pool.shutdown()
        self.result = sum([ sum(result) for result in results ])

    def ProcessPoolExecutor_submit_done_cb(self, data):
        pool = ProcessPoolExecutor()
        for value in data:
            future = pool.submit(self.worker, value)
            future.add_done_callback(self.result_future_cb)
        pool.shutdown()

    def test(self, test_cb, data):
        self.result = 0
        self.worker = self.worker_cb
        self.worker_gen = self.worker_gen_cb
        t = time.time()
        getattr(self, test_cb)(data)
        print("%6.3f %10d %s" % (time.time() - t, self.result, test_cb))

    def run(self, n, cnt):
        print("\n%d lists with %d values:" % (n, cnt))
        v = list(range(cnt))
        values = [ v for _ in range(n) ]
        self.result = 0
        t = time.time()
        for value in values:
            self.result_cb(self.worker_cb(value))
        print("%6.3f %10d single threaded" % (time.time() - t, self.result))
        print("fastprocesspool:")
        self.test("map", values)
        self.test("map_done_cb", values)
        self.test("map_failed_cb", values)
        self.test("imap", values)
        self.test("imap_done_cb", values)
        self.test("imap_failed_cb", values)
        self.test("submit", values)
        self.test("submit_done_cb", values)
        self.test("submit_failed_cb", values)
        print("multiprocessing.pool.Pool:")
        self.test("Pool_map", values)
        self.test("Pool_map_async_done_cb", values)
        self.test("Pool_apply_async_done_cb", values)
        print("concurrent.futures.ProcessPoolExecutor:")
        self.test("ProcessPoolExecutor_map", values)
        self.test("ProcessPoolExecutor_submit_done_cb", values)


class TestCompress(object):

    def __init__(self):
        self.result = []
        self.worker = None
        self.worker_gen = None

    def compress_cb(self, data):
        return zstd.ZstdCompressor(write_content_size = True, write_checksum = True,
                                   level = 14).compress(data)

    def compress_gen_cb(self, data):
        yield zstd.ZstdCompressor(write_content_size = True, write_checksum = True,
                                  level = 14).compress(data)

    def pack_compress_cb(self, data):
        result = zstd.ZstdCompressor(write_content_size = True, write_checksum = True,
                                   level = 14).compress(msgpack.packb(data))
        return result

    def pack_compress_gen_cb(self, data):
        yield zstd.ZstdCompressor(write_content_size = True, write_checksum = True,
                                  level = 14).compress(msgpack.packb(data))

    def failed_cb(self, exc):
        print(exc)

    def result_cb(self, result):
        self.result.append(result)

    def results_cb(self, results):
        self.result.extend(results)

    def result_future_cb(self, result):
        self.result.append(result.result())

    def map(self, data):
        pool = fastprocesspool.Pool()
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = list(pool.done)

    def map_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        pool.map(self.worker, data)
        pool.shutdown()

    def map_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.map(self.worker, data)
        pool.shutdown()
        self.result = list(pool.done)

    def imap(self, data):
        pool = fastprocesspool.Pool()
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = list(pool.done)

    def imap_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        pool.imap(self.worker_gen, data)
        pool.shutdown()

    def imap_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        pool.imap(self.worker_gen, data)
        pool.shutdown()
        self.result = list(pool.done)

    def submit(self, data):
        pool = fastprocesspool.Pool()
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = list(pool.done)

    def submit_done_cb(self, data):
        pool = fastprocesspool.Pool(done_callback = self.result_cb)
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()

    def submit_failed_cb(self, data):
        pool = fastprocesspool.Pool(failed_callback = self.failed_cb)
        for value in data:
            pool.submit(self.worker, value)
        pool.shutdown()
        self.result = list(pool.done)

    def Pool_map(self, data):
        pool = Pool()
        results = pool.map(self.worker, data)
        pool.close()
        pool.join()
        self.result = results

    def Pool_map_async_done_cb(self, data):
        pool = Pool()
        pool.map_async(self.worker, data, callback = self.results_cb)
        pool.close()
        pool.join()

    def Pool_apply_async_done_cb(self, data):
        pool = Pool()
        for value in data:
            pool.apply_async(self.worker, ( value, ), callback = self.result_cb)
        pool.close()
        pool.join()

    def ProcessPoolExecutor_map(self, data):
        pool = ProcessPoolExecutor()
        results = pool.map(self.worker, data)
        pool.shutdown()
        self.result = list(results)

    def ProcessPoolExecutor_submit_done_cb(self, data):
        pool = ProcessPoolExecutor()
        for value in data:
            future = pool.submit(self.worker, value)
            future.add_done_callback(self.result_future_cb)
        pool.shutdown()

    def test_compress(self, test_cb, data):
        self.result = []
        self.worker = self.compress_cb
        self.worker_gen = self.compress_gen_cb
        t = time.time()
        getattr(self, test_cb)(data)
        print("%6.3f %10d %s" % (time.time() - t, len(self.result), test_cb))

    def test_pack_compress(self, test_cb, data):
        self.result = []
        self.worker = self.pack_compress_cb
        self.worker_gen = self.pack_compress_gen_cb
        t = time.time()
        getattr(self, test_cb)(data)
        print("%6.3f %10d %s" % (time.time() - t, len(self.result), test_cb))

    def run_compress(self, n, cnt):
        packed_values = msgpack.packb(list(range(n)))
        print("\nCompress %d times %d values:" % (cnt, n))
        values = [ packed_values for _ in range(cnt) ]
        self.result = []
        t = time.time()
        for value in values:
            self.result_cb(self.compress_cb(value))
        print("%6.3f %10d single threaded" % (time.time() - t, len(self.result)))
        print("fastprocesspool:")
        self.test_compress("map", values)
        self.test_compress("map_done_cb", values)
        self.test_compress("map_failed_cb", values)
        self.test_compress("imap", values)
        self.test_compress("imap_done_cb", values)
        self.test_compress("imap_failed_cb", values)
        self.test_compress("submit", values)
        self.test_compress("submit_done_cb", values)
        self.test_compress("submit_failed_cb", values)
        print("multiprocessing.pool.Pool:")
        self.test_compress("Pool_map", values)
        self.test_compress("Pool_map_async_done_cb", values)
        self.test_compress("Pool_apply_async_done_cb", values)
        print("concurrent.futures.ProcessPoolExecutor:")
        self.test_compress("ProcessPoolExecutor_map", values)
        self.test_compress("ProcessPoolExecutor_submit_done_cb", values)

    def run_pack_compress(self, n, cnt):
        print("\nPack and compress %d times %d values:" % (cnt, n))
        values = [ list(range(n)) for _ in range(cnt) ]
        self.result = []
        t = time.time()
        for value in values:
            self.result_cb(self.pack_compress_cb(value))
        print("%6.3f %10d single threaded" % (time.time() - t, len(self.result)))
        print("fastprocesspool:")
        self.test_pack_compress("map", values)
        self.test_pack_compress("map_done_cb", values)
        self.test_pack_compress("map_failed_cb", values)
        self.test_pack_compress("imap", values)
        self.test_pack_compress("imap_done_cb", values)
        self.test_pack_compress("imap_failed_cb", values)
        self.test_pack_compress("submit", values)
        self.test_pack_compress("submit_done_cb", values)
        self.test_pack_compress("submit_failed_cb", values)
        print("multiprocessing.pool.Pool:")
        self.test_pack_compress("Pool_map", values)
        self.test_pack_compress("Pool_map_async_done_cb", values)
        self.test_pack_compress("Pool_apply_async_done_cb", values)
        print("concurrent.futures.ProcessPoolExecutor:")
        self.test_pack_compress("ProcessPoolExecutor_map", values)
        self.test_pack_compress("ProcessPoolExecutor_submit_done_cb", values)

    def run(self, n, cnt):
        self.run_compress(n, cnt)
        self.run_pack_compress(n, cnt)


if __name__ == "__main__":
    test = TestValues()
    test.run(5)
    #test.run(1000000)
    #test = TestLists()
    #test.run(20000, 10000)
    #test = TestCompress()
    #test.run(1000, 10000)
