# Copyright 2018 Martin Bammer. All Rights Reserved.
# Licensed under MIT license.

"""Implements a lightweight as fast process pool."""

__author__ = 'Martin Bammer (mrbm74@gmail.com)'

import sys
import atexit
import time
import io
import threading
import itertools
from collections import deque
from multiprocessing import Process, Pipe
if sys.version_info[0] > 2:
    import _thread
    from os import cpu_count
else:
    import thread as _thread
    from multiprocessing import cpu_count


# Create own semaphore class which is much faster than the original version in the
# threading module.
class Semaphore(object):

    def __init__(self):
        self._value = 0
        self._value_lock = _thread.allocate_lock()
        self._zero_lock = _thread.allocate_lock()
        self._zero_lock.acquire()

    def acquire(self):
        if self._value < 1:
            self._zero_lock.acquire()
        with self._value_lock:
            self._value -= 1

    def release(self):
        if self._zero_lock.locked():
            try:
                self._zero_lock.release()
            except:
                pass
        with self._value_lock:
            self._value += 1


_shutdown = False
_job_cnt = Semaphore()
_childs = set()

LOGGER_NAME = 'fastprocesspool'
DEFAULT_LOGGING_FORMAT = '[%(levelname)s/%(processName)s] %(message)s'


def _python_exit():
    global _shutdown
    _shutdown = True
    _job_cnt.release()
    for thread in _childs:
        thread.join()

atexit.register(_python_exit)


class Redirect2Pipe(object):

   def __init__(self, stream):
       self.stream = stream

   def write(self, buf):
       self.stream.send(buf)


def _child(stdout, stderr, jobs, errors):
    sys.stdout = Redirect2Pipe(stdout)
    sys.stderr = Redirect2Pipe(stderr)
    jobs_recv = jobs.recv
    jobs_send = jobs.send
    errors_send = errors.send
    while True:
        try:
            job = jobs_recv()
        except Exception as exc:
            errors_send(exc)
            break
        if job is None:
            break
        try:
            fn, done_callback, args, kwargs = job
            jobs_send(( done_callback, fn(*args, **kwargs) ))
        except Exception as exc:
            errors_send(exc)


def _child_results_thread(have_results, results, jobs):
    jobs_send = jobs.send
    while True:
        have_results.acquire()
        results_chunk = []
        exit_thread = False
        try:
            while True:
                result = results.popleft()
                if result is None:
                    exit_thread = True
                    break
                results_chunk.append(result)
        except:
            pass
        if results_chunk:
            jobs_send(results_chunk)
        if exit_thread:
            break


def _child_chunks(stdout, stderr, child_id, jobs, errors, max_chunk_size, itr):
    sys.stdout = Redirect2Pipe(stdout)
    sys.stderr = Redirect2Pipe(stderr)
    errors_send = errors.send
    results = deque()
    have_results = _thread.allocate_lock()
    have_results.acquire()
    thr_res = threading.Thread(target = _child_results_thread, args = ( have_results, results, jobs ),
                               name = child_id + "child_results")
    thr_res.daemon = True
    thr_res.start()
    if itr is None:
        jobs_recv = jobs.recv
        while True:
            try:
                jobs = jobs_recv()
            except Exception as exc:
                errors_send(exc)
                break
            if jobs is None:
                break
            for job in jobs:
                try:
                    fn, done_callback, args, kwargs = job
                    results.append((done_callback, fn(*args, **kwargs)))
                    if (len(results) >= max_chunk_size) and have_results.locked():
                        have_results.release()
                except Exception as exc:
                    errors_send(exc)
    else:
        for job in itr:
            try:
                fn, args = job
                results.append(fn(args))
                if (len(results) >= max_chunk_size) and have_results.locked():
                    have_results.release()
            except Exception as exc:
                errors_send(exc)
    results.append(None)
    if have_results.locked():
        have_results.release()
    thr_res.join()


class Pool(object):

    def __init__(self, max_childs = None, child_name_prefix = "", done_callback = None,
                 failed_callback = None, log_level = None, max_chunk_size = 1):
        global _shutdown, _job_cnt
        _shutdown = False
        _job_cnt = Semaphore()
        self.max_childs = None
        if max_childs is None:
            self.max_childs = cpu_count()
        elif max_childs > 0:
            self.max_childs = max_childs
        else:
            self.max_childs = cpu_count() + max_childs
        if self.max_childs <= 0:
            raise ValueError("Number of child threads must be greater than 0")
        self.child_name_prefix = child_name_prefix  + "-" if child_name_prefix else "ProcessPool%s-" % id(self)
        self._max_chunk_size = max_chunk_size
        self._child_cnt = 0
        self.jobs = deque()
        self.done = deque()
        self.failed = deque()
        self._done_cnt = Semaphore()
        self._failed_cnt = Semaphore()
        self.logger = None
        if done_callback:
            self._thr_done = threading.Thread(target = self._done_thread, args = ( done_callback, ),
                                              name = "ProcessPoolDone")
            self._thr_done.daemon = True
            self._thr_done.start()
        if failed_callback or log_level:
            if log_level:
                import logging
                self.logger = logging.getLogger(LOGGER_NAME)
                self.logger.propagate = False
                formatter = logging.Formatter(DEFAULT_LOGGING_FORMAT)
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                if log_level:
                    self.logger.setLevel(log_level)
            self._thr_failed = threading.Thread(target = self._failed_thread, args = ( failed_callback, ),
                                                name = "ProcessPoolFailed")
            self._thr_failed.daemon = True
            self._thr_failed.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def _stdout_thread(self, stdout):
        stdout_recv = stdout.recv
        while not _shutdown:
            text = stdout_recv()
            if text is None:
                break
            if self.logger is None:
                print(text)
            else:
                self.logger.info(text)

    def _stderr_thread(self, stderr):
        stderr_recv = stderr.recv
        while not _shutdown:
            text = stderr_recv()
            if text is None:
                break
            if self.logger is None:
                print(text, file = sys.stderr)
            else:
                self.logger.error(text)

    def _done_thread(self, done_callback):
        done_popleft = self.done.popleft
        _done_cnt_acquire = self._done_cnt.acquire
        while not _shutdown:
            try:
                result = done_popleft()
            except Exception as exc:
                if _shutdown:
                    break
                _done_cnt_acquire()
            else:
                done_callback(result)

    def _failed_thread(self, failed_callback):
        failed_popleft = self.failed.popleft
        _failed_cnt_acquire = self._failed_cnt.acquire
        while not _shutdown:
            try:
                result = failed_popleft()
            except Exception as exc:
                if _shutdown:
                    break
                _failed_cnt_acquire()
            else:
                failed_callback(result)

    def _sender_thread_loop(self, job_conn):
        _job_cnt_acquire = _job_cnt.acquire
        failed_append = self.failed.append
        jobs_popleft = self.jobs.popleft
        job_conn_send = job_conn.send
        while True:
            try:
                job = jobs_popleft()
            except:
                if _shutdown:
                    _job_cnt.release()
                    break
                # Locking is expensive. So only use it when needed.
                _job_cnt_acquire()
                continue
            if job is None or _shutdown:
                _job_cnt.release()
                break
            try:
                job_conn_send(job)
            except Exception as exc:
                failed_append(exc)

    def _sender_thread_loop_chunks(self, job_conn):
        _job_cnt_acquire = _job_cnt.acquire
        failed_append = self.failed.append
        jobs_popleft = self.jobs.popleft
        job_conn_send = job_conn.send
        running = True
        while running:
            jobs = []
            for _ in range(self._max_chunk_size):
                try:
                    job = jobs_popleft()
                    if job is None:
                        running = False
                        break
                    jobs.append(job)
                except:
                    break
            if _shutdown:
                _job_cnt.release()
                break
            if not jobs and running:
                # Locking is expensive. So only use it when needed.
                _job_cnt_acquire()
                continue
            try:
                job_conn_send(jobs)
            except Exception as exc:
                failed_append(exc)

    def _sender_thread(self, job_conn, rem_job_conn, rem_exc_conn, rem_stdout, rem_stderr,
                       thr_stdout, thr_stderr, thr_rcv, thr_exc, child, itr):
        if itr is None:
            if self._max_chunk_size > 1 :
                self._sender_thread_loop_chunks(job_conn)
            else:
                self._sender_thread_loop(job_conn)
            job_conn.send(None)
            child.join()
        else:
            while not _shutdown and child.is_alive():
                child.join(0.1)
        rem_job_conn.send(None)
        self._join_thread(thr_rcv, 0.0, None)
        rem_exc_conn.send(None)
        self._join_thread(thr_exc, 0.0, None)
        if thr_stdout.is_alive():
            rem_stdout.send(None)
            thr_stdout.join()
        if thr_stderr.is_alive():
            rem_stderr.send(None)
            thr_stderr.join()

    def _receiver_thread(self, job_conn, done_append):
        _done_cnt = self._done_cnt
        _done_cnt_release = _done_cnt.release
        done_append = self.done.append if done_append else False
        done_extend = self.done.extend
        job_conn_recv = job_conn.recv
        while True:
            results = job_conn_recv()
            if results is None or _shutdown:
                break
            if done_append is False:
                continue
            if isinstance(results, tuple):
                done_callback, value = results
                if done_callback is True:
                    done_append(value)
                    if _done_cnt._value < 1:
                        _done_cnt_release()
                elif callable(done_callback):
                    done_callback(value)
            else:
                for result in results:
                    done_callback, value = result
                    if done_callback is True:
                        done_append(value)
                        if _done_cnt._value < 1:
                            _done_cnt_release()
                    elif callable(done_callback):
                        done_callback(value)

    def _fast_receiver_thread(self, job_conn, done_append):
        _done_cnt = self._done_cnt
        _done_cnt_release = _done_cnt.release
        done_append = self.done.append if done_append else False
        done_extend = self.done.extend
        job_conn_recv = job_conn.recv
        while True:
            results = job_conn_recv()
            if results is None or _shutdown:
                break
            if done_append is False:
                continue
            done_extend(results)
            if _done_cnt._value < 1:
                _done_cnt_release()

    def _exception_thread(self, exc_conn):
        _failed_cnt = self._failed_cnt
        _failed_cnt_release = _failed_cnt.release
        failed_append = self.failed.append
        exc_conn_recv = exc_conn.recv
        while True:
            exc = exc_conn_recv()
            if exc is None or _shutdown:
                break
            failed_append(exc)
            if _failed_cnt._value < 1:
                _failed_cnt_release()

    def _start_child(self, itr, done_append):
        self._child_cnt += 1
        child_id = self.child_name_prefix + str(self._child_cnt)
        loc_job_conn, rem_job_conn = Pipe()
        loc_exc_conn, rem_exc_conn = Pipe()
        loc_stdout, rem_stdout = Pipe(False)
        loc_stderr, rem_stderr = Pipe(False)
        if (self._max_chunk_size > 1) or not itr is None:
            child = Process(target =_child_chunks,
                             args = ( rem_stdout, rem_stderr, child_id, rem_job_conn, rem_exc_conn,
                                      self._max_chunk_size, itr ),
                             name = child_id + "child_chunks")
        else:
            child = Process(target = _child,
                             args = ( rem_stdout, rem_stderr, rem_job_conn, rem_exc_conn ),
                             name = child_id + "child")
        child.daemon = True
        child.start()
        thr_stdout = threading.Thread(target = self._stdout_thread, args = ( loc_stdout, ),
                                      name = child_id + "stdout")
        thr_stdout.daemon = True
        thr_stdout.start()
        thr_stderr = threading.Thread(target = self._stderr_thread, args = ( loc_stderr, ),
                                      name = child_id + "stderr")
        thr_stderr.daemon = True
        thr_stderr.start()
        thr_rcv = threading.Thread(target = self._receiver_thread if itr is None else self._fast_receiver_thread,
                                   args = ( loc_job_conn, done_append ),
                                   name = child_id + "rcv")
        thr_rcv.daemon = True
        thr_rcv.start()
        thr_exc = threading.Thread(target = self._exception_thread, args = ( loc_exc_conn, ),
                                   name = child_id + "exc")
        thr_exc.daemon = True
        thr_exc.start()
        thr_snd = threading.Thread(target = self._sender_thread,
                                   args = ( loc_job_conn, rem_job_conn, rem_exc_conn, rem_stdout, rem_stderr,
                                            thr_stdout, thr_stderr, thr_rcv, thr_exc, child, itr ),
                                   name = child_id + "snd")
        thr_snd.daemon = True
        thr_snd.start()
        _childs.add(thr_snd)

    def _submit(self, fn, done_callback, args, kwargs):
        if _shutdown:
            raise ValueError("Pool not running")
        if (self._child_cnt == 0) or ((self._child_cnt < self.max_childs) and self.jobs):
            self._start_child(None, True)
        self.jobs.append(( fn, done_callback, args, kwargs ))
        if _job_cnt._value < 1:
            # Locking is expensive. So only use it when needed.
            _job_cnt.release()

    def submit(self, fn, *args, **kwargs):
        self._submit(fn, True, args, kwargs)

    def submit_done(self, fn, done_callback, *args, **kwargs):
        self._submit(fn, done_callback, args, kwargs)

    def map(self, fn, itr, done_append = True, shutdown_timeout = None):
        # done can be False, True or function
        if _shutdown:
            raise ValueError("Pool not running")
        itr = [ ( fn, it ) for it in itr ]
        chunksize, extra = divmod(len(itr), self.max_childs)
        if extra:
            chunksize += 1
        if self._max_chunk_size < 1:
            self._max_chunk_size = chunksize
        it = iter(itr)
        for i in range(min(len(itr), self.max_childs)):
            self._start_child(list(itertools.islice(it, chunksize)), done_append)
        if not shutdown_timeout is None:
            self.shutdown(shutdown_timeout)

    def _join_thread(self, thread, t, timeout):
        global _shutdown
        dt = 0.1 if timeout is None else t - time.time()
        while True:
            try:
                thread.join(dt)
                if not thread.is_alive():
                    return True
                if not timeout is None:
                    raise TimeoutError("Failed to join thread %s" % thread.name)
            except KeyboardInterrupt:
                _shutdown = True
                raise
            except:
                pass

    def shutdown(self, timeout = None):
        global _shutdown
        for _ in range(self.max_childs):
            self.jobs.append(None)
        _job_cnt.release()
        t = None if timeout is None else time.time() + timeout
        for thr_snd in _childs:
            self._join_thread(thr_snd, t, timeout)
        _childs.clear()
        if hasattr(self, "_thr_done"):
            _shutdown = True
            self._done_cnt.release()
            self._join_thread(self._thr_done, t, timeout)
        if hasattr(self, "_thr_failed"):
            _shutdown = True
            self._failed_cnt.release()
            self._join_thread(self._thr_failed, t, timeout)

    def cancel(self):
        global _shutdown
        _shutdown = True
        for _ in range(self.max_childs):
            self.jobs.appendleft(None)
        _job_cnt.release()
