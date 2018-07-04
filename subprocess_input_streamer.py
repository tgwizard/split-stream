import subprocess
from threading import Thread
from time import time
from typing import Union
from queue import Queue


class SubprocessInputStreamer:
    _end_of_stream = object()

    def __init__(self, popen_args, popen_kwargs=None, timeout=10):
        self.popen_args = popen_args
        self.popen_kwargs = popen_kwargs
        self.running = False
        self.timeout = timeout

    def start(self):
        assert not self.running

        self.stdout = None
        self.stderr = None
        self.error = None
        self.exit_code = None

        self._process = subprocess.Popen(self.popen_args, **(self.popen_kwargs or {}))

        self._queue = Queue()
        self.running = True

        if self._process.stdin is not None:
            self._stdin_thread = Thread(target=self._thread_write)
            self._stdin_thread.daemon = True
            self._stdin_thread.start()

        if self._process.stdout is not None:
            self._stdout_thread = Thread(
                target=self._thread_read, args=(self._process.stdout, 'stdout')
            )
            self._stdout_thread.daemon = True
            self._stdout_thread.start()

        if self._process.stderr is not None:
            self._stderr_thread = Thread(
                target=self._thread_read, args=(self._process.stderr, 'stderr')
            )
            self._stderr_thread.daemon = True
            self._stderr_thread.start()

    def stop(self):
        if not self.running:
            return

        end_time = time() + self.timeout

        self._queue.put(self._end_of_stream)
        self._queue.join()
        self._process.stdin.flush()
        self._process.stdin.close()

        self.running = False

        try:
            if self._process.stdin:
                self._stdin_thread.join(timeout=self._remaining_time(end_time))
                if self._stdin_thread.is_alive():
                    raise subprocess.TimeoutExpired(self._process.args, self.timeout)
            if self._process.stdout:
                self._stdout_thread.join(timeout=self._remaining_time(end_time))
                if self._stdout_thread.is_alive():
                    raise subprocess.TimeoutExpired(self._process.args, self.timeout)
            if self._process.stderr:
                self._stderr_thread.join(timeout=self._remaining_time(end_time))
                if self._stderr_thread.is_alive():
                    raise subprocess.TimeoutExpired(self._process.args, self.timeout)

            self.exit_code = self._process.wait(timeout=self._remaining_time(end_time))
        except Exception as e:
            self.error = e
            self._process.kill()
            self._process.wait()
        finally:
            self._process = None
            self._queue = None

    @classmethod
    def _remaining_time(cls, end_time):
        return end_time - time()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def write(self, b: Union[bytes, memoryview]):
        assert self.running, 'Must start before writing'
        if isinstance(b, memoryview):
            b = bytes(b)
        self._queue.put(b)

    def _thread_write(self):
        while self.running:
            b = self._queue.get()
            if b is self._end_of_stream:
                self._queue.task_done()
                return

            try:
                self._process.stdin.write(b)
            except BrokenPipeError:
                # Failed to write to subprocess stdin (broken pipe, stdin likely closed.
                pass
            except Exception:
                # Failed to write to subprocess stdin. Handle appropriately (but make
                # sure to consume the queue).
                pass
            finally:
                self._queue.task_done()

    def _thread_read(self, fp, out_name):
        setattr(self, out_name, fp.read())
        fp.close()
