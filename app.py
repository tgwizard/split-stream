import resource

import subprocess
import queue

import time
from io import BytesIO
from threading import Thread

import werkzeug.formparser
from flask import Flask, Response, request


def mb(x):
    return x / (1024 * 1024)


def get_memory_usage():
    return mb(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


class SubprocessStreamFile:
    _end_of_stream = object()

    def __init__(self, process_args):
        self.process_args = process_args
        self._running = False

    def _start(self):
        assert not self._running

        self.stdout = None
        self.stderr = None
        self.error = None

        self._process = subprocess.Popen(
            self.process_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        self._queue = queue.Queue()
        self._running = True

        self._stdin_thread = Thread(target=self._thread_write)
        self._stdin_thread.daemon = True
        self._stdin_thread.start()

        self._stdout_buffer = []
        self._stdout_thread = Thread(
            target=self._thread_read, args=(self._process.stdout, self._stdout_buffer)
        )
        self._stdout_thread.daemon = True
        self._stdout_thread.start()

        self._stderr_buffer = []
        self._stderr_thread = Thread(
            target=self._thread_read, args=(self._process.stderr, self._stderr_buffer)
        )
        self._stderr_thread.daemon = True
        self._stderr_thread.start()

    def _stop(self):
        if not self._running:
            return

        self._queue.put(self._end_of_stream)
        self._queue.join()
        self._process.stdin.flush()
        self._process.stdin.close()

        self._running = False

        try:
            self._stdin_thread.join(timeout=10)
            self._stdout_thread.join(timeout=2)
            self._stderr_thread.join(timeout=2)
        except subprocess.TimeoutExpired as e:
            print(e)
            self.error = e
            self._process.kill()
        finally:
            self._process = None
            self._queue = None

        if self._stdout_buffer:
            self.stdout = self._stdout_buffer[0]
        if self._stderr_buffer:
            self.stderr = self._stderr_buffer[0]

        self._stdout_buffer = None
        self._stderr_buffer = None

        assert not self._stdin_thread.is_alive(), 'Stdin thread is alive!'
        assert not self._stdout_thread.is_alive(), 'Stdout thread is alive!'
        assert not self._stderr_thread.is_alive(), 'Stderr thread is alive!'

    def write(self, b):
        if not self._running:
            self._start()

        self._queue.put(b)

    def _thread_write(self):
        while self._running:
            b = self._queue.get()
            if b is self._end_of_stream:
                self._queue.task_done()
                return
            self._process.stdin.write(b)
            self._queue.task_done()

    def _thread_read(self, fp, buffer):
        buffer.append(fp.read())
        fp.close()

    def seek(self, *args, **kwargs):
        # Hack...
        self._stop()


class SplitStreamWriter:
    def __init__(self, output_streams, buffer_size=10 * 1024 * 1024):
        super().__init__()

        self.output_streams = output_streams
        self.total_size = 0
        self.write_counts = 0

        self.buffer_size = buffer_size
        self.buffer = BytesIO()

    def _flush_buffer(self):
        for stream in self.output_streams:
            stream.write(self.buffer.getbuffer())
        self.buffer = BytesIO()

    def write(self, b):
        self.buffer.write(b)
        if self.buffer.tell() > self.buffer_size:
            self._flush_buffer()

        size = len(b)
        self.total_size += size
        self.write_counts += 1

        if self.write_counts % 50000 == 0:
            print(
                'Writing chunk %s, total size read: %sMB' % (
                    self.write_counts, mb(self.total_size)
                )
            )
            print('WHILE READING: %sMB' % get_memory_usage())
        return size

    def seek(self, *args, **kwargs):
        self._flush_buffer()

        for stream in self.output_streams:
            stream.seek(*args, **kwargs)
        return 0


class SplitStreamWriterFactory:
    def __init__(self, output_streams):
        self.writer = SplitStreamWriter(output_streams)

    def __call__(self, total_content_length, content_type, filename, content_length=None):
        print(
            'Start writing to streams: '
            'total_content_length=%s, content_type=%s, filename=%s, content_length=%s' % (
                total_content_length, content_type, filename, content_length
            )
        )
        return self.writer


def main():
    app = Flask('stream-split')

    @app.before_request
    def before_request(*args, **kwargs):
        print('BEFORE REQ: %sMB' % get_memory_usage())

    @app.teardown_request
    def after_request(*args, **kwargs):
        print('AFTER REQ: %sMB' % get_memory_usage())

    @app.route('/consume', methods=['POST'])
    def consume():
        x = list({} for _ in range(20000000))
        return Response(status=200)

    @app.route('/upload', methods=['POST'])
    def upload():
        temp_files = [
            # open('/Users/adam/Desktop/stream-split-a', mode='wb'),
            # open('/Users/adam/Desktop/stream-split-b', mode='wb'),
            # open('/Users/adam/Desktop/stream-split-c', mode='wb'),
        ]
        spx = SubprocessStreamFile(['exiftool', '-'])
        factory = SplitStreamWriterFactory(temp_files + [spx])
        print('Starting to read request')

        start = time.time()
        try:
            stream, form, files = werkzeug.formparser.parse_form_data(
                request.environ, stream_factory=factory
            )
        except Exception as e:
            print(e)
            return None

        end = time.time()
        print('Finished reading request: time=%s' % (end - start))
        print('Form: %s' % form)
        print('Files: %s' % files)

        print('Temp files: %s' % [f.name for f in temp_files])

        if spx.error:
            print('SPX ERROR: error=%s, spx.stderr=%s' % (spx.error, spx.stderr))
        else:
            print('SPX: ')
            print(spx.stdout.decode() if spx.stdout else '<NOTHING>')

        return Response(status=200)

    app.run('0.0.0.0', 5000)


if __name__ == '__main__':
    main()
