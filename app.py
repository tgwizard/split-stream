import resource

import subprocess

import time
from io import BytesIO

import werkzeug.formparser
from flask import Flask, Response, request

from subprocess_input_streamer import SubprocessInputStreamer


def mb(x):
    return x / (1024 * 1024)


def get_memory_usage():
    return mb(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


class WerkzeugSubprocessStreamProxy:
    def __init__(self, input_streamer: SubprocessInputStreamer):
        self.input_streamer = input_streamer

    def write(self, b: bytes):
        if not self.input_streamer.running:
            self.input_streamer.start()

        self.input_streamer.write(b)

    def seek(self, *args, **kwargs):
        # Hack: this is how we know we've finished reading the request file.
        self.input_streamer.stop()


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

    @app.route('/foo', methods=['POST'])
    def foo():
        class DummyWerkzeugFile:
            def write(self, b: bytes):
                print('reading file parts: size=%s' % len(b))

            def seek(self, *args, **kwargs):
                # Hack: this is how we know we've finished reading the request file.
                return 0

        def stream_factory(total_content_length, content_type, filename, content_length=None):
            return DummyWerkzeugFile()

        print('Starting to read request')
        start = time.time()

        stream, form, files = werkzeug.formparser.parse_form_data(
            request.environ, stream_factory=stream_factory
        )

        end = time.time()
        print('Finished reading request: time=%s' % (end - start))
        print('Form: %s' % form)
        print('Files: %s' % files)

        return Response(status=200)

    @app.route('/upload', methods=['POST'])
    def upload():
        temp_files = [
            # open('/Users/adam/Desktop/stream-split-a', mode='wb'),
            # open('/Users/adam/Desktop/stream-split-b', mode='wb'),
            # open('/Users/adam/Desktop/stream-split-c', mode='wb'),
        ]
        spx = SubprocessInputStreamer(
            ['exiftool', '-'], popen_kwargs={'stdin': subprocess.PIPE, 'stdout': subprocess.PIPE}
        )
        factory = SplitStreamWriterFactory(temp_files + [WerkzeugSubprocessStreamProxy(spx)])
        print('Starting to read request')

        start = time.time()
        try:
            stream, form, files = werkzeug.formparser.parse_form_data(
                request.environ, stream_factory=factory
            )
        except Exception as e:
            print(e)
            return None
        else:
            print('no exception')

        end = time.time()
        print('Finished reading request: time=%s' % (end - start))
        print('Form: %s' % form)
        print('Files: %s' % files)

        print('Temp files: %s' % [f.name for f in temp_files])

        if spx.error:
            print('SPX ERROR: error=%s, spx.stderr=%s' % (spx.error, spx.stderr))
        else:
            print('SPX: ')
            print(spx.stdout.decode() if spx.stdout else '<NO STDOUT>')
            print(spx.stderr.decode() if spx.stderr else '<NO STDERR>')

        return Response(status=200)

    app.run('0.0.0.0', 5000)


if __name__ == '__main__':
    main()
