import resource

import subprocess

import time
from io import IOBase, RawIOBase, BufferedRandom, BufferedWriter

import werkzeug.formparser
from flask import Flask, Response, request

from subprocess_input_streamer import SubprocessInputStreamer


def mb(x):
    return x / (1024 * 1024)


def get_memory_usage():
    return mb(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


class SubprocessStreamProxy:
    def __init__(self, input_streamer: SubprocessInputStreamer):
        self.input_streamer = input_streamer

    def write(self, b: bytes):
        print('writing!')
        if not self.input_streamer.running:
            self.input_streamer.start()

        self.input_streamer.write(b)

    def flush(self):
        self.input_streamer.stop()


class SplitStreamWriter(RawIOBase):
    def __init__(self, output_streams):
        super().__init__()
        self.output_streams = output_streams

    def writable(self):
        return True

    def write(self, b):
        for stream in self.output_streams:
            stream.write(b)
        return len(b)

    def flush(self):
        for stream in self.output_streams:
            stream.flush()


class WerkzeugStreamProxy:
    def __init__(self, raw):
        self.raw = raw

    def write(self, b: bytes):
        return self.raw.write(b)

    def seek(self, *args, **kwargs):
        # Hack: this is how Werkzeug tells us we've finished reading the request file.
        self.raw.flush()
        self.raw.close()
        return 0


class SplitStreamWriterFactory:
    def __init__(self, output_streams):
        self.writer = WerkzeugStreamProxy(
            BufferedWriter(SplitStreamWriter(output_streams), buffer_size=500 * 1024)
        )

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

    @app.route('/upload', methods=['POST'])
    def upload():
        try:
            temp_files = [
                open('/Users/adam/Desktop/stream-split-a', mode='wb'),
                # open('/Users/adam/Desktop/stream-split-b', mode='wb'),
                # open('/Users/adam/Desktop/stream-split-c', mode='wb'),
            ]
            spx = SubprocessInputStreamer(
                ['exiftool', '-'], popen_kwargs={'stdin': subprocess.PIPE, 'stdout': subprocess.PIPE}
            )
            factory = SplitStreamWriterFactory(temp_files + [SubprocessStreamProxy(spx)])
            print('Starting to read request')

            start = time.time()
            stream, form, files = werkzeug.formparser.parse_form_data(
                request.environ, stream_factory=factory
            )

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
        except Exception as e:
            print(e)
            return Response(status=500)

        return Response(status=200)

    app.run('0.0.0.0', 5000)


if __name__ == '__main__':
    main()
