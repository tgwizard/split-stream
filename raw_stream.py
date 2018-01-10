import subprocess
import time

from werkzeug.wsgi import get_content_length
from flask import Flask, Response, request

from subprocess_input_streamer import SubprocessInputStreamer


class SubprocessStreamProxy:
    def __init__(self, input_streamer: SubprocessInputStreamer):
        self.input_streamer = input_streamer

    def write(self, b: bytes):
        if not self.input_streamer.running:
            self.input_streamer.start()

        self.input_streamer.write(b)

    def flush(self):
        self.input_streamer.stop()


class SplitStreamWriter:
    def __init__(self, output_streams):
        self.output_streams = output_streams

    def write(self, b):
        for stream in self.output_streams:
            stream.write(b)
        return len(b)

    def flush(self):
        for stream in self.output_streams:
            stream.flush()


def main():
    app = Flask('stream-split')

    @app.route('/upload', methods=['POST'])
    def upload():
        temp_files = [
            open('./data/stream-split-a', mode='wb'),
            open('./data/stream-split-b', mode='wb'),
            open('./data/stream-split-c', mode='wb'),
        ]
        spx = SubprocessInputStreamer(
            ['exiftool', '-'], popen_kwargs={'stdin': subprocess.PIPE, 'stdout': subprocess.PIPE}
        )
        writer = SplitStreamWriter(temp_files + [SubprocessStreamProxy(spx)])

        content_length = get_content_length(request.environ)
        print('Starting to read request: content_length=%s' % content_length)
        start = time.time()

        stream = request.stream
        while True:
            data = stream.read(500 * 1024)
            if not data:
                break
            writer.write(data)
        writer.flush()

        end = time.time()
        print('Finished reading request: time=%s' % (end - start))

        print('Temp files: %s' % [f.name for f in temp_files])
        for f in temp_files:
            f.close()

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
