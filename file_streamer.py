import time

import werkzeug.formparser
from flask import Flask, Response, request


class DummyWerkzeugFile:
    def write(self, b: bytes):
        print('reading file parts: size=%s' % len(b))

    def seek(self, *args, **kwargs):
        # Hack: this is how we know we've finished reading the request file.
        return 0


def stream_factory(total_content_length, content_type, filename, content_length=None):
    print(
        'Start writing to stream: '
        'total_content_length=%s, content_type=%s, filename=%s, content_length=%s' % (
            total_content_length, content_type, filename, content_length
        )
    )
    # Here we can return anything with a writes(b: bytes) and seek() method, like a file.
    return DummyWerkzeugFile()


def main():
    app = Flask('file-streamer')

    @app.route('/upload', methods=['POST'])
    def upload():
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

    app.run('0.0.0.0', 5001)


if __name__ == '__main__':
    main()
