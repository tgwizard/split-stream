import time
from itertools import tee

from werkzeug.formparser import FormDataParser, MultiPartParser
from flask import Flask, Response, request
from werkzeug.http import parse_options_header
from werkzeug.wsgi import get_content_length, get_input_stream


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
    return DummyWerkzeugFile()


class StreamingMultipartFormDataParser(MultiPartParser):
    def __init__(self, **kwargs):
        kwargs.setdefault('stream_factory', stream_factory)
        super().__init__(**kwargs)

        self._raw_form_data = []
        self._raw_file_data = []
        self._has_inspected_form = False

    def parse_from_environ(self, environ):
        content_type = environ.get('CONTENT_TYPE', '')
        content_length = get_content_length(environ)
        mimetype, options = parse_options_header(content_type)
        return self._parse_multipart(get_input_stream(environ), mimetype, content_length, options)

    def _parse_multipart(self, stream, mimetype, content_length, options):
        boundary = options.get('boundary')
        if boundary is None:
            raise ValueError('Missing boundary')
        if isinstance(boundary, str):
            boundary = boundary.encode('ascii')
        form, files = self.parse(stream, boundary, content_length)
        return stream, form, files

    def parse(self, file, boundary, content_length):
        self._raw_form_data = []
        self._raw_file_data = []

        for part_type, part in self.parse_parts(file, boundary, content_length):
            if part_type == 'form':
                self._raw_form_data.append(part)
            elif part_type == 'file':
                self._raw_file_data.append(part)

        return self.cls(self._raw_form_data), self.cls(self._raw_file_data)

    def start_file_streaming(self, *args, **kwargs):
        if not self._has_inspected_form:
            self._inspect_form()

        return super().start_file_streaming(*args, **kwargs)

    def _inspect_form(self):
        # Here we can inspect the non-file form data, before starting to read the file.
        form = self.cls(self._raw_form_data)
        if form.get('foo') != 'bar':
            raise ValueError('aborting, bad foo')

        self._has_inspected_form = True


def main():
    app = Flask('file-streamer')

    @app.route('/upload', methods=['POST'])
    def upload():
        print('Starting to read request')
        start = time.time()

        try:
            stream, form, files = StreamingMultipartFormDataParser().parse_from_environ(
                request.environ
            )
        except ValueError as e:
            print('Caught ValueError: %s' % e)
            return Response(status=400)

        end = time.time()
        print('Finished reading request: time=%s' % (end - start))
        print('Form: %s' % form)
        print('Files: %s' % files)

        return Response(status=200)

    app.run('0.0.0.0', 5001)


if __name__ == '__main__':
    main()
