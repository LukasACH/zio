import zlib
from unittest import TestCase
from io import BytesIO
from zlib import compress

from zio.compressed_reader import CompressedStreamReader


class TestCompressedStreamReader(TestCase):
    def test_questions(self):
        data = BytesIO(compress(b''))
        with CompressedStreamReader(data) as reader:
            self.assertTrue(reader.readable())
            self.assertFalse(reader.writable())
            self.assertFalse(reader.seekable())

    def test_empty_stream(self):
        data = BytesIO(b'')
        with CompressedStreamReader(data) as reader:
            reader.read()

    def test_empty_compressed_stream(self):
        data = BytesIO(compress(b''))
        with CompressedStreamReader(data) as reader:
            self.assertEqual(reader.read(), b'')

    def test_compressed_readall(self):
        data = BytesIO(compress(b'foo'))
        with CompressedStreamReader(data) as reader:
            self.assertEqual(reader.read(), b'foo')

    def test_compressed_readline(self):
        data = BytesIO(compress(b'foo\nbar\n'))
        with CompressedStreamReader(data) as reader:
            self.assertEqual(reader.readline(), b'foo\n')
            self.assertEqual(reader.readline(), b'bar\n')

    def test_uncompressed_tail(self):
        data = BytesIO(compress(b'foo')+b'bar')
        with CompressedStreamReader(data) as reader:
            self.assertEqual(b'foo', reader.read())
        self.assertEqual(b'bar', data.read())

    def test_small_buffer(self):
        data = BytesIO(compress(b'abcdefghijklmnopqrstuvwxyz'))
        with CompressedStreamReader(data, 1) as reader:
            self.assertEqual(b'abcd', reader.read(4))

    def test_broken_compression(self):
        data = BytesIO(b'1234567890')
        with self.assertRaises(zlib.error):
            with CompressedStreamReader(data, 4) as reader:
                self.assertEqual(b'', reader.read(6))
        self.assertEqual(0, data.tell())
