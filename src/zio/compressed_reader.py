import zlib
from io import RawIOBase, SEEK_CUR
from typing import BinaryIO

BUFFER_SIZE = 8192


class CompressedStreamReader(RawIOBase):

    def readable(self):
        return True

    def __init__(self, stream: BinaryIO, buffer_size=BUFFER_SIZE):
        """
        The CompressedStreamReader can read a zlib compressed chunk from a binary input stream.

        :param stream: The raw binary input stream. It must be readable and seekable.
        :param buffer_size: The size of the chunks read form stream at a time. Defaults to {__name__}.BUFFER_SIZE
        """
        self._stream = stream
        self._buffer_size = buffer_size

    def __enter__(self):
        self._decompressor = zlib.decompressobj()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._decompressor.eof:
            self.readall()
        self._stream.seek(-len(self._decompressor.unused_data), SEEK_CUR)
        del self._decompressor

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return self.readall()
        if not size:
            return b''
        return_data = b''
        stream_position = self._stream.tell()
        while True:
            if self._decompressor.eof:
                break
            read_size = size - len(return_data)
            if read_size == 0:
                break
            compressed_chunk = self._decompressor.unconsumed_tail or self._stream.read(self._buffer_size)
            if compressed_chunk == b'':
                break
            try:
                decompressed_part = self._decompressor.decompress(compressed_chunk, read_size)
            except zlib.error as e:
                self._stream.seek(stream_position)
                raise zlib.error('Corrupted stream') from e
            return_data += decompressed_part
        return return_data


class CompressedFileReader(CompressedStreamReader):
    """
    Like the CompressedStreamReader, but operates directly on a file instead of a binary data stream.
    """

    def __init__(self, filename, buffer_size=BUFFER_SIZE):
        super().__init__(open(filename, 'br'), buffer_size=buffer_size)

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self._decompressor
        self._stream.close()
