import zlib
from io import RawIOBase, SEEK_CUR
from typing import BinaryIO

BUFFER_SIZE = 8192


class CompressedStreamReader(RawIOBase):
    """
    The CompressedStreamReader can read a zlib compressed chunk
    """

    def readable(self):
        return True

    def __init__(self, stream: BinaryIO):
        self._stream = stream

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
        while True:
            if self._decompressor.eof:
                break
            read_size = size - len(return_data)
            if read_size == 0:
                break
            compressed_chunk = self._decompressor.unconsumed_tail or self._stream.read(BUFFER_SIZE)
            decompressed_part = self._decompressor.decompress(compressed_chunk, read_size)
            return_data += decompressed_part
        return return_data


class CompressedFileReader(CompressedStreamReader):
    """
    Like the CompressedStreamReader, but operates directly on a file instead of a binary data stream.
    """

    def __init__(self, filename):
        super().__init__(open(filename, 'br'))

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self._decompressor
        self._stream.close()
