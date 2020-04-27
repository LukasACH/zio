import zlib
from io import RawIOBase
from typing import BinaryIO


class CompressedStreamWriter(RawIOBase):
    def writable(self) -> bool:
        return True

    def __init__(self, stream: BinaryIO):
        self._stream = stream

    def __enter__(self):
        self._compressor = zlib.compressobj()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stream.write(self._compressor.flush())
        del self._compressor

    def write(self, data):
        self._stream.write(self._compressor.compress(data))


class CompressedFileWriter(CompressedStreamWriter):
    def __init__(self, filename):
        super().__init__(open(filename, 'bw'))

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self._stream.close()
