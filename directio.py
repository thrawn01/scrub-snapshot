#! /usr/bin/env python 

from ctypes import cdll, util, c_int, c_void_p, c_size_t, \
        c_uint64, byref, get_errno, CDLL, string_at, memmove, c_char_p
import os
from io import RawIOBase


libc = CDLL(util.find_library('c'), use_errno=True)


class RawDirect(RawIOBase):

    def __init__(self, path, block_size=4096):
        self.fd = os.open(path, os.O_DIRECT | os.O_RDWR)
        self.block_size = block_size

        # Tell python about our libc calls
        self._memalign = libc.posix_memalign
        self._memalign.argtypes = [c_void_p, c_size_t, c_size_t]
        self._memalign.errcheck = self.error_check
        self._cread = libc.read
        self._cread.argtypes = [c_int, c_void_p, c_size_t]
        self._cread.errcheck = self.error_check
        self._cwrite = libc.write
        self._cwrite.argtypes = [c_int, c_void_p, c_size_t]
        self._cwrite.errcheck = self.error_check

        self.buf = c_void_p()
        self._memalign(byref(self.buf), self.block_size, self.block_size)

    @staticmethod
    def error_check(result, func, args):
        if result < 0:
            errno = get_errno()
            raise OSError(errno, os.strerror(errno))

    def _write(self, buf):
        memmove(self.buf, c_char_p(buf), self.block_size)
        return self._cwrite(self.fd, self.buf, self.block_size)

    def write(self, buf):
        if len(buf) == self.block_size:
            return self._write(buf)
        raise RuntimeError("Not Implemented Yet, Wrong length")

        offset, total = 0, 0
        write_count, remainder = 0, len(buf)
        # the buf is greater than the block size
        if len(buf) > self.block_size:
            write_count, remainder = divmod(len(buf), self.block_size)

        # Write the buf 1 block at a time
        for i in xrange(0, write_count):
            offset = i * self.block_size
            total = self._write(buf[offset:offset + self.block_size])

        # Read in the a block
        read_len = self._read(self.fd, read_buf, self.block_size)
        # Overlay the bytes
        read_buf.splice(0, buf[offset:remainder], remainder)
        # Seek back to the position we started the read from
        _lseek(self.fd, read_len * -1, os.SEEK_CUR)
        # Write out the read_buf
        total += self._write(read_buf)

    def _read(self):
        self._cread(self.fd, self.buf, self.block_size)
        return string_at(self.buf, self.block_size)

    def read(self, length=None):
        # If length is -1 or None, call readall()
        #if length == -1 or length == None:
            #return self.readall()

        if length == self.block_size:
            return self._read()
        raise RuntimeError("Not Implemented Yet, Wrong length")

        read_count, remainder = 0, length
        # the read length is greater than the block size
        if length > self.block_size:
            read_count, remainder = divmod(length, self.block_size)

        # read 1 block at a time
        for i in xrange(0, read_count):
            buf = self._read()
            result.append(buf)
            total += len(buf)

        # Read in an entire block
        buf = self._read()
        # Grab only the remainder of the requested read
        result.append(buf[0:remainder])

        return result.join()

    def close(self):
        # TODO: Free the memalign buffer (self.buf)
        return os.close(self.fd)


if __name__ == "__main__":
    file = DirectFile("/dev/volume/original")

    for i in xrange(0, count):
        input = file.pread(4096, 0)
        total = total + os.write(sys.stdout.fileno(), input)

