#! /usr/bin/python 

from ctypes import cdll, util, c_int, c_void_p, c_size_t, \
        c_uint64, POINTER, byref, get_errno, CDLL, string_at
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
        self._pread = libc.pread
        self._pread.argtypes = [c_int, c_void_p, c_size_t, c_uint64]
        self._pread.errcheck = self.error_check
        self._pwrite = libc.pwrite
        self._pwrite.argtypes = [c_int, c_void_p, c_size_t, c_uint64]
        self._pwrite.errcheck = self.error_check

    @staticmethod
    def error_check(result, func, args):
        if result < 0:
            errno = get_errno()
            raise OSError(errno, os.strerror(errno))

    def write(self, buf):
        if len(buf) == self.block_size:
            return _write(self.fd, buf, self.block_size)

        offset, total = 0, 0
        write_count, remainder = 0, len(buf)
        # the buf is greater than the block size
        if len(buf) > self.block_size:
            write_count, remainder = divmod(len(buf), self.block_size)

        # Write the buf 1 block at a time
        for i in xrange(0, write_count):
            offset = i * self.block_size
            total = _write(self.fd, buf[offset:offset + self.block_size], self.block_size)

        # Read in the a block
        read_len = _read(self.fd, read_buf, self.block_size)
        # Overlay the bytes
        read_buf.splice(0, buf[offset:remainder], remainder)
        # Seek back to the position we started the read from
        _lseek(self.fd, read_len * -1, os.SEEK_CUR)
        # Write out the read_buf
        total += _write(self.fd, read_buf, self.block_size)


    def read(self, length=None):
        # If length is -1 or None, call readall()
        if length == -1 or length == None:
            return self.readall()

        if length == self.block_size:
            _read(self.fd, read_buf, self.block_size)
            return read_buf

        read_count, remainder = 0, length
        # the read length is greater than the block size
        if length > self.block_size:
            read_count, remainder = divmod(length, self.block_size)

        # read 1 block at a time
        for i in xrange(0, read_count):
            total = _read(self.fd, buf, self.block_size)
            result.append(buf)

        # Read in an entire block
        read_len = _read(self.fd, buf, self.block_size)
        # Grab only the remainder of the requested read
        result.append(buf[0:remainder])

        return join(result)

        #buf = c_void_p()
        #self._memalign(byref(buf), self.block_size, length)
        #self._pread(self.fd, buf, length, offset)
        #return string_at(buf, length)


    def close(self):
        return os.close(self.fd)


if __name__ == "__main__":
    file = DirectFile("/dev/volume/original")

    for i in xrange(0, count):
        input = file.pread(4096, 0)
        total = total + os.write(sys.stdout.fileno(), input)

