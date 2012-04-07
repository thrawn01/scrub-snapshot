#! /usr/bin/env python 

from ctypes import cdll, util, c_int, c_void_p, c_size_t, \
        c_uint64, byref, get_errno, CDLL, string_at, memmove, c_char_p
import os
from io import RawIOBase


libc = CDLL(util.find_library('c'), use_errno=True)


class RawDirect(RawIOBase):

    def __init__(self, path, block_size=4096):
        self._fd = os.open(path, os.O_DIRECT | os.O_RDWR)
        self._block_size = block_size
        self._closed = False

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

        # NOT thread safe, DO NOT USE RawDirect as a singleton!
        # IE: Don't replace sys.stdout with it!
        self._buf = c_void_p()
        self._memalign(byref(self._buf), self._block_size, self._block_size)

    def get_closed(self):
        return self._closed

    closed = property(get_closed, None, None,
            "Returns True if the file handle is closed")

    @staticmethod
    def error_check(result, func, args):
        if result < 0:
            errno = get_errno()
            raise OSError(errno, os.strerror(errno))
        return result

    def _write(self, buf):
        memmove(self._buf, c_char_p(buf), self._block_size)
        return self._cwrite(self._fd, self._buf, self._block_size)

    def write(self, buf):
        if len(buf) == self._block_size:
            return self._write(buf)

        offset, total = 0, 0
        write_count, remainder = 0, len(buf)

        # the buf is greater than the block size
        if len(buf) > self._block_size:
            write_count, remainder = divmod(len(buf), self._block_size)

        # Write the buf 1 block at a time
        for i in xrange(0, write_count):
            offset = i * self._block_size
            total = self._write(buf[offset:offset + self._block_size])

        # Read in the a block
        read_buf = self._read()
        # Overlay the bytes to be written in the block
        read_buf = buf[offset:remainder] + read_buf[remainder:]
        assert(len(read_buf) == self._block_size)
        # Seek back to the position we started the read from
        os.lseek(self._fd, self._block_size * -1, os.SEEK_CUR)
        # Write out the read_buf
        self._write(read_buf)
        return total + remainder

    def _read(self):
        length = self._cread(self._fd, self._buf, self._block_size)
        return string_at(self._buf, length)

    def read(self, length=None):
        # If length is -1 or None, call self.readall()
        if length == -1 or length == None:
            return self.readall()

        if length == self._block_size:
            return self._read()

        read_count, remainder, result = 0, length, []
        # the read length is greater than the block size
        if length > self._block_size:
            read_count, remainder = divmod(length, self._block_size)

        # read 1 block at a time
        for i in xrange(0, read_count):
            buf = self._read()
            result.append(buf)

        # Read in an entire block
        buf = self._read()
        # Grab only the remainder of the requested read
        result.append(buf[0:remainder])

        return ''.join(result)

    def close(self):
        # TODO: Free the memalign buffer (self._buf)
        self._closed = True
        return os.close(self._fd)

    def readall(self):
        result = []
        while True:
            buf = self.read(self._block_size)
            if len(buf) == 0:
                break
            result.append(buf)
        return ''.join(result)

    def readinto(self, buf):
        length = self._cread(self._fd, self._buf, self._block_size)
        buf[0:len(buf)] = string_at(self._buf, length)
        return length

    def fileno(self):
        return self._fd

    def flush(self):
        pass

    def isatty(self):
        return False

    def readable(self):
        if not self._closed:
            return True
        return False

    def seekable(self):
        return True

    def tell(self):
        if not self._closed:
            return os.lseek(self._fd, 0, os.SEEK_CUR)
        return 0

    def truncate(self, size=None):
        return os.ftruncate(self._fd, size)


if __name__ == "__main__":
    file = DirectFile("/dev/volume/original")

    for i in xrange(0, count):
        input = file.pread(4096, 0)
        total = total + os.write(sys.stdout.fileno(), input)

