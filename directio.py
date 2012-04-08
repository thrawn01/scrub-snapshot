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

    def error_check(self, result, func, args):
        if result < 0:
            errno = get_errno()
            raise OSError(errno, os.strerror(errno))
        return result

    def write(self, buf):
        # If the buf is the size of the block
        if len(buf) == self._block_size:
            memmove(self._buf, c_char_p(buf), self._block_size)
            return self._cwrite(self._fd, self._buf, self._block_size)
        raise OSError(22, "Refusing to write buffer of length %d"\
            "; length must equal blocksize %d" % (len(buf), self._block_size))

    def read(self, length=None):
        # If length is -1 or None, call self.readall()
        if length == -1 or length == None:
            return self.readall()

        if length == self._block_size:
            length = self._cread(self._fd, self._buf, self._block_size)
            return string_at(self._buf, length)

        raise OSError(22, "Refusing to read buffer of length %d"\
            "; length must equal blocksize %d" % (length, self._block_size))

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
        if len(buf) != self._block_size:
            raise OSError(22, "Refusing to read buffer of length %d"\
                    "; length must equal blocksize %d" % (len(buf),
                        self._block_size))

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
        if self._closed:
            return 0
        return os.lseek(self._fd, 0, os.SEEK_CUR)

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET or whence == os.SEEK_CUR:
            blocks, remainder = divmod(offset, self._block_size)
            if remainder != 0:
                raise OSError(22, "Refusing to seek to %d; offset must "\
                    "be a multiple of blocksize %d" %
                        (offset, self._block_size))

        # TODO: Keep SEEK_END from resulting in a
        # offset that is not a multiple of our block size
        return os.lseek(self._fd, offset, whence)

    def truncate(self, size=None):
        return os.ftruncate(self._fd, size)
