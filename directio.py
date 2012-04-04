#! /usr/bin/python 

from ctypes import cdll, util, c_int, c_void_p, c_size_t, \
        c_uint64, POINTER, byref, get_errno, CDLL, string_at
import os


libc = CDLL(util.find_library('c'), use_errno=True)


class DirectFile(object):

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

    @staticmethod
    def posix_memalign(buf, size, length):
        self._memalign(byref(buf), self.block_size, count)

    def pwrite(self, buf, length, offset=0):
        pass
        #pwrite(self.fd, buf, count, offset)

    def pread(self, length, offset=0):
        buf = c_void_p()
        self._memalign(byref(buf), self.block_size, length)
        self._pread(self.fd, buf, length, offset)
        return string_at(buf, length)

    def close(self):
        return os.close(self.fd)


if __name__ == "__main__":
    file = DirectFile("/dev/volume/original")

    for i in xrange(0, count):
        input = file.pread(4096, 0)
        total = total + os.write(sys.stdout.fileno(), input)

