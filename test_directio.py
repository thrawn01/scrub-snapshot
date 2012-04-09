#! /usr/bin/env python

from io import UnsupportedOperation
from directio import RawDirect
import directio
from subprocess import call
import unittest
import tempfile
import os


def find_loopback_device():
    for i in range(0, 7):
        if call("losetup /dev/loop%d 2> /dev/null > /dev/null"
                % i, shell=True):
            return '/dev/loop%d' % i


def cannot_create_loopback():
    device = find_loopback_device()
    if not device:
        return True

    stats = os.stat(device)
    if stats.st_gid in os.getgroups():
        return False
    if stats.st_uid == os.getuid():
        return False
    return True


class TestRawDirect(unittest.TestCase):

    def setUp(self):
        fd, self.file = tempfile.mkstemp(dir='/tmp')
        os.write(fd, '\0' * 1048576)
        os.close(fd)

    def tearDown(self):
        os.unlink(self.file)

    def test_read(self):
        with open(self.file, 'w') as file:
            file.write('B' * 512)

        raw = RawDirect(self.file, block_size=512)
        self.assertEquals(raw.read(512), 'B' * 512)
        raw.close()

    def test_read_past_eof(self):
        # Create a file that is less than the block size
        fd, file = tempfile.mkstemp(dir='/tmp')
        os.write(fd, 'A' * 511)
        os.close(fd)

        raw = RawDirect(file, block_size=512)
        # If requested read is past the EOF
        # return the bytes we actually read
        self.assertEquals(raw.read(512), ('A' * 511))
        # Empty string means we are at EOF
        self.assertEquals(raw.read(512), '')
        raw.close()

    def test_read_less_than_block_size(self):
        with open(self.file, 'w') as file:
            file.write('F' * 512)
            file.write('B' * 1024)

        raw = RawDirect(self.file, block_size=4096)
        # Ask to only read 10 bytes
        self.assertRaises(OSError, raw.read, 10)
        # Read 512 bytes
        self.assertEquals(raw.read(512), 'F' * 512)
        # Read 1024
        self.assertEquals(raw.read(1024), 'B' * 1024)
        raw.close()

    def test_read_greater_than_block_size(self):
        with open(self.file, 'w') as file:
            file.write('J' * 520)

        raw = RawDirect(self.file, block_size=512)
        # Ask to read 8 more bytes then the block size
        self.assertRaises(OSError, raw.read, 520)
        raw.close()

    def test_readall(self):
        raw = RawDirect(self.file, block_size=512)
        # Read in the entire 1MB file
        buf = raw.readall()
        self.assertEquals(len(buf), 1048576)
        raw.close()

    def test_readinto(self):
        with open(self.file, 'w') as file:
            file.write('A' * 512)

        raw = RawDirect(self.file, block_size=512)
        buf = bytearray(512)
        self.assertEquals(raw.readinto(buf), 512)
        self.assertEquals(buf, 'A' * 512)
        raw.close()

    def test_write(self):
        raw = RawDirect(self.file, block_size=512)
        raw.write('A' * 512)
        raw.close()

        with open(self.file) as fd:
            self.assertEquals(fd.read(512), 'A' * 512)

    @unittest.skipIf(cannot_create_loopback(), "requires a loopback device")
    def test_write_past_device_eof(self):
        # Create a sparse file that is less than the block size
        fd, file = tempfile.mkstemp(dir='/tmp')
        os.ftruncate(fd, 511)
        os.close(fd)

        try:
            # Mount the file as a loopback device (simulate a block device)
            device = find_loopback_device()
            call("losetup %s %s" % (device, file), shell=True)

            raw = RawDirect(device, block_size=512)
            # Should raise IOError: [Errno 28] No space left on device
            self.assertRaises(OSError, raw.write, 'A' * 512)
            self.assertEquals(raw.tell(), 0)
            self.assertEquals(os.lseek(raw._fd, 0, os.SEEK_CUR), 0)
            raw.close()
        finally:
            call("losetup -d %s" % device, shell=True)

    def test_write_past_eof(self):
        # Create a sparse file that is less than the block size
        fd, file = tempfile.mkstemp(dir='/tmp')
        os.ftruncate(fd, 511)
        os.close(fd)
        # Should be 511 in size
        self.assertEquals(os.stat(file).st_size, 511)

        raw = RawDirect(file, block_size=512)
        # the write should succeed
        self.assertEquals(raw.write('A' * 512), 512)
        raw.close()

        # writting past the end should grow the size of the file
        self.assertEquals(os.stat(file).st_size, 512)

    def test_write_less_than_block_size(self):
        raw = RawDirect(self.file, block_size=4096)
        # Write only 10 bytes
        self.assertRaises(OSError, raw.write, ('G' * 10))
        # Write 512
        self.assertEquals(raw.write('A' * 512), 512)
        # Write 1024
        self.assertEquals(raw.write('B' * 1024), 1024)
        # Ensure the data is there
        raw.seek(0)
        self.assertEquals(raw.read(512), 'A' * 512)
        self.assertEquals(raw.read(1024), 'B' * 1024)
        raw.close()

    def test_write_greater_than_block_size(self):
        raw = RawDirect(self.file, block_size=512)
        # Write only 8 bytes more than the block size
        self.assertRaises(OSError, raw.write, ('J' * 520))
        raw.close()

    def test_closed(self):
        raw = RawDirect(self.file, block_size=512)
        self.assertEquals(raw.closed, False)
        self.assertEquals(raw.writable(), True)
        raw.close()
        self.assertEquals(raw.closed, True)
        self.assertEquals(raw.writable(), False)

    def test_truncate(self):
        # Our test file should be 1MB
        self.assertEquals(os.stat(self.file).st_size, 1048576)

        raw = RawDirect(self.file, block_size=512)
        raw.truncate(512)
        raw.close()
        self.assertEquals(os.stat(self.file).st_size, 512)

    def test_seek(self):
        raw = RawDirect(self.file, block_size=512)
        self.assertEquals(raw.seek(4096, os.SEEK_SET), 4096)
        self.assertEquals(raw.seek(-512, os.SEEK_CUR), 3584)
        # TODO: Test SEEK_END

    def test_tell(self):
        raw = RawDirect(self.file, block_size=512)
        raw.write('A' * 512)
        self.assertEquals(raw.tell(), 512)
        raw.write('B' * 512)
        self.assertEquals(raw.tell(), 1024)
        raw.seek(0)
        self.assertEquals(raw.read(512), ('A' * 512))
        self.assertEquals(raw.read(512), ('B' * 512))
        self.assertEquals(raw.tell(), 1024)


class TestBufferedDirect(unittest.TestCase):

    def setUp(self):
        fd, self.file = tempfile.mkstemp(dir='/tmp')
        os.write(fd, '\0' * 1048576)
        os.close(fd)

    def tearDown(self):
        os.unlink(self.file)

    def test_open_with(self):
        # BufferedIOBase gives us a context manager for free!
        with directio.open(self.file) as fd:
            self.assertEquals(fd.write('A' * 512), 512)
            fd.seek(0)
            self.assertEquals(fd.read(512), 'A' * 512)

    def test_open_read_only(self):
        fd = directio.open(self.file, 'r')
        self.assertRaises(UnsupportedOperation, fd.write, ('\0' * 512))
        fd.seek(0)
        self.assertEquals(fd.read(512), '\0' * 512)
        fd.close()

    def test_open_write_only(self):
        fd = directio.open(self.file, 'w')
        self.assertEquals(fd.write('A' * 512), 512)
        fd.seek(0)
        self.assertRaises(UnsupportedOperation, fd.read, 512)
        fd.close()

    def test_buffered_write(self):
        fd = directio.open(self.file)
        fd.write('A' * 512)
        fd.write('B' * 512)
        fd.write('C' * 512)
        fd.write('D' * 512)
        fd.write('E' * 512)
        fd.seek(-512, os.SEEK_CUR)
        self.assertEquals(fd.read(512), 'E' * 512)
        fd.close()

        fd = directio.open(self.file)
        self.assertEquals(fd.read(512), 'A' * 512)
        fd.seek(2048)
        self.assertEquals(fd.read(512), 'E' * 512)
        fd.close()
