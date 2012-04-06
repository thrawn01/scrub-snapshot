#! /usr/bin/env python

from directio import RawDirect
from subprocess import call
import unittest
import tempfile
import os

def find_loopback_device():
    for i in range(0, 7):
        if call("losetup /dev/loop%d 2> /dev/null" % i, shell=True):
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
            file.write('G' * 512)

        raw = RawDirect(self.file, block_size=512)
        # Ask to only read 10 bytes
        self.assertEquals(raw.read(10), 'G' * 10)
        raw.close()

    def test_read_greater_than_block_size(self):
        with open(self.file, 'w') as file:
            file.write('J' * 520)

        raw = RawDirect(self.file, block_size=512)
        # Ask to read 8 more bytes then the block size
        self.assertEquals(raw.read(520), 'J' * 520)
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
        raw = RawDirect(self.file, block_size=512)
        # Write only 10 bytes
        self.assertEquals(raw.write('G' * 10), 10)
        raw.close()

        with open(self.file) as fd:
            # Should contain the 10 G's
            # the rest of the file should be NULLS
            self.assertEquals(fd.read(12), ('G' * 10) + '\0\0')

    def test_write_greater_than_block_size(self):
        raw = RawDirect(self.file, block_size=512)
        # Write only 8 bytes more than the block size
        self.assertEquals(raw.write('J' * 520), 520)
        raw.close()

        with open(self.file) as fd:
            # Should contain the 10 G's
            # the rest of the file should be NULLS
            self.assertEquals(fd.read(522), ('J' * 520) + '\0\0')
