#! /usr/bin/env python

from directio import RawDirect
import unittest
import tempfile
import os


class TestRawDirect(unittest.TestCase):

    def setUp(self):
        fd, self.file = tempfile.mkstemp(dir='/tmp')
        os.write(fd, '\0' * 1048576)
        os.close(fd)

    def tearDown(self):
        os.unlink(self.file)

    def test_direct_write(self):
        raw = RawDirect(self.file, block_size=512)
        raw.write('A' * 512)
        raw.close()

        with open(self.file) as fd:
            self.assertEquals(fd.read(512), 'A' * 512)

    def test_direct_read(self):
        with open(self.file, 'w') as file:
            file.write('B' * 512)

        raw = RawDirect(self.file, block_size=512)
        self.assertEquals(raw.read(512), 'B' * 512)
        raw.close()

    def test_direct_read_past_eof(self):
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


