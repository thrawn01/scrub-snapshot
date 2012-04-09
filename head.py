#! /usr/bin/env python

import sys
import time
import os
import mmap
import directio


def buffered(offset, count):
    total = 0
    with open(sys.argv[1], 'r') as fd:
        os.lseek(fd.fileno(), offset, os.SEEK_SET)
        for i in xrange(0, count):
            total = total + os.write(sys.stdout.fileno(),
                    os.read(fd.fileno(), 4096))
            sys.stdout.flush()
    return total


def direct_io(offset, count):
    total = 0
    with directio.open(sys.argv[1], buffered=4096) as fd:
        fd.seek(offset)
        for i in xrange(0, count):
            total = total + os.write(sys.stdout.fileno(), fd.read(4096))
    return total
    #for i in xrange(0, count):
    #    input = file.pread(4096, offset)
    #    total = total + os.write(sys.stdout.fileno(), input)
#
#    # Close the file
#    file.close()
#    return total


def direct_map(offset, count):
    # Open the file for direct access
    fd = os.open(sys.argv[1], os.O_DIRECT | os.O_RDWR)

    cur_pos = offset
    total = 0
    for i in xrange(0, count):
        mem = mmap.mmap(fd, 4096, offset=cur_pos)
        input = mem[offset:4096]
        total = total + os.write(sys.stdout.fileno(), input)
        cur_pos = cur_pos + 4096

    # Close the file
    os.close(fd)
    return total


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage %s <file> <offset> <count>" % sys.argv[0]
        print "Read sectors from the file to stdout"
        sys.exit(1)

    offset = int(sys.argv[2]) * 4096
    count = int(sys.argv[3])
    print "Offset: ", offset

    if len(sys.argv) == 5 and sys.argv[4] == 'direct_map':
        os.write(sys.stdout.fileno(), "\n(Direct Map) Read: %d\n"
                % direct_map(offset, count))
        sys.exit(0)

    if len(sys.argv) == 5 and sys.argv[4] == 'direct_io':
        os.write(sys.stdout.fileno(), "\n(Direct) Read: %d\n"
                % direct_io(offset, count))
        sys.exit(0)

    os.write(sys.stdout.fileno(), "\n(Buffered) Read: %d\n"
                % buffered(offset, count))
    sys.exit(0)
