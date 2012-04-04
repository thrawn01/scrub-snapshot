#! /usr/bin/env python

import mmap
import sys
import time
import os

def buffered(data):
    total = 0
    with open(sys.argv[1], 'a') as file:
        os.lseek(file.fileno(), 0, os.SEEK_SET)
        for i in xrange(0, count):
            total = total + os.write(file.fileno(), data)
        file.flush()
    return total


def direct(data):
    # system call write() when used with O_DIRECT requires 
    # the buffer passed to be 512 byte aligned in length
    # Attempting to write 'data' string directly will result in 
    # OSError: [Errno 22] Invalid argument

    # Len of 'data' should be 4k, which is 512 aligned
    # passing -1 to mmap maps the object to a memory 
    # location instead of a file
    mem = mmap.mmap(-1, len(data))
    # Write the data into our reserved memory buffer
    mem.write(data)

    # Open the file for direct access
    fd = os.open(sys.argv[1], os.O_DIRECT | os.O_RDWR | os.O_SYNC)
    # Seek to the begining of the file (or block device)
    os.lseek(fd, 0, os.SEEK_SET)

    total = 0
    for i in xrange(0, count):
        total = total + os.write(fd, mem)

    # Close the file
    os.close(fd)
    return total


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage %s <file> <char> <count> [direct]" % sys.argv[0]
        print "Writes 4096 character chunks to a file"
        sys.exit(1)

    value = sys.argv[2]
    count = int(sys.argv[3])
    data = "".join([chr(int(value)) for i in xrange(0, 4096)])

    print "Writing: '%c'" % data[0]
    if len(sys.argv) == 5 and sys.argv[4] == 'direct':
        print "(Direct) Wrote: ", direct(data)
        sys.exit(0)

    print "(Buffered) Wrote: ", buffered(data)
    sys.exit(0)


