#! /usr/bin/env python

import sys
import time
import os

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage %s <file> <offset> <count>" % sys.argv[0]
        print "Reads bytes from the file to stdout"
        sys.exit(1)

    offset = int(sys.argv[2])
    count = int(sys.argv[3])

    print "Offset: ", offset
    print "Count: ", count

    with open(sys.argv[1], 'r') as fd:
        os.lseek(fd.fileno(), offset, os.SEEK_SET)
        for i in xrange(0, count):
            os.write(sys.stdout.fileno(), os.read(fd.fileno(), 1))
            sys.stdout.flush()
            #os.fsync(sys.stdout)

