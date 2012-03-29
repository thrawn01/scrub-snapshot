#! /usr/bin/env python

import sys
import time
import os


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage %s <file> <char> <count>" % sys.argv[0]
        print "Writes 4096 character chunks to a file"
        sys.exit(1)

    value = sys.argv[2]
    count = int(sys.argv[3])
    data = "".join([chr(int(value)) for i in xrange(0, 4096)])
    total = 0

    print "Writing: '%c'" % data[0]

    with open(sys.argv[1], 'a') as file:
        os.lseek(file.fileno(), 0, os.SEEK_SET)
        for i in xrange(0, count):
            total = total + os.write(file.fileno(), data)
            file.flush()

    print "Wrote: ", total

