#! /usr/bin/env python

import sys
import time


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage %s <file> <char> <count>" % sys.argv[0]
        print "Writes 4096 character chunks to a file"
        sys.exit(1)

    value = sys.argv[2]
    count = int(sys.argv[3])
    data = "".join([chr(int(value)) for i in xrange(0, 4096)])

    print "Writing: %s" % data
    print "Len: ", len(data)
    print "Count: ", count

    with open(sys.argv[1], 'a') as file:
        for i in xrange(0, count):
            if file.write(data) == 0:
                #sys.stdout.write('E')
                #sys.stdout.flush()
                print "Error Writing block %i" % i
                break
            file.flush()
            #sys.stdout.write('.')
            #sys.stdout.flush()
            #time.sleep(1)

    print "\n"

