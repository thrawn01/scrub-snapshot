#! /usr/bin/env python

import os
import sys
from struct import unpack_from
from optparse import OptionParser


def read(fd, offset, length):
    # Seek to the offset
    if os.lseek(fd, offset, os.SEEK_SET) == -1:
        raise RuntimeError("Unable to seek to offset '%d'" % offset)
    return os.read(fd, length)


def read_exception_metadata(fd, chunk_size, index):
    # exception = { uint64 old_chunk, uint64 new_chunkc }
    # if the size of each exception metadata is 16 bytes,
    # how many exceptions can fit in one chunk
    exceptions_per_chunk = chunk_size / 16
    # Offset where the exception metadata store begins
    store_offset = 1 + ((exceptions_per_chunk + 1) * index)
    # seek to the begining of the exception metadata store
    # and read the entire store
    store = read(fd, chunk_size * store_offset, chunk_size)
    exception = 0
    while True:
        # Unpack 1 exception metatdata from the store
        (old_chunk, new_chunk) = unpack_from('<QQ', store, exception * 16)
        # Yields the offset where the exception exists in the cow linear device
        yield new_chunk * chunk_size
        # Increment to the next exception in the metatdata store
        exception = exception + 1


def read_header(fd):
    SECTOR_SHIFT = 9
    SNAPSHOT_DISK_MAGIC = 0x70416e53
    SNAPSHOT_DISK_VERSION = 1
    SNAPSHOT_VALID_FLAG = 1

    # Read the cow metadata
    header = unpack_from("<IIII", read(fd, 0, 16))

    if header[0] != SNAPSHOT_DISK_MAGIC:
        print "Invalid COW device '%s'; header magic doesn't match" % cow
        return 1

    if header[1] != SNAPSHOT_VALID_FLAG:
        print "Invalid COW device '%s'; valid flag not set '%d' got '%d'"\
            % (cow, SNAPSHOT_VALID_FLAG, header[1])
        return 1

    if header[2] != SNAPSHOT_DISK_VERSION:
        print "Unknown metadata version '%s'; expected '%d' got '%d' "\
            % (cow, SNAPSHOT_DISK_VERSION, header[2])
        return 1

    header = list(header)
    # Chunk size in 512 bytes ( 0 << SECTOR_SHIFT == 512 )
    header[3] = header[3] << SECTOR_SHIFT
    return header


def scrub(snapshot, verbose=False):
    if not os.path.exists(snapshot):
        raise RuntimeError("Snapshot '%s' does not exist")

    # Rebuild the path to find the snapshot cow
    path = snapshot.split('/')
    cow = "/".join(['', path[1], 'mapper', "%s-%s-cow" % (path[2], path[3])])

    # Open the cow block device
    try:
        fd = os.open(cow, os.O_RDWR)
    except OSError, e:
        raise RuntimeError("Failed to open cow '%s'" % e)

    # Read the meta data header
    header = read_header(fd)

    if verbose:
        print "Magic: %X" % header[0]
        print "Valid: %d" % header[1]
        print "Version: %d" % header[2]
        print "Chunk Size: %d" % header[3]

    store = 0
    while True:
        # Iterate through all the exceptions
        for offset in read_exception_metadata(fd, header[3], store):
            # zero means we reached the last exception
            if offset == 0:
                return os.close(fd)
            if verbose:
                print "--- Exception ---"
                print read(fd, offset, header[3])
                print "-----------------"
            # Write a chunk full of NULL's at 'offset'
            #write(fd, offset, 0, length, header[3])
        # Seek the next store
        store = store + 1


if __name__ == "__main__":
    description = "Scrub the COW of an lvm snapshot then delete the snapshot"
    parser = OptionParser(usage="Usage: %prog <snapshot path> [-h]",
            description=description)
    parser.add_option('-v', '--verbose', const=True, action='store_const',
            help="Be very verbose when scrubbing the cow")
    options, args = parser.parse_args()

    if not len(args):
        parser.print_help()
        sys.exit(1)

    sys.exit(scrub(args[0], options.verbose))
