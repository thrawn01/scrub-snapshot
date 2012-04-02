#! /usr/bin/env python

import os
import sys
from struct import unpack_from

def read(fd, offset, length):
    # Seek to the offset
    if os.lseek(fd, offset, os.SEEK_SET) == -1:
        raise RuntimeError("Unable to seek to offset '%d'" % offset)
    return os.read(fd, length)


def read_exceptions_from_store(fd, chunk_size, index):
    # exception = { uint64 old_chunk, uint64 new_chunkc }
    # if the size of each exception is 16 bytes, 
    # how many exceptions can fit in one chunk
    exceptions_per_chunk = chunk_size / 16
    print "Exceptions Per Chunk: %d" % exceptions_per_chunk
    # Offset where the exception store begins
    store_offset = 1 + ((exceptions_per_chunk + 1) * index);
    # seek to the begining of the exception store and read a full chunk
    store = read(fd, chunk_size * store_offset, chunk_size)
    print "Exception Store: ", store
    exception = 0
    while True:
       (old_chunk, new_chunk) = unpack_from('<Q<Q', store, exception * 16)
       # Yields the offset where the exception exists in the cow
       yield new_chunk * chunk_size


def read_header(fd):
    SECTOR_SHIFT = 9
    SNAPSHOT_DISK_MAGIC = 0x70416e53
    SNAPSHOT_DISK_VERSION = 1
    SNAPSHOT_VALID_FLAG = 1

    # Seek to the begining of the block device)
    os.lseek(fd, 0, os.SEEK_SET)
    # Read the cow metadata
    header = unpack_from("<I<I<I<I", os.read(fd, 16))

    if header[0] != SNAPSHOT_DISK_MAGIC:
        print "Invalid COW device '%s'; header magic doesn't match" % cow
        return 1

    if header[1] != SNAPSHOT_VALID_FLAG:
        print "Invalid COW device '%s'; valid flag not set '%d' got '%d'"
            % (cow, SNAPSHOT_VALID_FLAG, header[1])
        return 1

    if header[2] != SNAPSHOT_DISK_VERSION:
        print "Unknown metadata version '%s'; expected '%d' got '%d' "
            % (cow, SNAPSHOT_DISK_VERSION, header[2])
        return 1

    # Chunk size in 512 bytes ( 0 << SECTOR_SHIFT == 512 )
    header[3] = header[3] << SECTOR_SHIFT
    return header


def scrub(cow)
    # Open the block device 
    fd = os.open(cow, os.O_RDWR)
    # Read the meta data header
    header = read_header(fd)

    print "Magic: %X" % header[0]
    print "Valid: %d" % header[1]
    print "Version: %d" % header[2]
    print "Chunk Size: %d" % header[3]

    store = 0
    while True:
        # Iterate through all the exceptions
        for offset in read_exceptions_from_store(fd, header[3], store):
            # zero means we reached the last exception
            if offset == 0:
                return os.close(fd)
            # Write a chunk full of NULL's at 'offset'
            write(fd, offset, 0, length, header[3])
        # Seek the next store
        store = store + 1


def main(argv):
    return scrub(argv[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))

