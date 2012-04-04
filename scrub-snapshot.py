#! /usr/bin/env python

import os
import sys
from struct import unpack_from
from optparse import OptionParser
from subprocess import check_output, call, CalledProcessError


class ScrubError(RuntimeError):
    pass


def run(cmd, verbose=False):
    if verbose:
        print "-- %s" % cmd
    if call(cmd, shell=True):
        raise ScrubError("Command '%s' returned non-zero exit status" % cmd)


def write(fd, offset, buf):
    try:
        # Seek to the offset
        if os.lseek(fd, offset, os.SEEK_SET) == -1:
            raise ScrubError("Unable to seek to offset '%d'" % offset)
        if os.write(fd, buf) == -1:
            raise ScrubError("Failed to scrub chunk at offset '%d'" % offset)
    except (OSError, IOError), e:
        raise ScrubError("Write Failed with: %s" % e)


def read(fd, offset, length):
    try:
        # Seek to the offset
        if os.lseek(fd, offset, os.SEEK_SET) == -1:
            raise ScrubError("Unable to seek to offset '%d'" % offset)
        return os.read(fd, length)
    except (OSError, IOError), e:
        raise ScrubError("Read Failed with: %s" % e)


def read_exception_metadata(fd, chunk_size, index):
    # exception = { uint64 old_chunk, uint64 new_chunkc }
    # if the size of each exception metadata is 16 bytes,
    # exceptions_per_chunk is how many exceptions can fit in one chunk
    exceptions_per_chunk = chunk_size / 16
    # Offset where the exception metadata store begins
    store_offset = 1 + ((exceptions_per_chunk + 1) * index)
    # seek to the begining of the exception metadata store
    # and read the entire store
    store = read(fd, chunk_size * store_offset, chunk_size)
    exception = 0
    while exception < exceptions_per_chunk:
        # Unpack 1 exception metatdata from the store
        (old_chunk, new_chunk) = unpack_from('<QQ', store, exception * 16)
        # Yields the offset where the exception exists in the cow
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
        print "-- Invalid COW device '%s'; header magic doesn't match" % cow
        return 1

    if header[1] != SNAPSHOT_VALID_FLAG:
        print "-- Invalid COW device '%s'; valid flag not set '%d' got '%d'"\
            % (cow, SNAPSHOT_VALID_FLAG, header[1])
        return 1

    if header[2] != SNAPSHOT_DISK_VERSION:
        print "-- Unknown metadata version '%s'; expected '%d' got '%d' "\
            % (cow, SNAPSHOT_DISK_VERSION, header[2])
        return 1

    header = list(header)
    # Chunk size is byte aligned to 512 bytes
    # (0 << SECTOR_SHIFT) == 512
    header[3] = header[3] << SECTOR_SHIFT
    return header


def scrub(cow, options):
    try:
        if options.verbose:
            print "-- Opening Cow '%s'" % cow
        # Open the cow block device
        fd = os.open(cow, os.O_RDWR)
    except OSError, e:
        raise ScrubError("Failed to open cow '%s'" % e)

    # Read the meta data header
    header = read_header(fd)

    if options.verbose:
        print "-- Magic: %X" % header[0]
        print "-- Valid: %d" % header[1]
        print "-- Version: %d" % header[2]
        print "-- Chunk Size: %d" % header[3]

    # Create a buffer of nulls the size of the chunk
    scrub_buf = '\0' * header[3]

    store, count = (0, 0)
    while True:
        # Iterate through all the exceptions
        for offset in read_exception_metadata(fd, header[3], store):
            # zero means we reached the last exception
            if offset == 0:
                if options.display_only:
                    print "-- Counted '%d' exceptions in the cow" % count
                return os.close(fd)
            if options.verbose == 2:
                print "--- Exception ---"
                print read(fd, offset, header[3])
                print "-----------------"
            count = count + 1
            if not options.display_only:
                if options.verbose:
                    print "-- Scrubing exception at %d" % offset
                # Write a chunk full of NULL's at 'offset'
                write(fd, offset, scrub_buf)
        # Seek the next store
        store = store + 1


def prepare_cow(cow, verbose):
    # Don't attempt to re-create a -zero linear device if it already exists
    if os.path.exists(cow + '-zero'):
        return

    try:
        # copy the cow device linear table mapping
        cow_table = check_output("dmsetup table %s" % cow, shell=True).rstrip()
    except CalledProcessError, e:
        raise ScrubError("dmsetup failed '%s'; not running as root?" % e)

    try:
        # create a new handle to the same blocks as in use by the cow
        run("echo '%s' | dmsetup create %s" % (cow_table, cow + '-zero'), verbose)

        # suspend the cow (this will essentially suspend the origin)
        run("dmsetup suspend %s" % cow, verbose)

        # update the table for the cow device to always return io errors
        # e.g. "0 204800 linear 8:16 4194688" => "0 204800 error"
        parts = cow_table.split()
        error_table = "%s %s %s" % (parts[0], parts[1], 'error')
        run("echo '%s' | dmsetup reload %s" % (error_table, cow), verbose)
    except ScrubError, e:
        # If somthing went wrong, remove the cow-zero volume
        run("dmsetup remove %s" % (cow + '-zero'), verbose)
        raise
    finally:
        # resume the cow to let writes start happening back on the origin
        run("dmsetup resume %s" % cow, verbose)


def remove_snapshot(snapshot, options):
    if not os.path.exists(snapshot):
        raise ScrubError("snapshot '%s' does not exist" % snapshot)

    # Rebuild the path to find the cow for our snapshot
    path = snapshot.split('/')
    cow_device = "%s-%s-cow" % (path[2], path[3])
    cow = "/".join(['', path[1], 'mapper', cow_device])

    # The -zero device might already exist if recovering from a botched scrub
    if not options.display_only:
        prepare_cow(cow_device, options.verbose)

    if os.path.exists(cow + '-zero'):
        cow = cow + '-zero'

    # scrub the cow
    scrub(cow, options)

    if not options.display_only or options.skip_remove:
        print "-- Removing snapshot"
        # Remove the cow-zero
        run("dmsetup remove -f %s" % cow_device + '-zero', options.verbose)
        # Remove the snapshot
        run("lvremove %s -ff" % snapshot, options.verbose)


if __name__ == "__main__":
    description = "Scrub the COW of an lvm snapshot then delete the snapshot"
    parser = OptionParser(usage="Usage: %prog <snapshot path> [-h]",
            description=description)
    parser.add_option('-v', '--verbose', action='count',
            help="Be verbose, -vv is very verbose")
    parser.add_option('-d', '--display-only', const=True, action='store_const',
            help="Do not scrub the cow, display cow stats & exit; implies -v")
    parser.add_option('-s', '--skip-remove', const=True, action='store_const',
            help="Do not remove the snapshot after scrubbing")
    options, args = parser.parse_args()

    if not len(args):
        parser.print_help()
        sys.exit(1)

    if options.display_only:
        print "-- Display Only, Not Scrubbing"
        if options.verbose < 2:
            options.verbose = 1

    try:
        sys.exit(remove_snapshot(args[0], options))
    except ScrubError, e:
        print "-- %s" % e
        sys.exit(1)
