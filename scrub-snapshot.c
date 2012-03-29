/*
 * Copyright (C) 2001-2002 Sistina Software (UK) Limited.
 * Copyright (C) 2006 Red Hat, Inc.
 *
 * This file is released under the GPL.
 */

/*
 * Snapshot COW device format (version 1) is basically:
 *
 *  - chunks are a certain number of sectors long (512 bytes), as
 *    defined at snapshot creation
 *  - first chunk contains a header containing, amongst other things,
 *    the number of sectors per chunk
 *  - next chunk contains as many exception records as it can contain
 *  - each exception record details the index of the chunk on the
 *    backing device which this exception shadows and the index of
 *    the corresponding shadow chunk on the COW device
 *  - that metadata chunk is followed by the corresponding chunks
 *    for each of those exceptions
 *  - the rest of the device continues with the same format - metadata
 *    area followed by the actual chunks, followed by metadata area ..
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <endian.h>
#include <byteswap.h>

#if __BYTE_ORDER == __LITTLE_ENDIAN
# define le32_to_cpu(i) ((uint32_t)(i))
# define le64_to_cpu(i) ((uint64_t)(i))
# else /* __BYTE_ORDER == __BIG_ENDIAN */
# define le32_to_cpu(i) (bswap_32 ((uint32_t)(i)))
# define le64_to_cpu(i) (bswap_64 ((uint64_t)(u)))
# endif

#define SECTOR_SHIFT 9

#define SNAPSHOT_DISK_MAGIC   0x70416e53
#define SNAPSHOT_DISK_VERSION 1

struct disk_header
{
  uint32_t magic;
  uint32_t valid;
  uint32_t version;
  uint32_t chunk_size;
};

struct disk_exception
{
  uint64_t old_chunk;
  uint64_t new_chunk;
};

struct pstore
{
  struct disk_header hdr;

  int cow_fd;
  int real_fd;
  int exceptions_per_area;

  void *meta_chunk;
  void *data_chunk;
};

static int
read_n_bytes (int      fd,
              void    *buf,
              ssize_t  n_bytes)
{
  while (n_bytes > 0)
    {
      ssize_t n;

      n = read (fd, buf, n_bytes);
      if (n <= 0)
        {
          if (n == -1 && errno == EINTR)
            continue;
          else
            return -1;
        }

      n_bytes -= n;
      buf     += n;
    }

  return n_bytes;
}

static int
write_n_bytes (int      fd,
               void    *buf,
               ssize_t  n_bytes)
{
  while (n_bytes > 0)
    {
      ssize_t n;

      n = write (fd, buf, n_bytes);
      if (n == -1)
        {
          if (errno == EINTR)
            continue;
          else
            return -1;
        }

      n_bytes -= n;
      buf     += n;
    }

  return n_bytes;
}

static int
read_chunk (struct pstore *ps,
            int            chunk_index,
            void          *buf)
{
  ssize_t chunk_len;

  chunk_len = ps->hdr.chunk_size << SECTOR_SHIFT;

  if (lseek (ps->cow_fd, chunk_len * chunk_index, SEEK_SET) == -1)
    return -1;

  return read_n_bytes (ps->cow_fd, buf, chunk_len);
}

static int
read_area (struct pstore *ps,
           int            area_index)
{
  int chunk_index;

  chunk_index = 1 + ((ps->exceptions_per_area + 1) * area_index);

  return read_chunk (ps, chunk_index, ps->meta_chunk);
}

static int
merge_chunk (struct pstore         *ps,
             struct disk_exception *exception)
{
  ssize_t chunk_len;

  if (read_chunk (ps, exception->new_chunk, ps->data_chunk) == -1)
    return -1;

  chunk_len = ps->hdr.chunk_size << SECTOR_SHIFT;

  if (lseek (ps->real_fd, chunk_len * exception->old_chunk, SEEK_SET) == -1)
    return -1;

  return write_n_bytes (ps->real_fd, ps->data_chunk, chunk_len);
}

void print_chunk(struct pstore *ps){
    int index;
    size_t chunk_len;

    chunk_len = ps->hdr.chunk_size << SECTOR_SHIFT;
    for(index=0; index < chunk_len; index++) {
        printf("%x", ps->meta_chunk + index);
    }
    printf("\n\n\n");
}

int
main (int argc, char **argv)
{
  const char    *cow_device;
  const char    *real_device;
  int            exit_code;
  struct pstore  ps;
  int            area;
  int            prev_area_was_full;

  exit_code = 1;

  if (argc != 2)
    {
      fprintf (stderr, "Usage: %s <cow-device>\n", argv[0]);
      goto error_out;
    }

  cow_device  = argv[1];
  //real_device = argv[2];

  ps.cow_fd = open (cow_device, O_RDONLY);
  if (ps.cow_fd < 0)
    {
      fprintf (stderr, "Error opening '%s' for reading: %s\n",
               cow_device, strerror (errno));
      goto error_out;
    }

  if (read_n_bytes (ps.cow_fd, &ps.hdr, sizeof (ps.hdr)) == -1)
    {
      fprintf (stderr, "Error reading from '%s': %s\n",
               cow_device, strerror (errno));
      goto error_close_cow;
    }

  ps.hdr.magic      = le32_to_cpu (ps.hdr.magic);
  ps.hdr.valid      = le32_to_cpu (ps.hdr.valid);
  ps.hdr.version    = le32_to_cpu (ps.hdr.version);
  ps.hdr.chunk_size = le32_to_cpu (ps.hdr.chunk_size);

  printf ("magic: 0x%x\n",    ps.hdr.magic);
  printf ("valid: %d\n",      ps.hdr.valid);
  printf ("version: %d\n",    ps.hdr.version);
  printf ("chunk_size: %d\n", ps.hdr.chunk_size);

  if (ps.hdr.magic != SNAPSHOT_DISK_MAGIC)
    {
      fprintf (stderr, "'%s' is not a valid Copy-On-Write device: header magic does not match\n",
               cow_device);
      goto error_close_cow;
    }

  if (ps.hdr.version != SNAPSHOT_DISK_VERSION)
    {
      fprintf (stderr, "'%s' is not a valid Copy-On-Write device: unknown metadata format version '%d', expected '%d'\n",
               cow_device, ps.hdr.version, SNAPSHOT_DISK_VERSION);
      goto error_close_cow;
    }

  if (ps.hdr.valid != 1)
    {
      fprintf (stderr, "'%s' is not a valid Copy-On-Write device: valid flag is not set\n",
               cow_device);
      goto error_close_cow;
    }

  ps.exceptions_per_area = (ps.hdr.chunk_size << SECTOR_SHIFT) / sizeof (struct disk_exception);

  printf ("exceptions_per_area: %d\n", ps.exceptions_per_area);

  /*ps.real_fd = open (real_device, O_WRONLY);
  if (ps.real_fd < 0)
    {
      fprintf (stderr, "Error opening '%s' for writing: %s\n",
               real_device, strerror (errno));
      goto error_close_real;
    }*/

  ps.meta_chunk = malloc (ps.hdr.chunk_size << SECTOR_SHIFT);
  if (ps.meta_chunk == NULL)
    {
      fprintf (stderr, "Failed to allocate %d bytes for the metadata buffer\n",
               ps.hdr.chunk_size << SECTOR_SHIFT);
      goto error_close_real;
    }

  ps.data_chunk = malloc (ps.hdr.chunk_size << SECTOR_SHIFT);
  if (ps.data_chunk == NULL)
    {
      fprintf (stderr, "Failed to allocate %d bytes for the data buffer\n",
               ps.hdr.chunk_size << SECTOR_SHIFT);
      goto error_free_meta;
    }

  area = 0;
  prev_area_was_full = 1;
  while (prev_area_was_full)
    {
      int exception;

      if (read_area (&ps, area) == -1)
        {
          fprintf (stderr, "Error reading area %d from '%s': %s\n",
                   area, cow_device, strerror (errno));
          goto error_free_data;
        }

      exception = 0;
      while (exception < ps.exceptions_per_area)
        {
          struct disk_exception de = ((struct disk_exception *) ps.meta_chunk)[exception];

          de.old_chunk = le64_to_cpu (de.old_chunk);
          de.new_chunk = le64_to_cpu (de.new_chunk);

          if (de.new_chunk == 0)
            {
              printf("No more chunks in cow; quiting...\n");
              prev_area_was_full = 0;
              break;
            }

          printf ("[area %d, exception %d] old: %lld, new: %lld\n",
                  area, exception, de.old_chunk, de.new_chunk);

          if (read_chunk (&ps, de.new_chunk, ps.data_chunk) == -1)
            return -1;

         print_chunk(&ps);

          /*if (merge_chunk (&ps, &de) == -1)
            {
              fprintf (stderr, "Error merging chunk %lld from '%s' to chunk %lld on '%s': %s\n",
                       de.new_chunk, cow_device, de.old_chunk, real_device, strerror (errno));
              goto error_free_data;
            }*/

          exception++;
        }

      area++;
    }

  exit_code = 0;

 error_free_meta:
  ps.meta_chunk = NULL;
  free (ps.meta_chunk);

 error_free_data:
  ps.data_chunk = NULL;
  free (ps.data_chunk);

 error_close_real:
  //close (ps.real_fd);
  //ps.real_fd = 0;

 error_close_cow:
  close (ps.cow_fd);
  ps.cow_fd = 0;

 error_out:
  return exit_code;
}
