/*
 *
 *  Copyright (C) 2011 IIT/CNR (http://www.iit.cnr.it/en)
 *                     Luca Deri <deri@ntop.org>
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <strings.h>
#include <limits.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h> /* mmap() */
#include <stdarg.h>
#include <sys/stat.h>
#include <db.h>
#include <errno.h>
#include <sys/file.h>

#include "tsdb_trace.h"
#include "quicklz.h"

/* ************************************************** */

//#define CHUNK_GROWTH        100000
#define CHUNK_GROWTH        10000
#define MAX_NUM_FRAGMENTS      16384

typedef struct {
  u_int8_t *chunk_mem;
  u_int32_t chunk_mem_len;
  u_int32_t begin_epoch;
  u_int8_t growable;
  u_int32_t num_hash_indexes;
  u_int8_t fragment_changed[MAX_NUM_FRAGMENTS];
  u_int32_t base_index;
  u_int8_t load_page_on_demand;
  u_int32_t load_epoch;
} tsdb_chunk;

typedef u_int32_t tsdb_value;

typedef struct {
  u_int8_t  alive_and_kicking;
  u_int8_t  read_only_mode;              /* Mode used to open the db file */
  u_int16_t num_values_per_entry;        /* How many tsdb_value will be specified per slot */
  u_int16_t values_len;                  /* Size of this value */
  u_int32_t default_unknown_value;       /* Default 0 */
  u_int32_t lowest_free_index;           /* Hint for accelerating the assignment of free indexes */
  u_int32_t rrd_slot_time_duration;      /* (sec) */
  qlz_state_compress state_compress;     /* */
  qlz_state_decompress state_decompress; /* */

  /* Chunks */
  tsdb_chunk chunk;

  /* Index mapping hash */
  DB *db;
} tsdb_handler;

typedef struct {
  u_int32_t epoch_start; /* Epoch since this mapping is valid       */
  u_int32_t epoch_end;   /* Epoch until which this mapping is valid */
  u_int32_t hash_idx;
} tsdb_hash_mapping;

/* ************************************************** */

extern int  tsdb_open(char *tsdb_path, tsdb_handler *handler,
		      u_int16_t *num_values_per_entry,
		      u_int32_t rrd_slot_time_duration,
		      u_int8_t read_only_mode);

extern void tsdb_close(tsdb_handler *handler);

extern u_int32_t normalize_epoch(tsdb_handler *handler,
				 u_int32_t *epoch);

extern int tsdb_goto_epoch(tsdb_handler *handler,
			    u_int32_t epoch_value,
			    u_int8_t create_if_needed,
			    u_int8_t growable,
			    u_int8_t load_page_on_demand);

extern int tsdb_set(tsdb_handler *handler,
		    char *hash_index,
		    tsdb_value *value_to_store);

extern int tsdb_get(tsdb_handler *handler,
		    char *hash_index,
		    tsdb_value **value_to_read);

extern void tsdb_drop_key(tsdb_handler *handler,
			  char *hash_index,
			  u_int32_t epoch_value);
