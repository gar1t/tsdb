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

#include <stdlib.h>
#include <limits.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <db.h>
#include <errno.h>

#include "tsdb_trace.h"
#include "quicklz.h"

#define CHUNK_GROWTH 10000
#define CHUNK_LEN_PADDING 400
#define MAX_NUM_FRAGMENTS 16384

typedef struct {
    u_int8_t *data;
    u_int32_t data_len;
    u_int32_t epoch;
    u_int8_t growable;
    u_int8_t fragment_changed[MAX_NUM_FRAGMENTS];
    u_int32_t base_index;
} tsdb_chunk;

typedef struct {
    u_int32_t *array;
    u_int32_t array_len;
} tsdb_tag;

typedef u_int32_t tsdb_value;

typedef struct {
    u_int8_t alive;
    u_int8_t read_only;
    u_int16_t values_per_entry;
    u_int16_t values_len;
    u_int32_t unknown_value;
    u_int32_t lowest_free_index;
    u_int32_t slot_duration;
    qlz_state_compress state_compress;
    qlz_state_decompress state_decompress;
    tsdb_chunk chunk;
    DB *db;
} tsdb_handler;

extern int  tsdb_open(char *tsdb_path, tsdb_handler *handler,
		      u_int16_t *values_per_entry,
		      u_int32_t slot_duration,
		      u_int8_t read_only);

extern void tsdb_close(tsdb_handler *handler);

extern void normalize_epoch(tsdb_handler *handler, u_int32_t *epoch);

extern int tsdb_goto_epoch(tsdb_handler *handler,
                           u_int32_t epoch_value,
                           u_int8_t fail_if_missing,
                           u_int8_t growable);

extern int tsdb_set(tsdb_handler *handler, char *key, tsdb_value *value);

extern int tsdb_set_with_index(tsdb_handler *handler, char *key,
                               tsdb_value *value, u_int32_t *index);

extern int tsdb_get_by_key(tsdb_handler *handler,
                           char *key,
                           tsdb_value **value);

extern int tsdb_get_key_index(tsdb_handler *handler,
                              char *key,
                              u_int32_t *index);

extern int tsdb_get_by_index(tsdb_handler *handler,
                             u_int32_t *index,
                             tsdb_value **value);

extern void tsdb_flush(tsdb_handler *handler);

extern int tsdb_tag_key(tsdb_handler *handler, char* key, char* tag_name);

extern int tsdb_get_tag_indexes(tsdb_handler *handler,
                                char *tag_name,
                                u_int32_t *indexes,
                                u_int32_t indexes_len,
                                u_int32_t *count);

#define TSDB_AND 1
#define TSDB_OR  2

extern int tsdb_get_consolidated_tag_indexes(tsdb_handler *handler,
                                             char **tag_names,
                                             u_int16_t tag_names_len,
                                             int consolidator,
                                             u_int32_t *indexes,
                                             u_int32_t indexes_len,
                                             u_int32_t *count);
