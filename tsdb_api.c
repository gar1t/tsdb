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

#include "tsdb_api.h"


/* *********************************************************************** */

static void map_raw_set(tsdb_handler *handler,
			void *key, u_int32_t key_len,
			void *value, u_int32_t value_len);

static void map_raw_delete(tsdb_handler *handler,
			   void *key, u_int32_t key_len);

static int map_raw_get(tsdb_handler *handler,
		       void *key, u_int32_t key_len,
		       void **value, u_int32_t *value_len);

/* *********************************************************************** */

int tsdb_open(char *tsdb_path, tsdb_handler *handler,
	      u_int16_t *num_values_per_entry,
	      u_int32_t rrd_slot_time_duration,
	      u_int8_t read_only_mode) {
  void *value;
  u_int32_t value_len;
  int ret, mode;

  memset(handler, 0, sizeof(tsdb_handler));
  handler->read_only_mode = read_only_mode;

  /* DB */
  if((ret = db_create(&handler->db, NULL, 0)) != 0) {
    traceEvent(TRACE_ERROR, "Error while creating DB handler [%s]", db_strerror(ret));
    return(-1);
  }

  mode = (read_only_mode ? 00444 : 00664 );
  if((ret = handler->db->open(handler->db,
			      NULL,
			      (const char*)tsdb_path,
			      NULL,
			      DB_BTREE,
			      (read_only_mode ? 0 : DB_CREATE),
			      mode)) != 0) {
    traceEvent(TRACE_ERROR, "Error while opening DB %s [%s][r/o=%u,mode=%o]",
	       tsdb_path, db_strerror(ret), read_only_mode, mode);
    return(-1);
  }

  if(map_raw_get(handler, "lowest_free_index",
		 strlen("lowest_free_index"), &value, &value_len) == 0) {
    handler->lowest_free_index = *((u_int32_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->lowest_free_index = 0;
      map_raw_set(handler, "lowest_free_index", strlen("lowest_free_index"),
		  &handler->lowest_free_index, sizeof(handler->lowest_free_index));
    }
  }

  if(map_raw_get(handler, "rrd_slot_time_duration",
		 strlen("rrd_slot_time_duration"), &value, &value_len) == 0) {
    handler->rrd_slot_time_duration = *((u_int32_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->rrd_slot_time_duration = rrd_slot_time_duration;
      map_raw_set(handler, "rrd_slot_time_duration", strlen("rrd_slot_time_duration"),
		  &handler->rrd_slot_time_duration, sizeof(handler->rrd_slot_time_duration));
    }
  }

  if(map_raw_get(handler, "num_values_per_entry",
		 strlen("num_values_per_entry"), &value, &value_len) == 0) {
    *num_values_per_entry = handler->num_values_per_entry = *((u_int16_t*)value);
  } else {
    if(!handler->read_only_mode) {
      handler->num_values_per_entry = *num_values_per_entry;
      map_raw_set(handler, "num_values_per_entry", strlen("num_values_per_entry"),
		  &handler->num_values_per_entry, sizeof(handler->num_values_per_entry));
    }
  }

  handler->values_len = handler->num_values_per_entry * sizeof(tsdb_value);

  traceEvent(TRACE_INFO, "lowest_free_index:      %u", handler->lowest_free_index);
  traceEvent(TRACE_INFO, "rrd_slot_time_duration: %u", handler->rrd_slot_time_duration);
  traceEvent(TRACE_INFO, "num_values_per_entry:   %u", handler->num_values_per_entry);

  memset(&handler->state_compress, 0, sizeof(handler->state_compress));
  memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

  handler->alive_and_kicking = 1;

  return(0);
}

/* *********************************************************************** */

static void map_raw_delete(tsdb_handler *handler,
			   void *key, u_int32_t key_len) {
  DBT key_data;

  if(handler->read_only_mode) {
    traceEvent(TRACE_WARNING, "Unable to delete value (read-only mode)");
    return;
  }

  memset(&key_data, 0, sizeof(key_data));
  key_data.data = key, key_data.size = key_len;

  if(handler->db->del(handler->db, NULL, &key_data, 0) != 0)
    traceEvent(TRACE_WARNING, "Error while deleting key");
}

/* *********************************************************************** */

static int map_raw_key_exists(tsdb_handler *handler,
			      void *key, u_int32_t key_len) {
  void *value;
  u_int value_len;

  if(map_raw_get(handler, key, key_len, &value, &value_len) == 0) {
    return(1);
  } else
    return(0);
}

/* *********************************************************************** */

static void map_raw_set(tsdb_handler *handler,
			void *key, u_int32_t key_len,
			void *value, u_int32_t value_len) {
  DBT key_data, data;

  if(handler->read_only_mode) {
    traceEvent(TRACE_WARNING, "Unable to set value (read-only mode)");
    return;
  }

  memset(&key_data, 0, sizeof(key_data));
  memset(&data, 0, sizeof(data));
  key_data.data = key, key_data.size = key_len;
  data.data = value, data.size = value_len;

  if(handler->db->put(handler->db, NULL, &key_data, &data, 0) != 0)
    traceEvent(TRACE_WARNING, "Error while map_set(%u, %u)", key, value);
}

/* *********************************************************************** */

static int map_raw_get(tsdb_handler *handler,
		       void *key, u_int32_t key_len,
		       void **value, u_int32_t *value_len) {
  DBT key_data, data;

  memset(&key_data, 0, sizeof(key_data));
  memset(&data, 0, sizeof(data));

  key_data.data = key, key_data.size = key_len;
  if(handler->db->get(handler->db, NULL, &key_data, &data, 0) == 0) {
    *value = data.data, *value_len = data.size;
    return(0);
  } else {
    return(-1);
  }
}

/* *********************************************************************** */

static void tsdb_flush_chunk(tsdb_handler *handler) {
  char *compressed;
  u_int compressed_len, new_len, num_fragments, i;
  u_int fragment_size = handler->values_len * CHUNK_GROWTH;
  char str[32];

  if(!handler->chunk.chunk_mem) return;

  new_len = handler->chunk.chunk_mem_len + 400 /* Static value */;
  compressed = (char*)malloc(new_len);
  if(!compressed) {
    traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", new_len);
    return;
  }

  /* Split chunks on the DB */
  num_fragments = handler->chunk.chunk_mem_len / fragment_size;

  for(i=0; i<num_fragments; i++) {
    u_int offset;

    if((!handler->read_only_mode)
       && handler->chunk.fragment_changed[i]) {
      offset = i * fragment_size;

      compressed_len = qlz_compress(&handler->chunk.chunk_mem[offset],
				    compressed, fragment_size,
				    &handler->state_compress);

      traceEvent(TRACE_NORMAL, "Compression %u -> %u [fragment %u] [%.1f %%]",
		 fragment_size, compressed_len, i,
		 ((float)(compressed_len*100))/((float)fragment_size));

      snprintf(str, sizeof(str), "%u-%u", handler->chunk.begin_epoch, i);

      map_raw_set(handler, str, strlen(str), compressed, compressed_len);
    } else
      traceEvent(TRACE_INFO, "Skipping fragment %u (unchanged)", i);
  }

  free(compressed);
  free(handler->chunk.chunk_mem);
  memset(&handler->chunk, 0, sizeof(handler->chunk));
}

/* *********************************************************************** */

void tsdb_close(tsdb_handler *handler) {

  if(!handler->alive_and_kicking) {
    return;
  }

  if(!handler->read_only_mode)
    map_raw_set(handler, "lowest_free_index", strlen("lowest_free_index"),
		&handler->lowest_free_index, sizeof(handler->lowest_free_index));

  tsdb_flush_chunk(handler);

  if(!handler->read_only_mode)
    traceEvent(TRACE_INFO, "Flushing database changes...");

  handler->db->close(handler->db, 0);
}

/* *********************************************************************** */

u_int32_t normalize_epoch(tsdb_handler *handler, u_int32_t *epoch) {
  // *epoch = *epoch + (handler->rrd_slot_time_duration-1);
  *epoch -= *epoch % handler->rrd_slot_time_duration;
  *epoch += timezone - daylight*3600;

  return(*epoch);
}

/* *********************************************************************** */

static void reserve_hash_index(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  map_raw_set(handler, str, strlen(str), "", 0);
}

/* *********************************************************************** */

static void unreserve_hash_index(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  map_raw_delete(handler, str, strlen(str));
}

/* *********************************************************************** */

static int hash_index_in_use(tsdb_handler *handler, u_int32_t idx) {
  char str[32];

  snprintf(str, sizeof(str), "rsv-%u", idx);
  return(map_raw_key_exists(handler, str, strlen(str)));
}

/* *********************************************************************** */

static int get_map_hash_index(tsdb_handler *handler, char *name, u_int32_t *value) {
  void *ptr;
  u_int32_t len;
  char str[32] = { 0 };

  snprintf(str, sizeof(str), "map-%s", name);

  if(map_raw_get(handler, str, strlen(str), &ptr, &len) == 0) {
    tsdb_hash_mapping *mappings = (tsdb_hash_mapping*)ptr;
    u_int i, found = 0, num_mappings = len / sizeof(tsdb_hash_mapping);

    for(i=0; i<num_mappings; i++) {
      if((mappings[i].epoch_start <= handler->chunk.load_epoch)
	 && ((mappings[i].epoch_end == 0)
	     || (mappings[i].epoch_end > handler->chunk.load_epoch))) {
	*value = mappings[i].hash_idx;
	found = 1;
	break;
      }
    } /* for */

    //free(ptr);
    // traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", idx, *value);
    return(found ? 0 : -1);
  }

  return(-1);
}

/* *********************************************************************** */

static int drop_map_hash_index(tsdb_handler *handler, char *idx,
			       u_int32_t epoch_value, u_int32_t *value) {
  void *ptr;
  u_int32_t len;
  char str[32];

  snprintf(str, sizeof(str), "map-%s", idx);

  if(map_raw_get(handler, str, strlen(str), &ptr, &len) == 0) {
    tsdb_hash_mapping *mappings = (tsdb_hash_mapping*)ptr;
    u_int i, found = 0, num_mappings = len / sizeof(tsdb_hash_mapping);

    for(i=0; i<num_mappings; i++) {
      if(mappings[i].epoch_end == 0) {
	mappings[i].epoch_end = epoch_value;
	found = 1;
	map_raw_set(handler, str, strlen(str), &ptr, len);
	break;
      }
    }

    //free(ptr);
    // traceEvent(TRACE_INFO, "[GET] Mapping %u -> %u", idx, *value);
    return(found ? 0 : -1);
  }

  return(-1);
}

/* *********************************************************************** */

void tsdb_drop_key(tsdb_handler *handler,
		    char *hash_name,
		    u_int32_t epoch_value) {
  u_int32_t hash_idx;

  if(drop_map_hash_index(handler, hash_name,
			 epoch_value, &hash_idx) == 0) {
    traceEvent(TRACE_INFO, "Index %s mapped to hash %u", hash_name, hash_idx);
    unreserve_hash_index(handler, hash_idx);
  } else
    traceEvent(TRACE_WARNING, "Unable to drop key %s", hash_name);
}

/* *********************************************************************** */

static void set_map_hash_index(tsdb_handler *handler, char *idx, u_int32_t value) {
  char str[32];
  tsdb_hash_mapping mapping;

  snprintf(str, sizeof(str), "map-%s", idx);
  mapping.epoch_start = handler->chunk.load_epoch;
  mapping.epoch_end = 0;
  mapping.hash_idx = value;
  map_raw_set(handler, str, strlen(str), &mapping, sizeof(mapping));

  traceEvent(TRACE_INFO, "[SET] Mapping %s -> %u", idx, value);
}

/* *********************************************************************** */

int tsdb_goto_epoch(tsdb_handler *handler,
		     u_int32_t epoch_value,
		     u_int8_t create_if_needed,
		     u_int8_t growable,
		     u_int8_t load_page_on_demand) {
  int rc;
  void *value;
  u_int32_t value_len, fragment_id = 0;
  char str[32];

  /* Flush ond chunk if loaded */
  tsdb_flush_chunk(handler);

  normalize_epoch(handler, &epoch_value);
  handler->chunk.load_page_on_demand = load_page_on_demand;
  handler->chunk.load_epoch = epoch_value;

  if(handler->chunk.load_page_on_demand)
    return(0);

  snprintf(str, sizeof(str), "%u-%u", epoch_value, fragment_id);
  rc = map_raw_get(handler, str, strlen(str), &value, &value_len);

  if(rc == -1) {
    if(!create_if_needed) {
      traceEvent(TRACE_INFO, "Unable to goto epoch %u", epoch_value);
      return(-1);
    } else
      traceEvent(TRACE_INFO, "Moving to goto epoch %u", epoch_value);

    /* Create the entry */
    handler->chunk.begin_epoch = epoch_value, handler->chunk.num_hash_indexes = CHUNK_GROWTH;
    handler->chunk.chunk_mem_len = handler->values_len*handler->chunk.num_hash_indexes;
    handler->chunk.chunk_mem = (u_int8_t*)malloc(handler->chunk.chunk_mem_len);

    if(handler->chunk.chunk_mem == NULL) {
      traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", handler->chunk.chunk_mem_len);
      return(-2);
    }

    memset(handler->chunk.chunk_mem, handler->default_unknown_value, handler->chunk.chunk_mem_len);
  } else {
    /* We need to decompress data and glue up all fragments */
    u_int32_t len, offset = 0;
    u_int8_t *ptr;

    fragment_id = 0;
    handler->chunk.chunk_mem_len = 0;

    while(1) {
      len = qlz_size_decompressed(value);

      ptr = (u_int8_t*)malloc(handler->chunk.chunk_mem_len + len);
      if(ptr == NULL) {
	traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)",
		   handler->chunk.chunk_mem_len+len);
	free(value);
	return(-2);
      }

      if(handler->chunk.chunk_mem_len > 0)
	memcpy(ptr, handler->chunk.chunk_mem, handler->chunk.chunk_mem_len);

      len = qlz_decompress(value, &ptr[offset], &handler->state_decompress);
      //free(value);

      traceEvent(TRACE_NORMAL, "Decompression %u -> %u [fragment %u] [%.1f %%]",
		 value_len, len, fragment_id,
		 ((float)(len*100))/((float)value_len));

      handler->chunk.chunk_mem_len += len;
      fragment_id++, offset = handler->chunk.chunk_mem_len;

      free(handler->chunk.chunk_mem);
      handler->chunk.chunk_mem = ptr;

      snprintf(str, sizeof(str), "%u-%u", epoch_value, fragment_id);
      if(map_raw_get(handler, str, strlen(str), &value, &value_len) == -1)
	break; /* No more fragments */
    } /* while */

    handler->chunk.begin_epoch = epoch_value;
    handler->chunk.num_hash_indexes = handler->chunk.chunk_mem_len / handler->values_len;

    traceEvent(TRACE_INFO, "Moved to goto epoch %u", epoch_value);
  }

  handler->chunk.growable = growable;

  return(0);
}

/* *********************************************************************** */

static int mapIndexToHash(tsdb_handler *handler, char *idx,
			  u_int32_t *value, u_int8_t create_idx_if_needed) {
  /* Check if this is a known value */
  if(get_map_hash_index(handler, idx, value) == 0) {
    traceEvent(TRACE_INFO, "Index %s mapped to hash %u", idx, *value);
    return(0);
  }

  if(!create_idx_if_needed) {
    traceEvent(TRACE_INFO, "Unable to find index %s", idx);
    return(-1);
  }


  while(1) {
    *value = handler->lowest_free_index++;

    if(!hash_index_in_use(handler, *value)) {
      set_map_hash_index(handler, idx, *value);
      reserve_hash_index(handler, *value);
      break;
    }
  }

  // traceEvent(TRACE_NORMAL, "lowest_free_index:      %u [%s]", handler->lowest_free_index, idx);

  /* traceEvent(TRACE_INFO, "Index %s mapped to hash %u", idx, *value); */

  return(0);
}


/* *********************************************************************** */

static int getOffset(tsdb_handler *handler, char *hash_name,
		     u_int64_t *offset, u_int8_t create_idx_if_needed) {
  u_int32_t hash_index;

  if(mapIndexToHash(handler, hash_name, &hash_index, create_idx_if_needed) == -1) {
    traceEvent(TRACE_INFO, "Unable to find index %s", hash_name);
    return(-1);
  } else
    traceEvent(TRACE_INFO, "%s mapped to idx %u", hash_name, hash_index);

  if(handler->chunk.load_page_on_demand || handler->read_only_mode) {
    u_int32_t fragment_id = hash_index / CHUNK_GROWTH, value_len;
    char str[32];
    void *value;

    /* We need to load the epoch handler->chunk.load_epoch/fragment_id */

    snprintf(str, sizeof(str), "%u-%u", handler->chunk.load_epoch, fragment_id);
    if(map_raw_get(handler, str, strlen(str), &value, &value_len) == -1)
      return(-1);

    handler->chunk.chunk_mem_len = qlz_size_decompressed(value);

    handler->chunk.chunk_mem = (u_int8_t*)malloc(handler->chunk.chunk_mem_len);
    if(handler->chunk.chunk_mem == NULL) {
      traceEvent(TRACE_WARNING, "Not enough memory (%u bytes)", handler->chunk.chunk_mem_len);
      return(-2);
    }

    qlz_decompress(value, handler->chunk.chunk_mem, &handler->state_decompress);
    //free(value);

    handler->chunk.begin_epoch = handler->chunk.load_epoch;
    handler->chunk.num_hash_indexes = handler->chunk.chunk_mem_len / handler->values_len;
    handler->chunk.base_index = fragment_id * CHUNK_GROWTH;

    /* Shift index */
    hash_index -= handler->chunk.base_index;
  }

 redo_getOffset:

  if(hash_index >= handler->chunk.num_hash_indexes) {
    if(!handler->chunk.growable) {
      traceEvent(TRACE_ERROR, "Index %u out of range %u...%u",
		 hash_index,
		 0,
		 handler->chunk.num_hash_indexes);
      return(-1);
    } else {
      u_int32_t to_add  = CHUNK_GROWTH * handler->values_len;
      u_int32_t new_len = handler->chunk.chunk_mem_len + to_add;
      u_int8_t *ptr    = malloc(new_len);

      if(ptr != NULL) {
	memcpy(ptr, handler->chunk.chunk_mem, handler->chunk.chunk_mem_len);
	memset(&ptr[handler->chunk.chunk_mem_len], handler->default_unknown_value, to_add);
	handler->chunk.num_hash_indexes += CHUNK_GROWTH;
	handler->chunk.chunk_mem = ptr, handler->chunk.chunk_mem_len = new_len;

	traceEvent(TRACE_INFO, "Grown table to %u elements", handler->chunk.num_hash_indexes);

	goto redo_getOffset;
      } else {
	traceEvent(TRACE_WARNING, "Not enough memory (%u bytes): unable to grow table", new_len);
	return(-2);
      }
    }
  }

  /* All hashes of one day are one attached to the other: fast insert, slow data extraction */
  *offset = handler->values_len * hash_index;

  if(*offset >= handler->chunk.chunk_mem_len)
    traceEvent(TRACE_ERROR, "INTERNAL ERROR [Id: %s/%u][Offset: %u/%u]",
	       hash_name, hash_index, *offset, handler->chunk.chunk_mem_len);

  return(0);
}

/* *********************************************************************** */

int tsdb_set(tsdb_handler *handler,
	     char *hash_index,
	     tsdb_value *value_to_store) {
  u_int32_t *value;
  u_int64_t offset;
  int rc;

  if(!handler->alive_and_kicking)
    return(-1);

  if((!handler->chunk.load_page_on_demand)
     && (handler->chunk.chunk_mem == NULL)) {
    traceEvent(TRACE_ERROR, "Missing epoch");
    return(-2);
  }

  if((rc = getOffset(handler, hash_index, &offset, 1)) == 0) {
    int fragment_id = offset/CHUNK_GROWTH;

    value = (tsdb_value*)(&handler->chunk.chunk_mem[offset]);
    memcpy(value, value_to_store, handler->values_len);

    /* Mark a fragment as changed */

    if(fragment_id > MAX_NUM_FRAGMENTS)
      traceEvent(TRACE_ERROR, "Internal error [%u > %u]", fragment_id, MAX_NUM_FRAGMENTS);
    else
      handler->chunk.fragment_changed[fragment_id] = 1;
  }

  return(rc);
}

/* *********************************************************************** */

int tsdb_get(tsdb_handler *handler,
	      char *hash_index,
	      tsdb_value **value_to_read) {
  u_int64_t offset;
  int rc;

  *value_to_read = &handler->default_unknown_value;

  if(!handler->alive_and_kicking)
    return(-1);

  if((!handler->chunk.load_page_on_demand)
     && (handler->chunk.chunk_mem == NULL)) {
    traceEvent(TRACE_ERROR, "Missing epoch");
    return(-2);
  }

  if((rc = getOffset(handler, hash_index, &offset, 0)) == 0) {
    *value_to_read = (tsdb_value*)(handler->chunk.chunk_mem + offset);
  }

  return(rc);
}
