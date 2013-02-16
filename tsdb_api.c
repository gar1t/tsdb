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

static void db_put(tsdb_handler *handler,
                   void *key, u_int32_t key_len,
                   void *value, u_int32_t value_len) {
    DBT key_data, data;

    if (handler->read_only) {
        trace_warning("Unable to set value (read-only mode)");
        return;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = key;
    key_data.size = key_len;
    data.data = value;
    data.size = value_len;

    if (handler->db->put(handler->db, NULL, &key_data, &data, 0) != 0) {
        trace_warning("Error while map_set(%u, %u)", key, value);
    }
}

static int db_get(tsdb_handler *handler,
                  void *key, u_int32_t key_len,
                  void **value, u_int32_t *value_len) {
    DBT key_data, data;

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = key;
    key_data.size = key_len;

    if (handler->db->get(handler->db, NULL, &key_data, &data, 0) == 0) {
        *value = data.data, *value_len = data.size;
        return 0;
    } else {
        return -1;
    }
}

int tsdb_open(char *tsdb_path, tsdb_handler *handler,
	      u_int16_t *values_per_entry,
	      u_int32_t slot_duration,
	      u_int8_t read_only) {
    void *value;
    u_int32_t value_len;
    int ret, mode;

    memset(handler, 0, sizeof(tsdb_handler));

    handler->read_only = read_only;

    if ((ret = db_create(&handler->db, NULL, 0)) != 0) {
        trace_error("Error while creating DB handler [%s]", db_strerror(ret));
        return -1;
    }

    mode = (read_only ? 00444 : 00664 );

    if ((ret = handler->db->open(handler->db,
                                 NULL,
                                 (const char*)tsdb_path,
                                 NULL,
                                 DB_BTREE,
                                 (read_only ? 0 : DB_CREATE),
                                 mode)) != 0) {
        trace_error("Error while opening DB %s [%s][r/o=%u,mode=%o]",
                    tsdb_path, db_strerror(ret), read_only, mode);
        return -1;
    }

    if (db_get(handler, "lowest_free_index",
               strlen("lowest_free_index"),
               &value, &value_len) == 0) {
        handler->lowest_free_index = *((u_int32_t*)value);
    } else {
        if (!handler->read_only) {
            handler->lowest_free_index = 0;
            db_put(handler, "lowest_free_index",
                   strlen("lowest_free_index"),
                   &handler->lowest_free_index,
                   sizeof(handler->lowest_free_index));
        }
    }

    if (db_get(handler, "rrd_slot_time_duration",
               strlen("rrd_slot_time_duration"),
               &value, &value_len) == 0) {
        handler->slot_duration = *((u_int32_t*)value);
    } else {
        if (!handler->read_only) {
            handler->slot_duration = slot_duration;
            db_put(handler, "slot_duration",
                   strlen("slot_duration"),
                   &handler->slot_duration,
                   sizeof(handler->slot_duration));
        }
    }

    if (db_get(handler, "values_per_entry",
               strlen("values_per_entry"),
               &value, &value_len) == 0) {
        *values_per_entry = handler->values_per_entry =
            *((u_int16_t*)value);
    } else {
        if (!handler->read_only) {
            handler->values_per_entry = *values_per_entry;
            db_put(handler, "values_per_entry",
                   strlen("values_per_entry"),
                   &handler->values_per_entry,
                   sizeof(handler->values_per_entry));
        }
    }

    handler->values_len = handler->values_per_entry * sizeof(tsdb_value);

    trace_info("lowest_free_index: %u", handler->lowest_free_index);
    trace_info("slot_duration: %u", handler->slot_duration);
    trace_info("values_per_entry: %u", handler->values_per_entry);

    memset(&handler->state_compress, 0, sizeof(handler->state_compress));
    memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

    handler->alive = 1;

    return 0;
}

static void tsdb_flush_chunk(tsdb_handler *handler) {
    char *compressed;
    u_int compressed_len, new_len, num_fragments, i;
    u_int fragment_size;
    char str[32];

    if (!handler->chunk.chunk_mem) return;

    fragment_size = handler->values_len * CHUNK_GROWTH;
    new_len = handler->chunk.chunk_mem_len + CHUNK_LEN_PADDING;
    compressed = (char*)malloc(new_len);
    if (!compressed) {
        trace_warning("Not enough memory (%u bytes)", new_len);
        return;
    }

    // Split chunks on the DB
    num_fragments = handler->chunk.chunk_mem_len / fragment_size;

    for (i=0; i < num_fragments; i++) {
        u_int offset;

        if ((!handler->read_only) && handler->chunk.fragment_changed[i]) {
            offset = i * fragment_size;

            compressed_len = qlz_compress(&handler->chunk.chunk_mem[offset],
                                          compressed, fragment_size,
                                          &handler->state_compress);

            trace_info("Compression %u -> %u [fragment %u] [%.1f %%]",
                       fragment_size, compressed_len, i,
                       ((float)(compressed_len*100))/((float)fragment_size));

            snprintf(str, sizeof(str), "%u-%u", handler->chunk.begin_epoch, i);

            db_put(handler, str, strlen(str), compressed, compressed_len);
        } else {
            trace_info("Skipping fragment %u (unchanged)", i);
        }
    }

    free(compressed);
    free(handler->chunk.chunk_mem);
    memset(&handler->chunk, 0, sizeof(handler->chunk));
}

void tsdb_close(tsdb_handler *handler) {

    if (!handler->alive) {
        return;
    }

    tsdb_flush_chunk(handler);

    if (!handler->read_only) {
        trace_info("Flushing database changes...");
    }

    handler->db->close(handler->db, 0);

    handler->alive = 0;
}

void normalize_epoch(tsdb_handler *handler, u_int32_t *epoch) {
    *epoch -= *epoch % handler->slot_duration;
    *epoch += timezone - daylight * 3600;
}

static int get_key_index(tsdb_handler *handler, char *key, u_int32_t *index) {
    void *ptr;
    u_int32_t len;
    char str[32] = { 0 };

    snprintf(str, sizeof(str), "key-%s", key);

    if (db_get(handler, str, strlen(str), &ptr, &len) == 0) {
        *index = *(u_int32_t*)ptr;
        return 0;
    }

    return -1;
}

static void set_key_index(tsdb_handler *handler, char *key, u_int32_t index) {
    char str[32];

    snprintf(str, sizeof(str), "key-%s", key);

    db_put(handler, str, strlen(str), &index, sizeof(index));

    trace_info("[SET] Mapping %s -> %u", key, index);
}

int tsdb_goto_epoch(tsdb_handler *handler,
                    u_int32_t epoch_value,
                    u_int8_t create_if_needed,
                    u_int8_t growable,
                    u_int8_t load_page_on_demand) {
    int rc;
    void *value;
    u_int32_t value_len, fragment_id = 0;
    char str[32];

    // Flush ond chunk if loaded
    tsdb_flush_chunk(handler);

    normalize_epoch(handler, &epoch_value);
    handler->chunk.load_page_on_demand = load_page_on_demand;
    handler->chunk.load_epoch = epoch_value;

    if (handler->chunk.load_page_on_demand) {
        return 0;
    }

    snprintf(str, sizeof(str), "%u-%u", epoch_value, fragment_id);
    rc = db_get(handler, str, strlen(str), &value, &value_len);

    if (rc == -1) {
        if (!create_if_needed) {
            trace_info("Unable to goto epoch %u", epoch_value);
            return -1;
        }

        trace_info("Creating epoch %u", epoch_value);

        handler->chunk.begin_epoch = epoch_value;
        u_int32_t mem_len = handler->values_len * CHUNK_GROWTH;
        handler->chunk.chunk_mem_len = mem_len;
        handler->chunk.chunk_mem = (u_int8_t*)malloc(mem_len);
        if (handler->chunk.chunk_mem == NULL) {
            trace_warning("Not enough memory (%u bytes)", mem_len);
            return -2;
        }

        memset(handler->chunk.chunk_mem,
               handler->unknown_value,
               handler->chunk.chunk_mem_len);
    } else {
        u_int32_t len, offset = 0;
        u_int8_t *ptr;

        trace_info("Loading epoch %u", epoch_value);

        fragment_id = 0;
        handler->chunk.chunk_mem_len = 0;

        while (1) {
            len = qlz_size_decompressed(value);

            ptr = (u_int8_t*)malloc(handler->chunk.chunk_mem_len + len);
            if (ptr == NULL) {
                trace_warning("Not enough memory (%u bytes)",
                              handler->chunk.chunk_mem_len+len);
                free(value);
                return -2;
            }

            if (handler->chunk.chunk_mem_len > 0) {
                memcpy(ptr, handler->chunk.chunk_mem,
                       handler->chunk.chunk_mem_len);
            }

            len = qlz_decompress(value, &ptr[offset],
                                 &handler->state_decompress);

            trace_info("Decompression %u -> %u [fragment %u] [%.1f %%]",
                       value_len, len, fragment_id,
                       ((float)(len*100))/((float)value_len));

            handler->chunk.chunk_mem_len += len;
            fragment_id++, offset = handler->chunk.chunk_mem_len;

            free(handler->chunk.chunk_mem);
            handler->chunk.chunk_mem = ptr;

            snprintf(str, sizeof(str), "%u-%u", epoch_value, fragment_id);
            if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
                break; // No more fragments
            }
        }

        handler->chunk.begin_epoch = epoch_value;
    }

    handler->chunk.growable = growable;

    return 0;
}

static int ensure_key_index(tsdb_handler *handler, char *key,
                            u_int32_t *index, u_int8_t for_write) {
    if (get_key_index(handler, key, index) == 0) {
        trace_info("Index %s mapped to hash %u", key, *index);
        return 0;
    }

    if (!for_write) {
        trace_info("Unable to find index %s", key);
        return -1;
    }

    *index = handler->lowest_free_index++;
    set_key_index(handler, key, *index);

    db_put(handler,
           "lowest_free_index", strlen("lowest_free_index"),
           &handler->lowest_free_index,
           sizeof(handler->lowest_free_index));

    return 0;
}

static int prepare_read_write(tsdb_handler *handler, char *key,
                              u_int64_t *offset, u_int8_t for_write) {
    u_int32_t index;

    if (ensure_key_index(handler, key, &index, for_write) == -1) {
        trace_info("Unable to find index %s", key);
        return -1;
    }

    trace_info("%s mapped to idx %u", key, index);

    if (handler->chunk.load_page_on_demand || handler->read_only) {
        u_int32_t fragment_id = index / CHUNK_GROWTH, value_len;
        char str[32];
        void *value;

        // We need to load the epoch handler->chunk.load_epoch/fragment_id

        snprintf(str, sizeof(str), "%u-%u", handler->chunk.load_epoch,
                 fragment_id);
        if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
            return -1;
        }

        handler->chunk.chunk_mem_len = qlz_size_decompressed(value);

        handler->chunk.chunk_mem =
            (u_int8_t*)malloc(handler->chunk.chunk_mem_len);
        if (handler->chunk.chunk_mem == NULL) {
            trace_warning("Not enough memory (%u bytes)",
                          handler->chunk.chunk_mem_len);
            return -2;
        }

        qlz_decompress(value, handler->chunk.chunk_mem,
                       &handler->state_decompress);

        handler->chunk.begin_epoch = handler->chunk.load_epoch;
        handler->chunk.base_index = fragment_id * CHUNK_GROWTH;

        // Shift index
        index -= handler->chunk.base_index;
    }

 get_offset:

    if (index >= (handler->chunk.chunk_mem_len / handler->values_len)) {
        if (!handler->chunk.growable) {
            trace_error("Index %u out of range", index);
            return -1;
        }

        u_int32_t to_add = CHUNK_GROWTH * handler->values_len;
        u_int32_t new_len = handler->chunk.chunk_mem_len + to_add;
        u_int8_t *ptr = malloc(new_len);

        if (!ptr) {
            trace_warning("Not enough memory (%u bytes): unable to grow "
                          "table", new_len);
            return -2;
        }

        memcpy(ptr, handler->chunk.chunk_mem, handler->chunk.chunk_mem_len);
        memset(&ptr[handler->chunk.chunk_mem_len],
               handler->unknown_value, to_add);
        handler->chunk.chunk_mem = ptr;
        handler->chunk.chunk_mem_len = new_len;

        trace_warning("Epoch grown to %u", new_len);

        goto get_offset;
    }

    *offset = handler->values_len * index;

    if (*offset >= handler->chunk.chunk_mem_len) {
        trace_error("INTERNAL ERROR [Id: %s/%u][Offset: %u/%u]",
                    key, index, *offset,
                    handler->chunk.chunk_mem_len);
    }

    return 0;
}

int tsdb_set(tsdb_handler *handler, char *key, tsdb_value *value) {
    u_int32_t *chunk_ptr;
    u_int64_t offset;
    int rc;

    if (!handler->alive) {
        return -1;
    }

    if ((!handler->chunk.load_page_on_demand)
        && (handler->chunk.chunk_mem == NULL)) {
        trace_error("Missing epoch");
        return -2;
    }

    rc = prepare_read_write(handler, key, &offset, 1);
    if (rc == 0) {
        chunk_ptr = (tsdb_value*)(&handler->chunk.chunk_mem[offset]);
        memcpy(chunk_ptr, value, handler->values_len);

        // Mark a fragment as changed
        int fragment_id = offset / CHUNK_GROWTH;
        if (fragment_id > MAX_NUM_FRAGMENTS) {
            trace_error("Internal error [%u > %u]",
                        fragment_id, MAX_NUM_FRAGMENTS);
        } else {
            handler->chunk.fragment_changed[fragment_id] = 1;
        }
    }

    return rc;
}

int tsdb_get(tsdb_handler *handler, char *key, tsdb_value **value) {
    u_int64_t offset;
    int rc;

    *value = &handler->unknown_value;

    if (!handler->alive) {
        return -1;
    }

    if ((!handler->chunk.load_page_on_demand)
        && (handler->chunk.chunk_mem == NULL)) {
        trace_error("Missing epoch");
        return -2;
    }

    rc = prepare_read_write(handler, key, &offset, 0);
    if (rc == 0) {
        *value = (tsdb_value*)(handler->chunk.chunk_mem + offset);
    }

    return rc ;
}
