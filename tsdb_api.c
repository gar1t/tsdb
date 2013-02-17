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
#include "tsdb_bitmap.h"

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
        trace_error("Error while map_set(%u, %u)", key, value);
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

    if (db_get(handler, "slot_duration",
               strlen("slot_duration"),
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

    if (!handler->chunk.data) return;

    fragment_size = handler->values_len * CHUNK_GROWTH;
    new_len = handler->chunk.data_len + CHUNK_LEN_PADDING;
    compressed = (char*)malloc(new_len);
    if (!compressed) {
        trace_error("Not enough memory (%u bytes)", new_len);
        return;
    }

    // Split chunks on the DB
    num_fragments = handler->chunk.data_len / fragment_size;

    for (i=0; i < num_fragments; i++) {
        u_int offset;

        if ((!handler->read_only) && handler->chunk.fragment_changed[i]) {
            offset = i * fragment_size;

            compressed_len = qlz_compress(&handler->chunk.data[offset],
                                          compressed, fragment_size,
                                          &handler->state_compress);

            trace_info("Compression %u -> %u [fragment %u] [%.1f %%]",
                       fragment_size, compressed_len, i,
                       ((float)(compressed_len*100))/((float)fragment_size));

            snprintf(str, sizeof(str), "%u-%u", handler->chunk.epoch, i);

            db_put(handler, str, strlen(str), compressed, compressed_len);
        } else {
            trace_info("Skipping fragment %u (unchanged)", i);
        }
    }

    free(compressed);
    free(handler->chunk.data);
    memset(&handler->chunk, 0, sizeof(handler->chunk));
    handler->chunk.epoch = 0;
    handler->chunk.data_len = 0;
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

int tsdb_get_key_index(tsdb_handler *handler, char *key, u_int32_t *index) {
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
                    u_int32_t epoch,
                    u_int8_t fail_if_missing,
                    u_int8_t growable) {
    int rc;
    void *value;
    u_int32_t value_len, fragment = 0;
    char str[32];

    if (handler->chunk.epoch == epoch) {
        return 0;
    }

    tsdb_flush_chunk(handler);

    normalize_epoch(handler, &epoch);
    snprintf(str, sizeof(str), "%u-%u", epoch, fragment);

    rc = db_get(handler, str, strlen(str), &value, &value_len);

    if (rc == -1 && fail_if_missing) {
        return -1;
    }

    handler->chunk.epoch = epoch;
    handler->chunk.growable = growable;

    if (rc == 0) {
        u_int32_t len, offset = 0;
        u_int8_t *ptr;

        trace_info("Loading epoch %u", epoch);

        while (1) {
            len = qlz_size_decompressed(value);
            ptr = (u_int8_t*)malloc(handler->chunk.data_len + len);
            if (ptr == NULL) {
                trace_error("Not enough memory (%u bytes)",
                              handler->chunk.data_len+len);
                free(value);
                return -2;
            }
            if (handler->chunk.data_len > 0) {
                memcpy(ptr, handler->chunk.data, handler->chunk.data_len);
            }
            len = qlz_decompress(value, &ptr[offset],
                                 &handler->state_decompress);

            trace_info("Decompression %u -> %u [fragment %u] [%.1f %%]",
                       value_len, len, fragment,
                       ((float)(len*100))/((float)value_len));

            handler->chunk.data_len += len;
            fragment++;
            offset = handler->chunk.data_len;

            free(handler->chunk.data);
            handler->chunk.data = ptr;

            snprintf(str, sizeof(str), "%u-%u", epoch, fragment);
            if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
                break; // No more fragments
            }
        }
    }

    return 0;
}

static int ensure_key_index(tsdb_handler *handler, char *key,
                            u_int32_t *index, u_int8_t for_write) {
    if (tsdb_get_key_index(handler, key, index) == 0) {
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

static int prepare_offset_by_index(tsdb_handler *handler, u_int32_t *index,
                                   u_int64_t *offset, u_int8_t for_write) {

    if (!handler->chunk.data) {
        if (!for_write) {
            return -1;
        }

        u_int32_t fragment = *index / CHUNK_GROWTH, value_len;
        char str[32];
        void *value;

        // Load the epoch handler->chunk.epoch/fragment

        snprintf(str, sizeof(str), "%u-%u", handler->chunk.epoch, fragment);
        if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
            u_int32_t mem_len = handler->values_len * CHUNK_GROWTH;
            handler->chunk.data_len = mem_len;
            handler->chunk.data = (u_int8_t*)malloc(mem_len);
            if (handler->chunk.data == NULL) {
                trace_error("Not enough memory (%u bytes)", mem_len);
                return -2;
            }
            memset(handler->chunk.data,
                   handler->unknown_value,
                   handler->chunk.data_len);
        } else {
            handler->chunk.data_len = qlz_size_decompressed(value);
            handler->chunk.data = (u_int8_t*)malloc(handler->chunk.data_len);
            if (handler->chunk.data == NULL) {
                trace_error("Not enough memory (%u bytes)",
                            handler->chunk.data_len);
                return -2;
            }
            qlz_decompress(value, handler->chunk.data,
                           &handler->state_decompress);
        }
        handler->chunk.base_index = fragment * CHUNK_GROWTH;
        index -= handler->chunk.base_index;
    }

 get_offset:

    if (*index >= (handler->chunk.data_len / handler->values_len)) {
        if (!for_write || !handler->chunk.growable) {
            return -1;
        }

        u_int32_t to_add = CHUNK_GROWTH * handler->values_len;
        u_int32_t new_len = handler->chunk.data_len + to_add;
        u_int8_t *ptr = malloc(new_len);

        if (!ptr) {
            trace_error("Not enough memory (%u bytes): unable to grow "
                        "table", new_len);
            return -2;
        }

        memcpy(ptr, handler->chunk.data, handler->chunk.data_len);
        memset(&ptr[handler->chunk.data_len],
               handler->unknown_value, to_add);
        handler->chunk.data = ptr;
        handler->chunk.data_len = new_len;

        trace_warning("Epoch grown to %u", new_len);

        goto get_offset;
    }

    *offset = handler->values_len * *index;

    if (*offset >= handler->chunk.data_len) {
        trace_error("INTERNAL ERROR [Id: %u][Offset: %u/%u]",
                    *index, *offset, handler->chunk.data_len);
    }

    return 0;
}

static int prepare_offset_by_key(tsdb_handler *handler, char *key,
                                 u_int64_t *offset, u_int8_t for_write) {
    u_int32_t index;

    if (!handler->chunk.epoch) {
        return -1;
    }

    if (ensure_key_index(handler, key, &index, for_write) == -1) {
        trace_info("Unable to find index %s", key);
        return -1;
    }

    trace_info("%s mapped to idx %u", key, index);

    return prepare_offset_by_index(handler, &index, offset, for_write);
}

int tsdb_set_with_index(tsdb_handler *handler, char *key,
                        tsdb_value *value, u_int32_t *index) {
    u_int32_t *chunk_ptr;
    u_int64_t offset;
    int rc;

    if (!handler->alive) {
        return -1;
    }

    if (!handler->chunk.epoch) {
        trace_error("Missing epoch");
        return -2;
    }

    rc = prepare_offset_by_key(handler, key, &offset, 1);
    if (rc == 0) {
        chunk_ptr = (tsdb_value*)(&handler->chunk.data[offset]);
        memcpy(chunk_ptr, value, handler->values_len);

        // Mark a fragment as changed
        int fragment = offset / CHUNK_GROWTH;
        if (fragment > MAX_NUM_FRAGMENTS) {
            trace_error("Internal error [%u > %u]",
                        fragment, MAX_NUM_FRAGMENTS);
        } else {
            handler->chunk.fragment_changed[fragment] = 1;
        }

        *index = offset / handler->values_len;
    }

    return rc;
}

int tsdb_set(tsdb_handler *handler, char *key, tsdb_value *value) {
    u_int32_t index;
    return tsdb_set_with_index(handler, key, value, &index);
}

int tsdb_get_by_key(tsdb_handler *handler, char *key, tsdb_value **value) {
    u_int64_t offset;
    int rc;

    if (!handler->alive || !handler->chunk.data) {
        return -1;
    }

    rc = prepare_offset_by_key(handler, key, &offset, 0);
    if (rc == 0) {
        *value = (tsdb_value*)(handler->chunk.data + offset);
    }

    return rc ;
}

int tsdb_get_by_index(tsdb_handler *handler, u_int32_t *index,
                      tsdb_value **value) {
    u_int64_t offset;
    int rc;

    if (!handler->alive || !handler->chunk.data) {
        return -1;
    }

    rc = prepare_offset_by_index(handler, index, &offset, 0);
    if (rc == 0) {
        *value = (tsdb_value*)(handler->chunk.data + offset);
    }

    return rc ;
}

void tsdb_flush(tsdb_handler *handler) {
    if (!handler->alive || handler->read_only) {
        return;
    }
    trace_info("Flushing database changes");
    tsdb_flush_chunk(handler);
    handler->db->sync(handler->db, 0);
}

static int load_tag_array(tsdb_handler *handler, char *name,
                          tsdb_tag *tag) {
    void *ptr;
    u_int32_t len;
    char str[255] = { 0 };

    snprintf(str, sizeof(str), "tag-%s", name);

    if (db_get(handler, str, strlen(str), &ptr, &len) == 0) {
        u_int32_t *array;
        array = (u_int32_t*)malloc(len);
        if (array == NULL) {
            free(ptr);
            return -2;
        }
        memcpy(array, ptr, len);
        tag->array = array;
        tag->array_len = len;
        return 0;
    }

    return -1;
}

static int allocate_tag_array(tsdb_tag *tag) {
    u_int32_t array_len = CHUNK_GROWTH / sizeof(u_int32_t);

    u_int32_t* array = malloc(array_len);
    if (!array) {
        return -1;
    }

    memset(array, 0, array_len);

    tag->array = array;
    tag->array_len = array_len;

    return 0;
}

static void set_tag(tsdb_handler *handler, char *name, tsdb_tag *tag) {
    char str[255];

    snprintf(str, sizeof(str), "tag-%s", name);

    db_put(handler, str, strlen(str), tag->array, tag->array_len);
}

static int ensure_tag_array(tsdb_handler *handler, char *name, tsdb_tag *tag) {
    if (load_tag_array(handler, name, tag) == 0) {
        return 0;
    }

    if (allocate_tag_array(tag) == 0) {
        return 0;
    }

    return -1;
}

int tsdb_tag_key(tsdb_handler *handler, char *key, char *tag_name) {
    u_int32_t index;

    if (tsdb_get_key_index(handler, key, &index) == -1) {
        return -1;
    }

    tsdb_tag tag;
    if (ensure_tag_array(handler, tag_name, &tag)) {
        return -1;
    }

    set_bit(tag.array, index);
    set_tag(handler, tag_name, &tag);

    free(tag.array);

    return 0;
}

void scan_tag_indexes(tsdb_tag *tag, u_int32_t *indexes,
                      u_int32_t max_index, u_int32_t *count) {
    u_int32_t i, j, index;
    u_int32_t max_word = max_index / BITS_PER_WORD;

    *count = 0;

    for (i = 0; i <= max_word; i++) {
        if (tag->array[i] == 0) {
            continue;
        }
        for (j = 0; j < BITS_PER_WORD; j++) {
            index = i * BITS_PER_WORD + j;
            if (index > max_index) {
                break;
            }
            if (get_bit(tag->array, index)) {
                indexes[(*count)++] = index;
            }
        }
    }
}

static u_int32_t max_tag_index(tsdb_handler *handler, u_int32_t max_len) {
    if (handler->lowest_free_index < max_len) {
        return handler->lowest_free_index - 1;
    } else {
        return max_len - 1;
    }
}

int tsdb_get_tag_indexes(tsdb_handler *handler, char *tag_name,
                         u_int32_t *indexes, u_int32_t indexes_len,
                         u_int32_t *count) {
    tsdb_tag tag;
    if (load_tag_array(handler, tag_name, &tag) == 0) {
        u_int32_t max_index = max_tag_index(handler, indexes_len);
        scan_tag_indexes(&tag, indexes, max_index, count);
        free(tag.array);
        return 0;
    }

    return -1;
}

int tsdb_get_consolidated_tag_indexes(tsdb_handler *handler,
                                      char **tag_names,
                                      u_int16_t tag_names_len,
                                      int consolidator,
                                      u_int32_t *indexes,
                                      u_int32_t indexes_len,
                                      u_int32_t *count) {
    u_int32_t i, j, max_index;
    tsdb_tag consolidated, current;

    consolidated.array = NULL;
    consolidated.array_len = 0;
    max_index = max_tag_index(handler, indexes_len);

    *count = 0;

    for (i = 0; i < tag_names_len; i++) {
        if (load_tag_array(handler, tag_names[i], &current) == 0) {
            if (consolidated.array) {
                for (j = 0; j < max_index; j++) {
                    switch (consolidator) {
                    case TSDB_AND:
                        consolidated.array[j] &= current.array[j];
                        break;
                    case TSDB_OR:
                        consolidated.array[j] |= current.array[j];
                        break;
                    default:
                        consolidated.array[j] = current.array[j];
                    }
                }
            } else {
                consolidated.array = current.array;
                consolidated.array_len = current.array_len;
            }
        }
    }

    if (consolidated.array) {
        scan_tag_indexes(&consolidated, indexes, max_index, count);
        free(consolidated.array);
    }

    return 0;
}
