#include "test_core.h"

static char *get_file_arg(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: test-advanced TEST-DB\n");
        exit(1);
    }
    char *file = argv[1];
    if (file_exists(file)) {
        fprintf(stderr, "%s exists\n", file);
        exit(1);
    }
    return file;
}

#define slot_seconds 1

int main(int argc, char *argv[]) {

    char *file = get_file_arg(argc, argv);
    set_trace_level(0);

    tsdb_handler db;
    int ret;
    tsdb_value write_val;
    tsdb_value *read_val;

    // Open (create) a new db.

    u_int16_t vals_per_entry = 2;
    ret = tsdb_open(file, &db, &vals_per_entry, slot_seconds, 0);
    assert_int_equal(0, ret);

    //===================================================================
    // Basic read / write
    //===================================================================

    // Move to an epoch that isn't there and indicate we want to fail.
    // This is useful when reading values -- there's no point in trying
    // a value if the epoch doesn't exist.
    //
    ret = tsdb_goto_epoch(&db, 60, 1, 0);
    assert_int_equal(-1, ret);

    // When we fail to goto an epoch, nothing is loaded (internal check).
    //
    assert_true(db.chunk.data == NULL);
    assert_int_equal(0, db.chunk.data_len);

    // Let's go to an non-existing epoch, but not fail.
    //
    ret = tsdb_goto_epoch(&db, 60, 0, 0);
    assert_int_equal(0, ret);

    // We still haven't loaded anything (nothing to load).
    //
    assert_true(db.chunk.data == NULL);
    assert_int_equal(0, db.chunk.data_len);

    // Next we'll try to read a key -- there's no epoch data, so it will
    // fail.
    //
    ret = tsdb_get_by_key(&db, "key-1", &read_val);
    assert_int_equal(-1, ret);

    // We still haven't loaded any data for the epoch.
    //
    assert_true(db.chunk.data == NULL);
    assert_int_equal(0, db.chunk.data_len);

    // Next we'll write a value.
    //
    write_val = 111;
    ret = tsdb_set(&db, "key-1", &write_val);
    assert_int_equal(0, ret);

    // And read it back.
    //
    ret = tsdb_get_by_key(&db, "key-1", &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(111, *read_val);

    // The database we created actually accepts two values -- we've only
    // written and read one so far. Let's specify two values on write.
    //
    tsdb_value wide_val[2] = { 111, 222 };
    ret = tsdb_set(&db, "key-2", wide_val);
    assert_int_equal(0, ret);

    // And read them back.
    //
    ret = tsdb_get_by_key(&db, "key-2", &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(111, read_val[0]);
    assert_int_equal(222, read_val[1]);

    // Now the epoch has data.
    //
    assert_true(db.chunk.data != NULL);
    assert_true(db.chunk.data_len > 0);

    // Let's move to another epoch and try some more tests.
    //
    ret = tsdb_goto_epoch(&db, 120, 0, 0);
    assert_int_equal(0, ret);
    assert_true(db.chunk.data == NULL);
    assert_int_equal(0, db.chunk.data_len);

    // Reading a key that doesn't exit.
    //
    ret = tsdb_get_by_key(&db, "key-1", &read_val);
    assert_int_equal(-1, ret);

    // Writing the key.
    //
    wide_val[0] = 22222;
    wide_val[1] = 33333;
    ret = tsdb_set(&db, "key-1", wide_val);
    assert_int_equal(0, ret);

    // Let's move back to the previous epoch, and indiate that we want to
    // fail if the epoch doesn't exist. Because we've already created it,
    // however, we should succeed.
    //
    ret = tsdb_goto_epoch(&db, 60, 1, 0);
    assert_int_equal(0, ret);

    // And read some values.
    //
    ret = tsdb_get_by_key(&db, "key-1", &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(111, *read_val);
    ret = tsdb_get_by_key(&db, "key-2", &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(111, read_val[0]);
    assert_int_equal(222, read_val[1]);

    // And back to the next epoch.
    //
    ret = tsdb_goto_epoch(&db, 120, 1, 0);
    assert_int_equal(0, ret);
    ret = tsdb_get_by_key(&db, "key-1", &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(22222, read_val[0]);
    assert_int_equal(33333, read_val[1]);

    //===================================================================
    // Epoch growth
    //===================================================================

    // tsdb initially allocates a chunk (array) to store keys. When
    // the chunk is full and additional space is needed because to
    // accommodate new keys, tsdb will grow the chunk size.
    //
    // Let's illustrate by filling up a chunk, measuring the chunk
    // size, and then adding new keys to verify that the chunk size
    // grows.
    //
    // We'll  move to the next epoch to start. We'll indicate that we
    // don't want to fail if the epoch is missing and that we want the
    // epoch to grow as needed to accommodate keys.
    //
    ret = tsdb_goto_epoch(&db, 180, 0, 1);
    assert_int_equal(0, ret);

    // As we'd expect, the epoch data is empty to start.
    //
    assert_int_equal(0, db.chunk.data_len);

    // Let's add one key so we can measure how much data is allocated for
    // a single chunk.
    //
    wide_val[0] = 444444;
    wide_val[1] = 555555;
    ret = tsdb_set(&db, "key-1", wide_val);
    assert_int_equal(0, ret);

    // Here's our initial chunk size and the corresponding number of keys
    // we can expect to fit into a single chunk.
    //
    uint chunk_size = db.chunk.data_len;
    uint keys_per_chunk = chunk_size / db.values_len;

    // Let's fill up the remaining chunk slots.
    //
    uint i;
    char key[32];
    for (i = 1; i < keys_per_chunk; i++) {
        sprintf(key, "key-%i", i + 1);
        ret = tsdb_set(&db, key, wide_val);
        assert_int_equal(0, ret);
    }

    // At this point our chunk data should be full, but we haven't yet
    // allocated more data.
    //
    assert_int_equal(chunk_size, db.chunk.data_len);

    // Let's add one more key -- this will cause more data to be allocated
    // for the chunk.
    //
    sprintf(key, "key-%i", i + 1);
    ret = tsdb_set(&db, key, wide_val);
    assert_int_equal(0, ret);
    assert_int_equal(chunk_size * 2, db.chunk.data_len);

    // And read the values back.
    //
    for (i = 0; i < keys_per_chunk; i++) {
        sprintf(key, "key-%i", i + 1);
        ret = tsdb_get_by_key(&db, key, &read_val);
        assert_int_equal(0, ret);
        assert_int_equal(444444, read_val[0]);
        assert_int_equal(555555, read_val[1]);
    }

    //===================================================================
    // Lookup by index
    //===================================================================

    // tsdb supports lookup by key, however, it also provides an
    // optimization that lets you retrieve a key's index and use it
    // to read values more efficiently from an epoch. This works because,
    // once assigned, a key is always associated with the same index
    // for the life of the tsdb database.
    //
    // This is useful when scanning a series of epochs for a one or more
    // keys -- rather than lookup the associated key index for each
    // epoch, the caller can save and reuse the applicable indexes.
    //
    // Let's retrieve the index for "key-1" (one of the keys we've written
    // to this database) and use it to read a value from the current
    // epoch.
    //
    u_int32_t index;
    ret = tsdb_get_key_index(&db, "key-1", &index);
    assert_int_equal(0, ret);
    ret = tsdb_get_by_index(&db, &index, &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(444444, read_val[0]);
    assert_int_equal(555555, read_val[1]);

    // There an alternative form of tsdb_set that updates an index
    // argument with the key's index. This can be used to save a
    // call to tsdb_get_key_index if a key's index is needed
    // immediately after a set.
    //
    wide_val[0] = 66666666;
    wide_val[1] = 77777777;
    ret = tsdb_set_with_index(&db, "key-1", wide_val, &index);
    assert_int_equal(0, ret);
    assert_true(index >= 0);

    // And as above, we can use the index to read the values back
    // directly.
    //
    ret = tsdb_get_by_index(&db, &index, &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(66666666, read_val[0]);
    assert_int_equal(77777777, read_val[1]);

    // We can read up to the maximum index (chunk slots - 1) but no
    // further. We've already expanded the chunk size, so can currently
    // read up to two chunk's worth of values using indexes.
    //
    index = (keys_per_chunk * 2) - 1;
    ret = tsdb_get_by_index(&db, &index, &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(0, read_val[0]);
    assert_int_equal(0, read_val[1]);

    // Reading passed the last item fails.
    //
    index++;
    ret = tsdb_get_by_index(&db, &index, &read_val);
    assert_int_equal(-1, ret);

    tsdb_close(&db);

    return 0;
}
