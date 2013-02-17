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

int main(int argc, char *argv[]) {

    char *file = get_file_arg(argc, argv);
    set_trace_level(0);

    tsdb_handler db;
    int ret;
    tsdb_value write_val;
    tsdb_value *read_val;

    // Open (create) a new db.

    u_int16_t vals_per_entry = 1;
    ret = tsdb_open(file, &db, &vals_per_entry, 60, 0);
    assert_int_equal(0, ret);

    //===================================================================
    // Basic use
    //===================================================================

    // We can't add a tag for a key that doesn't exist.
    //
    ret = tsdb_tag_key(&db, "key-1", "test");
    assert_int_equal(-1, ret);

    // Let's add a key.
    //
    ret = tsdb_goto_epoch(&db, 60, 0, 0);
    assert_int_equal(0, ret);
    write_val = 111;
    ret = tsdb_set(&db, "key-1", &write_val);
    assert_int_equal(0, ret);

    // Now we can tag it.
    //
    ret = tsdb_tag_key(&db, "key-1", "blue");
    assert_int_equal(0, ret);

    // Let's add key-2, but for a different tag.
    //
    write_val = 222;
    ret = tsdb_set(&db, "key-2", &write_val);
    assert_int_equal(0, ret);
    ret = tsdb_tag_key(&db, "key-2", "red");
    assert_int_equal(0, ret);

    // And tag our second key using the first tag.
    //
    ret = tsdb_tag_key(&db, "key-2", "blue");
    assert_int_equal(0, ret);

    //===================================================================
    // Single tag queries
    //===================================================================

    // The primary purpose of a tag is to allow efficient lookup of
    // associated key indexes.
    //
    // Indexes for a tag can be collected using tsdb_get_tag_indexes.
    //
    u_int32_t matches[100];
    u_int32_t match_count;
    ret = tsdb_get_tag_indexes(&db, "blue", matches, 100, &match_count);
    assert_int_equal(0, ret);
    assert_int_equal(2, match_count);

    // We have two matches. Let's use the indexes to lookup values.
    //
    ret = tsdb_get_by_index(&db, &matches[0], &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(111, *read_val);
    ret = tsdb_get_by_index(&db, &matches[1], &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(222, *read_val);

    // Let's check for indexes tagged wth "red".
    //
    ret = tsdb_get_tag_indexes(&db, "red", matches, 100, &match_count);
    assert_int_equal(0, ret);
    assert_int_equal(1, match_count);

    // And their value.
    //
    ret = tsdb_get_by_index(&db, &matches[0], &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(222, *read_val);

    //===================================================================
    // Complex tag queries
    //===================================================================

    char *red_and_blue[2] = { "red", "blue" };

    ret = tsdb_get_consolidated_tag_indexes(&db, red_and_blue, 2, TSDB_AND,
                                            matches, 100, &match_count);
    assert_int_equal(0, ret);
    assert_int_equal(1, match_count);

    // And the value for the matched index.
    //
    ret = tsdb_get_by_index(&db, &matches[0], &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(222, *read_val);

    // Let's add a few more indexes with tags.
    //
    write_val = 333;
    ret = tsdb_set(&db, "key-3", &write_val);
    assert_int_equal(0, ret);
    tsdb_tag_key(&db, "key-3", "red");
    tsdb_tag_key(&db, "key-3", "blue");
    tsdb_tag_key(&db, "key-3", "green");

    write_val = 444;
    ret = tsdb_set(&db, "key-4", &write_val);
    assert_int_equal(0, ret);
    tsdb_tag_key(&db, "key-4", "red");
    tsdb_tag_key(&db, "key-4", "green");

    // Let's get indexes that are tagged with both blue and green.
    //
    char *blue_and_green[2] = { "blue", "green" };
    ret = tsdb_get_consolidated_tag_indexes(&db, blue_and_green, 2, TSDB_AND,
                                            matches, 100, &match_count);
    assert_int_equal(0, ret);
    assert_int_equal(1, match_count);
    ret = tsdb_get_by_index(&db, &matches[0], &read_val);
    assert_int_equal(0, ret);
    assert_int_equal(333, *read_val);

    // And now the indexes tagged with blue or green.
    //
    ret = tsdb_get_consolidated_tag_indexes(&db, blue_and_green, 2, TSDB_OR,
                                            matches, 100, &match_count);
    assert_int_equal(0, ret);
    assert_int_equal(4, match_count);
    tsdb_get_by_index(&db, &matches[0], &read_val);
    assert_int_equal(111, *read_val);
    tsdb_get_by_index(&db, &matches[1], &read_val);
    assert_int_equal(222, *read_val);
    tsdb_get_by_index(&db, &matches[2], &read_val);
    assert_int_equal(333, *read_val);
    tsdb_get_by_index(&db, &matches[3], &read_val);
    assert_int_equal(444, *read_val);

    tsdb_close(&db);

    return 0;
}
