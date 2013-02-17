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
    //tsdb_value *read_val;

    // Open (create) a new db.

    u_int16_t vals_per_entry = 1;
    ret = tsdb_open(file, &db, &vals_per_entry, 60, 0);
    assert_int_equal(0, ret);

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

    // Let's get the new tag.
    //
    tsdb_tag blue;
    ret = tsdb_get_tag(&db, "blue", &blue);
    assert_int_equal(0, ret);

    // Let's use the tag to test key membership.
    //
    ret = tsdb_is_tag_key(&db, &blue, "key-1");
    assert_int_equal(1, ret);

    // key-2 doesn't exist at all, so we don't expect it to be tagged.
    //
    ret = tsdb_is_tag_key(&db, &blue, "key-2");
    assert_int_equal(0, ret);

    // Let's add key-2, but for a different tag.
    //
    write_val = 222;
    ret = tsdb_set(&db, "key-2", &write_val);
    assert_int_equal(0, ret);
    ret = tsdb_tag_key(&db, "key-2", "red");
    assert_int_equal(0, ret);
    
    // And test association with our first tag.
    //
    ret = tsdb_is_tag_key(&db, &blue, "key-2");
    assert_int_equal(0, ret);

    // And tag our second key using the first tag.
    //
    ret = tsdb_tag_key(&db, "key-2", "blue");
    assert_int_equal(0, ret);

    // If we test association with the first tag, we'll continue to
    // get false until we re-read the tag.
    //
    ret = tsdb_is_tag_key(&db, &blue, "key-2");
    assert_int_equal(0, ret);

    // Let's re-read the tag and check again.
    //
    // Note we free the tag array before reusing the struct -- if we
    // didn't do this, we'd lose that memory.
    //
    // TODO: Is this the right pattern? Should tsdb_get_tag complain
    // when is sees something in array? Should we provide a tsdb_free_tag
    // function that fees the memory and sets the array to 0? I think so.
    //
    free(blue.array);
    ret = tsdb_get_tag(&db, "blue", &blue);
    assert_int_equal(0, ret);
    ret = tsdb_is_tag_key(&db, &blue, "key-2");
    assert_int_equal(1, ret);

    tsdb_close(&db);

    return 0;
}
