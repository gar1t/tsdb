#include "test_core.h"

static char *get_file_arg(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: test-simple TEST-DB\n");
        exit(1);
    }
    char *file = argv[1];
    if (file_exists(file)) {
        fprintf(stderr, "%s exists\n", file);
        exit(1);
    }
    return file;
}

#define num_epochs 100
#define slot_seconds 60
#define num_keys 1000

int main(int argc, char *argv[]) {

    char *file = get_file_arg(argc, argv);
    set_trace_level(0);

    tsdb_handler db;
    int ret;
    int cur, start, stop;
    char key[255];
    uint i;
    uint write_val;
    uint *read_val;

    // Open (create) a new db.

    u_int16_t vals_per_entry = 1;
    ret = tsdb_open(file, &db, &vals_per_entry, slot_seconds, 0);
    assert_int_equal(0, ret);

    // Move through epochs for write.

    start = 1000000000;
    stop = start + (num_epochs - 1) * slot_seconds;

    for (cur = start; cur <= stop; cur += slot_seconds) {

        ret = tsdb_goto_epoch(&db, cur, 1, 1, 0);
        assert_int_equal(0, ret);

        // Write keys.

        for (i = 1; i <= num_keys; i++) {
            sprintf(key, "key-%i", i);
            write_val = i * 1000;
            ret = tsdb_set(&db, key, &write_val);
            assert_int_equal(0, ret);
        }
    }

    // Move through epochs for reads.

    for (cur = start; cur <= stop; cur += slot_seconds) {

        ret = tsdb_goto_epoch(&db, cur, 1, 1, 0);
        assert_int_equal(0, ret);

        // Read keys.

        for (i = 1; i <= num_keys; i++) {
            sprintf(key, "key-%i", i);
            ret = tsdb_get(&db, key, &read_val);
            assert_int_equal(0, ret);
            write_val = i * 1000;
            assert_int_equal(write_val, *read_val);
        }
    }

    tsdb_close(&db);

    return 0;
}
