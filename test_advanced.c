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
    //uint write_val;
    //uint *read_val;

    // Open (create) a new db.

    u_int16_t vals_per_entry = 2;
    ret = tsdb_open(file, &db, &vals_per_entry, slot_seconds, 0);
    assert_int_equal(0, ret);

    // Move to an epoch that isn't there and indicate we want to fail.
    //
    ret = tsdb_goto_epoch(&db, 60, 0, 0, 0);
    assert_int_equal(-1, ret);

    tsdb_close(&db);

    return 0;
}
