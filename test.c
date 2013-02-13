#include <sys/stat.h>

#include "tsdb_api.h"

static int file_exists(char *filename) {
    FILE *file;
    if ((file = fopen(filename, "r"))) {
        fclose(file);
        return 1;
    } else {
        return 0;
    }
}

static char *get_file_arg(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: test TEST-DB\n");
        exit(1);
    }
    char *file = argv[1];
    if (file_exists(file)) {
        fprintf(stderr, "%s exists\n", file);
        exit(1);
    }
    return file;
}

static void check(int result, char *msg) {
    if (result != 0) {
        fprintf(stderr, "Unexpected result from %s: %i", msg, result);
        exit(1);
    }
}

static void check_read_val(uint val, uint expected, char *key) {
    if (val != expected) {
        fprintf(stderr, "Got %u for %s, expected %u\n", val, key, expected);
        exit(1);
    }
}

#define num_epochs 10000
#define slot_seconds 60
#define num_keys 100

int main(int argc, char *argv[]) {

    char *file = get_file_arg(argc, argv);
    traceLevel = 0;

    tsdb_handler db;
    int ret;
    int cur, start, stop;
    char key[255];
    uint i;
    uint *read_val;

    // open/create db

    u_int16_t vals_per_entry = 1;
    ret = tsdb_open(file, &db, &vals_per_entry, slot_seconds, 0);
    check(ret, "tsdb_open");

    // move through epochs for writes

    start = 1000000000;
    stop = start + num_epochs * slot_seconds;

    for (cur = start; cur <= stop; cur += slot_seconds) {

        ret = tsdb_goto_epoch(&db, cur, 1, 1, 0);
        check(ret, "tsdb_goto_epoch");

        // write fields

        for (i = 1; i <= num_keys; i++) {
            sprintf(key, "key-%i", i);
            ret = tsdb_set(&db, key, &i);
            check(ret, "tsdb_set");
        }
    }

    // move through epochs for reads

    for (cur = start; cur <= stop; cur += slot_seconds) {

        ret = tsdb_goto_epoch(&db, cur, 1, 1, 0);
        check(ret, "tsdb_goto_epoch");

        // read fields

        for (i = 1; i <= num_keys; i++) {
            sprintf(key, "key-%i", i);
            ret = tsdb_get(&db, key, &read_val);
            check(ret, "tsdb_get");
            check_read_val(*read_val, i, key);
        }
    }

    tsdb_close(&db);

    return 0;
}
