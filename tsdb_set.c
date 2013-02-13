#include "tsdb_api.h"

typedef struct {
    char *file;
    char *key;
    u_int32_t timestamp;
    int values_start;
    int values_stop;
    char **argv;
    int verbose;
} set_args;

static void help(int code) {
    printf("tsdb-set [-v] file key [-t timestamp] [values]\n");
    exit(code);
}

static u_int32_t str_to_uint32(const char *str, const char *argname) {
    uint num = 0;
    if (sscanf(str, "%u", &num) == EOF) {
        printf("tsdb-set: invalid value for %s\n", argname);
        exit(1);
    }
    return num;
}

static void process_args(int argc, char *argv[], set_args *args) {
    int c;

    args->timestamp = 0;
    args->verbose = 0;

    while ((c = getopt(argc, argv, "hvt:")) != -1) {
        switch (c) {
        case 'h':
            help(0);
            break;
        case 'v':
            args->verbose = 1;
            break;
        case 't':
            args->timestamp = str_to_uint32(optarg, "timestamp");
            break;
        default:
            help(1);
        }
    }

    int remaining = argc - optind;
    if (remaining < 2) {
        help(1);
    }

    args->file = argv[optind];
    args->key = argv[optind + 1];
    args->values_start = optind + 2;
    args->values_stop = argc - 1;
    args->argv = argv;
}

static void check_file_exists(const char *path) {
    FILE *file;
    if ((file = fopen(path, "r"))) {
        fclose(file);
    } else {
        printf("tsdb-set: %s doesn't exist (use tsdb-create)\n", path);
        exit(1);
    }
}

static void open_db(char *file, tsdb_handler *db) {
    u_int16_t unused16 = 0;
    u_int32_t unused32 = 0;
    if (tsdb_open(file, db, &unused16, unused32, 0)) {
        printf("tsdb-set: error opening db %s\n", file);
        exit(1);
    }
}

static void goto_epoch(tsdb_handler *db, u_int32_t epoch) {
    if (tsdb_goto_epoch(db, epoch, 1, 1, 0) ) {
        printf("tsdb-set: error settting epoch\n");
        exit(1);
    }
}

static void set_values(tsdb_handler *db, char* key, tsdb_value *values) {
    if (tsdb_set(db, key, values)) {
        printf("tsdb-set: error setting value\n");
        exit(1);
    }
}

static tsdb_value *alloc_values(set_args *args, int value_count) {
    tsdb_value *values = calloc(value_count, sizeof(tsdb_value));
    int cur_value = args->values_start;
    int i = 0;
    while (i < value_count && cur_value <= args->values_stop) {
        values[i++] = str_to_uint32(args->argv[cur_value++], "value");
    }
    return values;
}

static void set_tsdb_values(set_args *args) {
    tsdb_handler db;
    tsdb_value *values;
    u_int32_t epoch = (args->timestamp ? args->timestamp : time(NULL));

    open_db(args->file, &db);
    values = alloc_values(args, db.num_values_per_entry);
    goto_epoch(&db, epoch);
    set_values(&db, args->key, values);
    free(values);
    tsdb_close(&db);
}

static void init_trace(int verbose) {
    traceLevel = verbose ? 99 : 0;
}

int main(int argc, char *argv[]) {
    set_args args;

    process_args(argc, argv, &args);
    init_trace(args.verbose);
    check_file_exists(args.file);
    set_tsdb_values(&args);

    return 0;
}
