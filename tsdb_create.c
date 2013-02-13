#include "tsdb_api.h"

typedef struct {
    char *file;
    u_int32_t slot_seconds;
    u_int16_t values_per_entry;
    int verbose;
} create_args;

static void init_trace(int verbose) {
    set_trace_level(verbose ? 99 : 0);
}

static void help(int code) {
    printf("tsdb-create [-v] file slot_seconds [values_per_entry]\n");
    exit(code);
}

static void check_file_exists(const char *path) {
    FILE *file;
    if ((file = fopen(path, "r"))) {
        fclose(file);
        printf("tsdb-create: %s already exists\n", path);
        exit(1);
    }
}

static u_int32_t str_to_uint32(const char *str, const char *argname) {
    uint num = 0;
    if (sscanf(str, "%u", &num) == EOF) {
        printf("tsdb-create: invalid value for %s\n", argname);
        exit(1);
    }
    return num;
}

static void process_create_args(int argc, char *argv[], create_args *args) {
    int c;

    args->verbose = 0;

    while ((c = getopt(argc, argv, "hv")) != -1) {
        switch (c) {
        case 'h':
            help(0);
            break;
        case 'v':
            args->verbose = 1;
            break;
        default:
            help(1);
        }
    }

    int remaining = argc - optind;
    if (remaining < 2 || remaining > 3) {
        help(1);
    }
    args->file = argv[optind];
    args->slot_seconds = str_to_uint32(argv[optind + 1], "slot_seconds");
    if (remaining == 3) {
        args->values_per_entry =
            str_to_uint32(argv[optind + 2], "values_per_entry");
    } else {
        args->values_per_entry = 1;
    }
}

static void validate_slot_seconds(u_int32_t slot_seconds) {
    if (slot_seconds == 0) {
        printf("tsdb-create: invalid value for slot_seconds\n");
        exit(1);
    }
}

static void validate_values_per_entry(u_int16_t values_per_entry) {
    if (values_per_entry == 0) {
        printf("tsdb-create: invalid value for values_per_entry\n");
        exit(1);
    }
}

static void create_db(char *file,
                      u_int32_t slot_seconds,
                      u_int16_t values_per_entry) {
    tsdb_handler handler;
    int rc;
    rc = tsdb_open(file, &handler, &values_per_entry, slot_seconds, 0);
    if (rc) {
        printf("tsdb-create: error creating database\n");
        exit(1);
    }
    tsdb_close(&handler);
}

int main(int argc, char *argv[]) {

    create_args args;

    process_create_args(argc, argv, &args);
    init_trace(args.verbose);
    check_file_exists(args.file);
    validate_slot_seconds(args.slot_seconds);
    validate_values_per_entry(args.values_per_entry);
    create_db(args.file, args.slot_seconds, args.values_per_entry);

    return 0;
}
