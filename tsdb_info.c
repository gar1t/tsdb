#include "tsdb_api.h"

typedef struct {
    char *file;
} info_args;

static void help(int code) {
    printf("tsdb-info FILE\n");
    exit(code);
}

static void process_args(int argc, char *argv[], info_args *args) {
    int c;
    while ((c = getopt(argc, argv, "h")) != -1) {
        switch (c) {
        case 'h':
            help(0);
            break;
        default:
            help(1);
        }
    }

    int remaining = argc - optind;
    if (remaining != 1) {
        help(1);
    }
    args->file = argv[optind];
}

static void check_file_exists(const char *path) {
    FILE *file;
    if ((file = fopen(path, "r"))) {
        fclose(file);
    } else {
        printf("tsdb-info: %s doesn't exist\n", path);
        exit(1);
    }
}

static void print_db_info(char *file) {
    tsdb_handler db;
    int rc;
    u_int16_t unused16 = 0;
    u_int32_t unused32 = 0;
    struct stat info;

    rc = tsdb_open(file, &db, &unused16, unused32, 1);
    if (rc) {
        printf("tsdb-info: error opening %s\n", file);
        exit(1);
    }

    rc = stat(file, &info);
    if (rc) {
        printf("tsdb-info: error getting file info for %s\n", file);
        exit(1);
    }

    printf("          File: %s\n", file);
    printf("          Size: %zd\n", info.st_size);
    printf("Vals Per Entry: %u\n", db.values_per_entry);
    printf("  Slot Seconds: %u\n", db.slot_duration);;
    tsdb_close(&db);
}

int main(int argc, char *argv[]) {
    info_args args;

    set_trace_level(0);
    process_args(argc, argv, &args);
    check_file_exists(args.file);
    print_db_info(args.file);

    return 0;
}
