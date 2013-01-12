#include "tsdb_api.h"

typedef struct {
  char *file;
  char *key;
  u_int32_t start;
  u_int32_t end;
  int verbose;
} get_args;

static void help(int code) {
  printf("tsdb-get [-v] file key [-s start] [-e end]\n");
  exit(code);
}

static void check_strtol_error(int no_digits, long val, int err,
                               char *argname) {
  if (no_digits
      || (err == ERANGE && (val == LONG_MAX || val == LONG_MIN))
      || (err != 0 && val == 0)) {
    printf("tsdb-get: invalid value for %s\n", argname);
    exit(1);
  }
}

static int unit_seconds_val(char *units, char *argname) {
  if (*units == '\0') {
    return 0;
  } else if (strcmp(units, "s") == 0) {
    return 1;
  } else if (strcmp(units, "m") == 0) {
    return 60;
  } else if (strcmp(units, "h") == 0) {
    return 3600;
  } else if (strcmp(units, "d") == 0) {
    return 86400;
  } else {
    printf("tsdb-get: unknown unit for %s\n", argname);
    exit(1);
  }
}

static u_int32_t epoch_val(char *str, u_int32_t now, char *argname) {
  char *units;
  long numval;
  int unit_seconds;

  errno = 0;
  numval = strtol(str, &units, 10);
  check_strtol_error(str == units, numval, errno, argname);
  unit_seconds = unit_seconds_val(units, argname);
  if (unit_seconds == 0) {
    return numval;
  } else {
    return now + numval * unit_seconds;
  }
}

static void process_args(int argc, char *argv[], get_args *args) {
  int c;
  u_int32_t now = time(NULL);

  args->start = now;
  args->end = now;
  args->verbose = 0;

  while ((c = getopt(argc, argv, "hvs:e:")) != -1) {
    switch (c) {
    case 's':
      args->start = epoch_val(optarg, now, "start");
      break;
    case 'e':
      args->end = epoch_val(optarg, now, "end");
      break;
    case 'v':
      args->verbose = 1;
      break;
    case 'h':
      help(0);
      break;
    default:
      help(1);
    }
  }

  int remaining = argc - optind;
  if (remaining != 2) {
    help(1);
  }
  args->file = argv[optind];
  args->key = argv[optind + 1];
}

static void check_file_exists(const char *path) {
  FILE *file;
  if ((file = fopen(path, "r"))) {
    fclose(file);
  } else {
    printf("tsdb-get: %s doesn't exist\n", path);
    exit(1);
  }
}

static void init_trace(int verbose) {
  traceLevel = verbose ? 99 : 0;
}

static void open_db(char *file, tsdb_handler *db) {
  u_int16_t unused16 = 0;
  u_int32_t unused32 = 0;
  if (tsdb_open(file, db, &unused16, unused32, 0)) {
    printf("tsdb-get: error opening db %s\n", file);
    exit(1);
  }
}

static int goto_epoch(tsdb_handler *db, u_int32_t epoch) {
  if (tsdb_goto_epoch(db, epoch, 0, 0, 0)) {
    return 0;
  } else {
    return 1;
  }
}

static int get_vals(tsdb_handler *db, char *key, tsdb_value **vals) {
  return tsdb_get(db, key, vals);
}

static void print_missing(u_int32_t epoch, int value_count) {
  int i;
  printf("%u", epoch);
  for (i = 0; i < value_count; i++) {
    printf(" -");
  }
  printf("\n");
}

static void print_vals(u_int32_t epoch, int count, tsdb_value *vals) {
  int i;
  printf("%u", epoch);
  for (i = 0; i < count; i++) {
    printf(" %u", vals[i]);
  }
  printf("\n");
}

static void print_key_vals(tsdb_handler *db, char *key, u_int32_t epoch) {
  tsdb_value *vals;
  if (get_vals(db, key, &vals) >= 0) {
    print_vals(epoch, db->num_values_per_entry, vals);
  } else {
    print_missing(epoch, db->num_values_per_entry);
  }
}

static void print_epoch_vals(tsdb_handler *db, u_int32_t epoch, char *key) {
  if (goto_epoch(db, epoch)) {
    print_key_vals(db, key, epoch);
  } else {
    print_missing(epoch, db->num_values_per_entry);
  }
}

static void print_tsdb_values(char *file, char *key, u_int32_t start,
                              u_int32_t end) {
  tsdb_handler db;
  u_int32_t cur_epoch = start;

  open_db(file, &db);
  normalize_epoch(&db, &cur_epoch);
  normalize_epoch(&db, &end);
  while (cur_epoch <= end) {
    print_epoch_vals(&db, cur_epoch, key);
    cur_epoch += db.rrd_slot_time_duration;
  }
}

int main(int argc, char *argv[]) {
  get_args args;

  process_args(argc, argv, &args);
  init_trace(args.verbose);
  check_file_exists(args.file);
  print_tsdb_values(args.file, args.key, args.start, args.end);

  return 0;
}
