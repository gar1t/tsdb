#include "tsdb_api.h"

#define info(...) traceEvent(TRACE_INFO, __VA_ARGS__);
#define error(...) traceEvent(TRACE_ERROR, __VA_ARGS__);
#define safe(X) if (X) { error("%s:%u", __FILE__, __LINE__); return -1; }
#define TIMESLOT 60
#define now() time(NULL)

int main(int argc, char *argv[]) {
  char *tsdb_path = "test2.tsdb";
  tsdb_handler handler;
  // ??? u_int32_t num_hash_indexes = 1000000;
  u_int32_t i;
  u_int32_t val;
  u_int16_t num_values_per_entry = 1;

  traceLevel = 99;

  safe(tsdb_open(tsdb_path,
                 &handler,
                 &num_values_per_entry,
                 TIMESLOT /* rrd_slot_time_duration */,
                 0 /* read only mode */));

  for (i = 0; i < 8; i++) {
    info("Run %u", i);
    safe(tsdb_goto_epoch(&handler,
                         now() + TIMESLOT * i,
                         1 /* create_if_needed */,
                         1 /* growable */,
                         0 /* load page on demand */));
    val = i * i * i;
    safe(tsdb_set(&handler, "foobar", &val));
  }

  tsdb_close(&handler);

  return 0;
}
