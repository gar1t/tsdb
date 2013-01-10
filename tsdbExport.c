/*
 *
 *  Copyright (C) 2011 IIT/CNR (http://www.iit.cnr.it/en)
 *                     Luca Deri <deri@ntop.org>
 *
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "tsdb_api.h"
#include <rrd.h>

/* *********************************** */

static void help(void) {
  printf("tsdbExport -i <tsdb path> [-o <outfile path>] -n <key> "
	 "-b <begin day (+/-offset)> -d <num days> [-t] [-j]\n");
  exit(0);
}

/* ***************************************************************** */

static void dump_argcv(int argc, char *argv[]){
  int i;

  return; // FIX

  for(i=0; i<argc; i++)
    traceEvent(TRACE_INFO, "%s", argv[i]);
}

/* ***************************************************************** */

static u_int roundRRDtime(u_int the_time, u_int rrd_step) {
  return((the_time / rrd_step) * rrd_step);
}

/* ***************************************************************** */

static int create_rrd(tsdb_handler *handler, char *name, u_int start_time, u_int rrd_step) {
  struct stat s;

  if(stat(name, &s) != 0) {
    char *argv[32], str[6][32];
    int argc = 0, rc, j;

    for(j=0; j<handler->num_values_per_entry; j++) {
      sprintf(str[j], "DS:counter%d:GAUGE:172800:0:U", j);

      argv[argc++] = str[j];
    }

    argv[argc++] = "RRA:AVERAGE:0.5:1:366";
    // argv[argc++] = "RRA:AVERAGE:0.5:7:53";
    //argv[argc++] = "RRA:AVERAGE:0.5:31:12";

    dump_argcv(argc, argv);
    rrd_clear_error();
    rc = rrd_create_r(name, rrd_step, roundRRDtime(start_time-1, rrd_step)-rrd_step, argc, (const char**)argv);

    if(rc != 0)
      traceEvent(TRACE_WARNING, "%s", rrd_get_error());

    return(rc);
  } else
    return(0);
}

/* ***************************************************************** */

static int update_rrd(tsdb_handler *handler, char *name, u_int when, u_int32_t *values) {
  char *argv[32];
  int argc = 0, rc, j;
  char str[32];
  time_t t = when;

  snprintf(str, sizeof(str), "%u", when);

  for(j=0; j<handler->num_values_per_entry; j++)
    snprintf(&str[strlen(str)], sizeof(str)-strlen(str), ":%u", values[j]);

  argv[argc++] = str;
  traceEvent(TRACE_INFO, "rrd_update(%s,%s) - %s", name, str, ctime(&t));

  dump_argcv(argc, argv);
  rrd_clear_error();
  rc = rrd_update_r(name, NULL, argc, (const char **)argv);

  if(rc != 0)
    traceEvent(TRACE_WARNING, "%s", rrd_get_error());

  return(rc);
}

/* *********************************** */

int main(int argc, char *argv[]) {
  int i, text_mode = 0, num_loops;
  tsdb_handler handler;
  int c, begin_day = 0, num_days = 0;
  u_int32_t when, the_time, *rsp, defaults[] = { 0, 0 }, begin_when;
  char *hash_id = NULL;
  char *tsdb_path = NULL, *out_filepath = NULL;
  u_int32_t rrd_slot_time_duration = 86400;
  u_int16_t num_values_per_entry;
  u_int8_t js_mode = 0;
  FILE *out;

  while((c = getopt(argc,argv,"hvi:o:n:b:d:tj")) != '?') {
    if((c == 255) || (c == -1)) break;

    switch(c) {
    case 'v':
      traceLevel = 99;
      break;

    case 'i':
      tsdb_path = strdup(optarg);
      break;

    case 'o':
      out_filepath = strdup(optarg);
      break;

    case 'n':
      hash_id = strdup(optarg);
      break;

    case 'b':
      begin_day = atoi(optarg);
      break;

    case 'd':
      num_days = atoi(optarg);
      break;

    case 'j':
      js_mode = 1;
      break;

    case 't':
      text_mode = 1;
      break;

    default:
      help();
      break;
    }
  }

  if((!tsdb_path) || ((!text_mode) && (!out_filepath))
     || (begin_day == 0) || (num_days == 0) || (hash_id == NULL))
    help();

  traceEvent(TRACE_INFO, "Opening %s", tsdb_path);

  if(tsdb_open(tsdb_path, &handler,
	       &num_values_per_entry, rrd_slot_time_duration, 1) != 0)
    return(-1);

  tzset();

  when = begin_when = time(NULL) + rrd_slot_time_duration * begin_day;

  normalize_epoch(&handler, &when);

  the_time = time(NULL);

  if(!text_mode) {
    unlink(out_filepath);
    create_rrd(&handler, out_filepath, when, rrd_slot_time_duration);
  } else {
    if(out_filepath != NULL) {
      if((out = fopen(out_filepath, "w")) == NULL) {
	traceEvent(TRACE_ERROR, "Unable to create file %s\n", out_filepath);
	return(-1);
      }
    } else
      out = stdout;

    if(!js_mode)
      fprintf(out, "# date, value\n");
  }

  for(num_loops=0; num_loops<handler.num_values_per_entry; num_loops++) {
    if(text_mode) {
      if(js_mode)
	fprintf(out, "var results_%d = [\n", num_loops);
      else {
	if(num_loops == 0) fprintf(out, "# date, value\n");
      }
    }

    when = begin_when;

  for(i=0; i<num_days; i++) {
    int rc;

    traceEvent(TRACE_INFO, "Moving to epoch %u", when);

    if((rc = tsdb_goto_epoch(&handler, when, 0, 0, 1 /* load on demand */)) == -1) {
      traceEvent(TRACE_INFO, "Unable to find epoch %u", when);
      rsp = defaults; /* Missing */
    } else
      rc = tsdb_get(&handler, hash_id, &rsp);

    if(text_mode) {
      /* http://www.simile-widgets.org/timeplot/ */
      time_t w = (time_t)when;
      struct tm *t = localtime(&w);
      int j;

      if(js_mode) {
	fprintf(out, "[Date.UTC(%04u,%02u,%02u),%u],\n",
		1900+t->tm_year, t->tm_mon, t->tm_mday,
		(rc >= 0) ? rsp[num_loops] : 0);
      } else {
	fprintf(out, "%04u-%02u-%02u ", 1900+t->tm_year, 1+t->tm_mon, t->tm_mday);

	for(j=0; j<handler.num_values_per_entry; j++) {
	  if(j > 0)  fprintf(out, ",");
	  fprintf(out, "%u", (rc >= 0) ? rsp[j] : 0);
	}

	fprintf(out, "\n");
      }
    } else
      update_rrd(&handler, out_filepath, when, rsp);

    // traceEvent(TRACE_INFO, "%u: %u", when, rsp);
    when += rrd_slot_time_duration;
  }

  if(text_mode && js_mode) {
    fprintf(out, "];\n");
  }
  }

  if(text_mode && out_filepath) {
    fclose(out);
  }

  the_time = time(NULL)-the_time;

  traceEvent(TRACE_INFO, "Time elapsed: %u sec", the_time);

  traceEvent(TRACE_INFO, "Closing %s", tsdb_path);

  tsdb_close(&handler);

  traceEvent(TRACE_INFO, "Done");
  return(0);
}
