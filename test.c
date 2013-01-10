/*
 *
 *  Copyright (C) 2011 IIT/CNR (http://www.iit.cnr.it/en)
 *                     Luca Deri <deri@ntop.org>
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

int main(int argc, char *argv[]) {
  char *tsdb_path = "my.tsdb";
  tsdb_handler handler;
  u_int32_t num_hash_indexes = 1000000, i;
  u_int16_t num_values_per_entry = 1;

  traceLevel = 99;

  if(tsdb_open(tsdb_path, &handler, &num_values_per_entry, 86400 /* rrd_slot_time_duration */, 0) != 0)
    return(-1);

  for(i=0; i<8; i++) {
    traceEvent(TRACE_INFO, "Run %u", i);

    if(tsdb_goto_epoch(&handler, time(NULL)-(86400*i), 
			1 /* create_if_needed */,
			1 /* growable */,
			num_hash_indexes) == -1)
      return(-1);
  }

  tsdb_close(&handler);

  return(0);
}
