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

int __trace_level = 2;

void trace_event(int level, char* file, int line, char *format, ...) {
    if (level > __trace_level) {
        return;
    }

    va_list va_ap;
    char buf[2048], out_buf[640];
    char theDate[32], *extra_msg = "";
    time_t theTime = time(NULL);

    va_start (va_ap, format);
    memset(buf, 0, sizeof(buf));
    strftime(theDate, 32, "%d/%b/%Y %H:%M:%S", localtime(&theTime));

    vsnprintf(buf, sizeof(buf) - 1, format, va_ap);

    if (level == 1) {
        extra_msg = "ERROR: ";
    } else if(level == 2) {
        extra_msg = "WARNING: ";
    }

    while (buf[strlen(buf) - 1] == '\n') {
        buf[strlen(buf) - 1] = '\0';
    }

    snprintf(out_buf, sizeof(out_buf), "%s [%s:%d] %s%s", theDate,
#ifdef WIN32
             strrchr(file, '\\') + 1,
#else
             file,
#endif
             line, extra_msg, buf);
    printf("%s\n", out_buf);

    fflush(stdout);
    va_end(va_ap);
}

