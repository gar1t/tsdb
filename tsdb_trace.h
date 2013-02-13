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

#define TRACE_ERROR     1, __FILE__, __LINE__
#define TRACE_WARNING   2, __FILE__, __LINE__
#define TRACE_INFO      3, __FILE__, __LINE__

#define trace_error(...)   trace_event(TRACE_ERROR,   __VA_ARGS__)
#define trace_warning(...) trace_event(TRACE_WARNING, __VA_ARGS__)
#define trace_info(...)    trace_event(TRACE_INFO,    __VA_ARGS__)

#define set_trace_level(level) __trace_level = level

extern int __trace_level;
extern void trace_event(int level, char* file, int line, char * format, ...);
