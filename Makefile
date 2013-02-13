CC           = gcc -g
WARNS        = -Wshadow -Wpointer-arith -Wmissing-prototypes -Wmissing-declarations -Wnested-externs
CFLAGS       = -Wall -I.
LDFLAGS      = -L /opt/local/lib
SYSLIBS      = -lrrd -ldb

TSDB_LIB   = libtsdb.a
TSDB_LIB_O = tsdb_api.o tsdb_trace.o quicklz.o

TARGETS     = $(TSDB_LIB) test tsdb-create tsdb-info tsdb-set tsdb-get

all: $(TARGETS)

%.o: %.c %.h
	${CC} ${CFLAGS} ${INCLUDE} -c $< -o $@


$(TSDB_LIB): $(TSDB_LIB_O)
	ar rs $@ ${TSDB_LIB_O}
	ranlib $@

test: test.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) test.o $(TSDB_LIB) $(SYSLIBS) -o $@

tsdb-create: tsdb_create.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) tsdb_create.o $(TSDB_LIB) $(SYSLIBS) -o $@

tsdb-info: tsdb_info.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) tsdb_info.o $(TSDB_LIB) $(SYSLIBS) -o $@

tsdb-set: tsdb_set.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) tsdb_set.o $(TSDB_LIB) $(SYSLIBS) -o $@

tsdb-get: tsdb_get.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) tsdb_get.o $(TSDB_LIB) $(SYSLIBS) -o $@

clean:
	/bin/rm -f ${TARGETS} *.o *~
