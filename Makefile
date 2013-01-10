CC           = gcc -g
WARNS        =  -Wshadow -Wpointer-arith -Wmissing-prototypes -Wmissing-declarations -Wnested-externs
INCLUDE      = -I. -I /opt/local/include $(WARNS)
CFLAGS       = -Wall ${INCLUDE} # -fPIC
LDFLAGS      = -L /opt/local/lib
SYSLIBS      = -lrrd -ldb

TSDB_LIB   = libtsdb2.a
TSDB_LIB_O = tsdb_api.o tsdb_trace.o quicklz.o

TARGETS     = $(TSDB_LIB) test tsdbExport

all: $(TARGETS)

%.o: %.c %.h
#	@echo "=*= making object $@ =*="
	${CC} ${CFLAGS} ${INCLUDE} -c $< -o $@


$(TSDB_LIB): $(TSDB_LIB_O)
	ar rs $@ ${TSDB_LIB_O}
	ranlib $@

test: test.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) test.o $(TSDB_LIB) $(SYSLIBS) -o $@

tsdbExport: tsdbExport.o $(TSDB_LIB) Makefile
	$(CC) $(LDFLAGS) tsdbExport.o $(TSDB_LIB) $(SYSLIBS) -o $@

domain2tsdb: domain2tsdb.o $(TSDB_LIB) Makefile
	$(CC) $(LDFLAGS) domain2tsdb.o -o domain2tsdb $(TSDB_LIB) $(SYSLIBS)

clean:
	/bin/rm -f ${TARGETS} *.o *~
