#!/bin/bash

cd $(dirname "$0")

SCRIPT_DIR=.
TCL_DIR=$SCRIPT_DIR/..
WORK_DIR=$SCRIPT_DIR/work

# Relative path from WORK_DIR
SQLITE_DIR=../..
GS_DIR=$SQLITE_DIR/../..

SRC_DIR=$SQLITE_DIR/src
EXT_DIR=$SQLITE_DIR/gs_ext

UTILITY_DIR=$GS_DIR/utility
SERVER_DIR=$GS_DIR/server

ENABLE_DEBUG=no
ENABLE_CLEAN=no

for i in $@
do
	case $i in
	--debug)
		ENABLE_DEBUG=yes ;;
	--clean)
		ENABLE_CLEAN=yes ;;
	*)
		echo "Usage: $0 [--debug] [--clean]"
		exit 1 ;;
	esac
done

CFLAGS="\
-I. \
-I$SRC_DIR \
-I$EXT_DIR \
-I$UTILITY_DIR \
-I$SERVER_DIR \
-DSQLITE_OMIT_ALTERTABLE \
-DSQLITE_OMIT_ANALYZE \
-DSQLITE_OMIT_ATTACH \
-DSQLITE_OMIT_AUTHORIZATION \
-DSQLITE_OMIT_AUTOINCREMENT \
-DSQLITE_OMIT_AUTOVACUUM \
-DSQLITE_OMIT_BUILTIN_TEST \
-DSQLITE_OMIT_CHECK \
-DSQLITE_OMIT_COMPILEOPTION_DIAGS \
-DSQLITE_OMIT_CTE \
-DSQLITE_OMIT_DEPRECATED \
-DSQLITE_OMIT_FOREIGN_KEY \
-DSQLITE_OMIT_GET_TABLE \
-DSQLITE_OMIT_INCRBLOB \
-DSQLITE_OMIT_INTEGRITY_CHECK \
-DSQLITE_OMIT_LOAD_EXTENSION \
-DSQLITE_OMIT_LOOKASIDE \
-DSQLITE_OMIT_PROGRESS_CALLBACK \
-DSQLITE_OMIT_REINDEX \
-DSQLITE_OMIT_SCHEMA_PRAGMAS \
-DSQLITE_OMIT_SCHEMA_VERSION_PRAGMAS \
-DSQLITE_OMIT_SHARED_CACHE \
-DSQLITE_OMIT_TCL_VARIABLE \
-DSQLITE_OMIT_TEMPDB \
-DSQLITE_OMIT_TRIGGER \
-DSQLITE_OMIT_UTF16 \
-DSQLITE_OMIT_VACUUM \
-DSQLITE_OMIT_VIEW \
-DSQLITE_OMIT_VIRTUALTABLE \
-DSQLITE_OMIT_WAL \
-DSQLITE_ZERO_MALLOC \
-DSQLITE_OMIT_XFER_OPT \
-DSQLITE_OMIT_COMPLETE \
-DSQLITE_DEFAULT_MEMSTATUS=0 \
-DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS \
-DSQLITE_MAX_MMAP_SIZE=0 \
-DSQLITE_DEFAULT_CACHE_SIZE=1000000 \
-DGD_ENABLE_NEWSQL_SERVER"

COMPILE_OPT=" \
-Wall \
-Wextra \
-Wformat=2 \
-Wcast-qual \
-Wcast-align \
-Wpointer-arith \
-Wwrite-strings \
-Wconversion"

if [ $ENABLE_DEBUG = yes ]
then
	CONFIGURE_OPT="$CONFIGURE_OPT --enable-debug"
	COMPILE_OPT="$COMPILE_OPT -gstabs+ -O0 -DSQLITE_DEBUG=1"
else
	COMPILE_OPT="$COMPILE_OPT -O3 -fno-tree-vectorize -DNDEBUG"
fi

if [ $ENABLE_CLEAN = yes ]
then
	rm -rf $WORK_DIR
	exit 0
fi

if type "tclsh" > /dev/null 2>&1
then
	echo "tclsh exists"
else
	echo "No tclsh"
	exit 0
fi

mkdir -p $WORK_DIR || exit 1

cd $WORK_DIR
(
	#	NewSQL(rewrite.cpp) requires non-API symbols which are
	#	hidden when the library is built via the amalgamation
	#   so add -DSQLITE_PRIVATE=\"\" 
	CFLAGS="$CFLAGS -DSQLITE_PRIVATE=\"\""

	echo "Executing configure..."
	echo "CONFIGURE_OPT=$CONFIGURE_OPT"
	echo "CFLAGS=$CFLAGS"
	$SQLITE_DIR/configure $CONFIGURE_OPT CFLAGS="$CFLAGS" || exit 1

	echo "Building SQLiteLib..."
	make -j $(($(grep -c processor /proc/cpuinfo)*2)) sqlite3.h libsqlite3.la || exit 1

	cp .libs/libsqlite3.a libsqlite3.a

	cmd="${CC:=gcc} -c $COMPILE_OPT $CFLAGS $EXT_DIR/backend.cpp -o backend.o"
	echo "$cmd"
	$cmd || exit 1

	cmd="${CC:=gcc} -c $COMPILE_OPT $CFLAGS $EXT_DIR/memgs.c -o memgs.o"
	echo "$cmd"
	$cmd || exit 1

	cmd="${CC:=gcc} -c $COMPILE_OPT $CFLAGS $EXT_DIR/maings.c -o maings.o"
	echo "$cmd"
	$cmd || exit 1

	cmd="ar r libsqlite3.a backend.o memgs.o maings.o"
	echo "$cmd"
    $cmd || exit 1
)
