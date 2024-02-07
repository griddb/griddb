serverbinname = gsserver
serverenvname = buildenv-server.txt

bin_PROGRAMS = $(serverbinname)
buildenvname = $(serverenvname)

srcdir = .
bindir = $(srcdir)/../bin
deploybinfile = $(bindir)/$(bin_PROGRAMS)
buildenvfile = $(bindir)/$(buildenvname)

topdir = $(srcdir)/..
utildir = $(topdir)/utility
libdir = $(topdir)/3rd_party
trdir = $(srcdir)/tr-cpp
newsqlincdir = $(srcdir)/include
sqlitedir = $(libdir)/sqlite_mod
sqliteworkdir = $(libdir)/sqlite_mod/build/work

all-local: $(buildenvfile)

$(deploybinfile): $(bin_PROGRAMS)
	test -x $(bin_PROGRAMS) && mkdir -p $(bindir) && cp -p $(bin_PROGRAMS) $(bindir)/.

$(buildenvfile): $(deploybinfile)
	rm -fv $(buildenvfile)
	echo '[hostname]' >> $(buildenvfile)
	hostname >> $(buildenvfile)
	echo '[bin]' >> $(buildenvfile)
	ls -ln --time-style="+%Y-%m-%d %H:%M:%S" $(bin_PROGRAMS) >> $(buildenvfile)
	echo '[bin-version]' >> $(buildenvfile)
	./$(bin_PROGRAMS) --version >> $(buildenvfile) 2>&1
	echo '[md5sum]' >> $(buildenvfile)
	md5sum $(bin_PROGRAMS) >> $(buildenvfile)
	echo '[ldd]' >> $(buildenvfile)
	ldd $(bin_PROGRAMS) >> $(buildenvfile)
	echo '[ldd-libs]' >> $(buildenvfile)
	ldd $(bin_PROGRAMS) | grep ' => .\+\.so' | cut -d ' ' -f 3 | xargs ls -ln --time-style="+%Y-%m-%d %H:%M:%S" >> $(buildenvfile)
	echo '[ldd-libs-actual]' >> $(buildenvfile)
	@for i in `ldd $(bin_PROGRAMS) | grep ' => .\+\.so' | cut -d ' ' -f 3` ; do readlink -f $$i ; done | xargs ls -ln --time-style="+%Y-%m-%d %H:%M:%S" >> $(buildenvfile)
	echo '[bin-depends]' >> $(buildenvfile)
	( LANG=C readelf -d $(bin_PROGRAMS) ) | grep 'NEEDED\|SONAME' >> $(buildenvfile) || :
	echo '[bin-version-needs]' >> $(buildenvfile)
	( LANG=C readelf -V $(bin_PROGRAMS) ) | grep 'Version:' >> $(buildenvfile) || :
	echo '[cc-version]' >> $(buildenvfile)
	@CC@ --version >> $(buildenvfile)
	echo '[cxx-version]' >> $(buildenvfile)
	@CXX@ --version >> $(buildenvfile)

clean-local:
	rm -fv $(bindir)/$(serverbinname) $(bindir)/$(serverenvname)
	$(sqlitedir)/build/build.sh --clean

distclean-local:
	rm -fv Makefile.in



if BUILD_SH_DEBUG
buildshopt=--debug
sqlitedebugopt=-DSQLITE_DEBUG=1
else !BUILD_SH_DEBUG
buildshopt=
sqlitedebugopt=
endif !BUILD_SH_DEBUG

sqlitebasicdeps = \
	$(sqlitedir)/src/parse.y \
	$(sqlitedir)/src/sqlite.h.in

sqlitedeps = \
	$(sqlitebasicdeps) \
	$(sqlitedir)/gs_ext/backend.cpp \
	$(sqlitedir)/gs_ext/backend.hpp \
	$(sqlitedir)/gs_ext/maings.c \
	$(sqlitedir)/gs_ext/memgs.c \
	$(sqlitedir)/src/alter.c \
	$(sqlitedir)/src/analyze.c \
	$(sqlitedir)/src/attach.c \
	$(sqlitedir)/src/auth.c \
	$(sqlitedir)/src/backup.c \
	$(sqlitedir)/src/bitvec.c \
	$(sqlitedir)/src/btmutex.c \
	$(sqlitedir)/src/btree.c \
	$(sqlitedir)/src/btree.h \
	$(sqlitedir)/src/btreeInt.h \
	$(sqlitedir)/src/build.c \
	$(sqlitedir)/src/callback.c \
	$(sqlitedir)/src/complete.c \
	$(sqlitedir)/src/ctime.c \
	$(sqlitedir)/src/date.c \
	$(sqlitedir)/src/delete.c \
	$(sqlitedir)/src/expr.c \
	$(sqlitedir)/src/fault.c \
	$(sqlitedir)/src/fkey.c \
	$(sqlitedir)/src/func.c \
	$(sqlitedir)/src/global.c \
	$(sqlitedir)/src/hash.c \
	$(sqlitedir)/src/hash.h \
	$(sqlitedir)/src/hwtime.h \
	$(sqlitedir)/src/insert.c \
	$(sqlitedir)/src/journal.c \
	$(sqlitedir)/src/legacy.c \
	$(sqlitedir)/src/lempar.c \
	$(sqlitedir)/src/loadext.c \
	$(sqlitedir)/src/main.c \
	$(sqlitedir)/src/malloc.c \
	$(sqlitedir)/src/mem0.c \
	$(sqlitedir)/src/mem1.c \
	$(sqlitedir)/src/mem2.c \
	$(sqlitedir)/src/mem3.c \
	$(sqlitedir)/src/mem5.c \
	$(sqlitedir)/src/memjournal.c \
	$(sqlitedir)/src/mutex_noop.c \
	$(sqlitedir)/src/mutex_unix.c \
	$(sqlitedir)/src/mutex_w32.c \
	$(sqlitedir)/src/mutex.c \
	$(sqlitedir)/src/mutex.h \
	$(sqlitedir)/src/notify.c \
	$(sqlitedir)/src/os_common.h \
	$(sqlitedir)/src/os_setup.h \
	$(sqlitedir)/src/os_unix.c \
	$(sqlitedir)/src/os_win.c \
	$(sqlitedir)/src/os_win.h \
	$(sqlitedir)/src/os.c \
	$(sqlitedir)/src/os.h \
	$(sqlitedir)/src/pager.c \
	$(sqlitedir)/src/pager.h \
	$(sqlitedir)/src/pcache.c \
	$(sqlitedir)/src/pcache.h \
	$(sqlitedir)/src/pcache1.c \
	$(sqlitedir)/src/pragma.c \
	$(sqlitedir)/src/prepare.c \
	$(sqlitedir)/src/printf.c \
	$(sqlitedir)/src/random.c \
	$(sqlitedir)/src/resolve.c \
	$(sqlitedir)/src/rowset.c \
	$(sqlitedir)/src/select.c \
	$(sqlitedir)/src/shell.c \
	$(sqlitedir)/src/sqlite3ext.h \
	$(sqlitedir)/src/sqliteInt.h \
	$(sqlitedir)/src/sqliteLimit.h \
	$(sqlitedir)/src/status.c \
	$(sqlitedir)/src/table.c \
	$(sqlitedir)/src/tclsqlite.c \
	$(sqlitedir)/src/threads.c \
	$(sqlitedir)/src/tokenize.c \
	$(sqlitedir)/src/trigger.c \
	$(sqlitedir)/src/update.c \
	$(sqlitedir)/src/utf.c \
	$(sqlitedir)/src/util.c \
	$(sqlitedir)/src/vacuum.c \
	$(sqlitedir)/src/vdbe.c \
	$(sqlitedir)/src/vdbe.h \
	$(sqlitedir)/src/vdbeapi.c \
	$(sqlitedir)/src/vdbeaux.c \
	$(sqlitedir)/src/vdbeblob.c \
	$(sqlitedir)/src/vdbeInt.h \
	$(sqlitedir)/src/vdbemem.c \
	$(sqlitedir)/src/vdbesort.c \
	$(sqlitedir)/src/vdbetrace.c \
	$(sqlitedir)/src/vtab.c \
	$(sqlitedir)/src/wal.c \
	$(sqlitedir)/src/wal.h \
	$(sqlitedir)/src/walker.c \
	$(sqlitedir)/src/where.c \
	$(sqlitedir)/src/whereInt.h \
	$(sqlitedir)/tool/mkkeywordhash.c \
	$(srcdir)/c_utility_cpp.h \
	$(srcdir)/c_utility.h \
	$(srcdir)/newsql_interface.h

$(sqliteworkdir)/libsqlite3.a: $(sqliteworkdir)/sqlite3.h $(sqlitedeps)
	$(sqlitedir)/build/build.sh $(buildshopt)

$(sqliteworkdir)/sqlite3.h: $(sqlitebasicdeps)
	rm -rf $(sqliteworkdir)/sqlite3.h
	$(sqlitedir)/build/build.sh $(buildshopt)

$(trdir)/libtrcpp.a:
	./configure
	make

BUILT_SOURCES = \
$(sqliteworkdir)/sqlite3.h

QP_SOURCES= \
	$(srcdir)/tql.h \
	$(srcdir)/tql_token.h \
	$(srcdir)/wkt.cpp \
	$(srcdir)/wkt.h \
	$(srcdir)/boolean_expression.h \
	$(srcdir)/boolean_expression.cpp \
	$(srcdir)/expression.cpp \
	$(srcdir)/expression.h \
	$(srcdir)/gis_geomfromtext.h \
	$(srcdir)/gis_geometry.h \
	$(srcdir)/gis_pointgeom.h \
	$(srcdir)/gis_polygon.h \
	$(srcdir)/gis_surface.h \
	$(srcdir)/gis_geometry.cpp \
	$(srcdir)/lexer.h \
	$(srcdir)/lexer.cpp \
	$(srcdir)/function_map.cpp \
	$(srcdir)/function_map.h \
	$(srcdir)/tql.cpp \
	$(srcdir)/query.cpp \
	$(srcdir)/query.h \
	$(srcdir)/value_operator.h \
	$(srcdir)/rtree_map.h \
	$(srcdir)/rtree_map.cpp

QP_INCLUDE=../server/tr-cpp
QP_LDADD=../server/tr-cpp/libtrcpp.a

NEWSQL_SOURCES = \
$(QP_SOURCES)\
$(libdir)/ebb/ebb_request_parser.c \
$(libdir)/sha2/sha2.c \
$(libdir)/uuid/uuid/clear.c \
$(libdir)/uuid/uuid/compare.c \
$(libdir)/uuid/uuid/copy.c \
$(libdir)/uuid/uuid/gen_uuid.c \
$(libdir)/uuid/uuid/isnull.c \
$(libdir)/uuid/uuid/pack.c \
$(libdir)/uuid/uuid/parse.c \
$(libdir)/uuid/uuid/unpack.c \
$(libdir)/uuid/uuid/unparse.c \
$(libdir)/uuid/uuid/uuid_time.c \
$(srcdir)/zlib_utils.cpp \
$(srcdir)/array_processor.cpp \
$(srcdir)/authentication.cpp \
$(srcdir)/base_index.cpp \
$(srcdir)/base_container.cpp \
$(srcdir)/blob_processor.cpp \
$(srcdir)/btree_map.cpp \
$(srcdir)/btree_map_ext.cpp \
$(srcdir)/checkpoint_service.cpp \
$(srcdir)/chunk_manager.cpp \
$(srcdir)/cluster_event_type.cpp \
$(srcdir)/cluster_manager.cpp \
$(srcdir)/cluster_service_base.cpp \
$(srcdir)/cluster_service.cpp \
$(srcdir)/collection.cpp \
$(srcdir)/config_table.cpp \
$(srcdir)/container_archive.cpp \
$(srcdir)/container_key.cpp \
$(srcdir)/event_engine.cpp \
$(srcdir)/geometry_processor.cpp \
$(srcdir)/gs_error.cpp \
$(srcdir)/http.cpp \
$(srcdir)/json.cpp \
$(srcdir)/log_manager.cpp \
$(srcdir)/message_row_store.cpp \
$(srcdir)/message_schema.cpp \
$(srcdir)/meta_store.cpp \
$(srcdir)/partition_table.cpp \
$(srcdir)/query_api_processor.cpp \
$(srcdir)/query_collection.cpp \
$(srcdir)/query_processor.cpp \
$(srcdir)/query_timeseries.cpp \
$(srcdir)/recovery_manager.cpp \
$(srcdir)/result_set.cpp \
$(srcdir)/row.cpp \
$(srcdir)/schema.cpp \
$(srcdir)/service_address.cpp \
$(srcdir)/string_array_processor.cpp \
$(srcdir)/string_processor.cpp \
$(srcdir)/sync_manager.cpp \
$(srcdir)/sync_service_base.cpp \
$(srcdir)/sync_service.cpp \
$(srcdir)/system_service.cpp \
$(srcdir)/time_series.cpp \
$(srcdir)/transaction_context.cpp \
$(srcdir)/transaction_manager.cpp \
$(srcdir)/transaction_manager_authentication.cpp \
$(srcdir)/transaction_service_base.cpp \
$(srcdir)/transaction_service.cpp \
$(srcdir)/user_management.cpp \
$(srcdir)/uuid_utils.cpp \
$(srcdir)/value.cpp \
$(srcdir)/value_operator.cpp \
$(srcdir)/value_processor.cpp \
$(srcdir)/main.cpp \
$(srcdir)/client_stub.c \
$(srcdir)/c_utility.cpp \
$(srcdir)/query_function.cpp \
$(srcdir)/sql_common.cpp \
$(srcdir)/sql_compiler.cpp \
$(srcdir)/sql_compiler_join.cpp \
$(srcdir)/sql_execution.cpp \
$(srcdir)/sql_internal_parser.cpp \
$(srcdir)/sql_job_manager.cpp \
$(srcdir)/sql_lexer.cpp \
$(srcdir)/sql_parser.cpp \
$(srcdir)/sql_processor.cpp \
$(srcdir)/sql_processor_ddl.cpp \
$(srcdir)/sql_processor_dml.cpp \
$(srcdir)/sql_processor_dql.cpp \
$(srcdir)/sql_processor_result.cpp \
$(srcdir)/sql_service.cpp \
$(srcdir)/sql_result_set.cpp \
$(srcdir)/sql_temp_store.cpp \
$(srcdir)/sql_tuple.cpp \
$(srcdir)/sql_tuple_var.cpp \
$(srcdir)/sql_type.cpp \
$(srcdir)/sql_expression.cpp \
$(srcdir)/sql_expression_aggr.cpp \
$(srcdir)/sql_expression_core.cpp \
$(srcdir)/sql_expression_numeric.cpp \
$(srcdir)/sql_expression_string.cpp \
$(srcdir)/sql_expression_time.cpp \
$(srcdir)/sql_expression_utils.cpp \
$(srcdir)/sql_operator.cpp \
$(srcdir)/sql_operator_group.cpp \
$(srcdir)/sql_operator_join.cpp \
$(srcdir)/sql_operator_scan.cpp \
$(srcdir)/sql_operator_sort.cpp \
$(srcdir)/sql_operator_utils.cpp \
$(srcdir)/sql_utils_algorithm.cpp \
$(srcdir)/sql_utils_container.cpp \
$(srcdir)/sql_utils_vdbe.cpp \
$(srcdir)/sql_value.cpp \
$(srcdir)/affinity_manager.cpp \
$(srcdir)/chunk_buffer.cpp \
$(srcdir)/chunk.cpp \
$(srcdir)/container_message_v4.cpp \
$(srcdir)/database_manager.cpp \
$(srcdir)/data_store_v4.cpp \
$(srcdir)/interchangeable.cpp \
$(srcdir)/key_data_store.cpp \
$(srcdir)/login.cpp \
$(srcdir)/nosql_client_session.cpp \
$(srcdir)/nosql_command.cpp \
$(srcdir)/object_manager_v4.cpp \
$(srcdir)/partition.cpp \
$(srcdir)/partition_file.cpp \
$(srcdir)/socket_wrapper.cpp \
$(srcdir)/sql_command_manager.cpp \
$(srcdir)/utility_v5.cpp


BASE_CPPFLAGS = \
-DGD_ENABLE_UNICAST_NOTIFICATION

NEWSQL_CPPFLAGS = \
$(BASE_CPPFLAGS) \
$(sqlitedebugopt) \
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
-DGD_ENABLE_NEWSQL_SERVER

NEWSQL_LDADD = \
$(QP_LDADD) \
$(utildir)/libutil.a \
$(libdir)/MessagePack/libmsgpack.a \
-lpthread -lrt -ldl -lstdc++ -lz \
$(sqliteworkdir)/libsqlite3.a

NEWSQL_INCLUDES = \
-I$(srcdir) \
-I$(utildir) \
-I$(libdir)/MessagePack/include \
-I$(libdir)/picojson/include \
-I$(libdir)/ebb \
-I$(libdir)/fletcher32_simd \
-I$(libdir)/sha2 \
-I$(libdir)/uuid \
-I$(srcdir)/tr-cpp \
-I$(sqliteworkdir) \
-I$(sqlitedir)/src \
-I$(newsqlincdir)

NEWSQL_LDFLAG = \
-rdynamic

gsserver_SOURCES = $(NEWSQL_SOURCES)
gsserver_CPPFLAGS = $(NEWSQL_CPPFLAGS) $(NEWSQL_INCLUDES)
gsserver_LDADD = $(NEWSQL_LDADD)
gsserver_LDFLAGS = $(NEWSQL_LDFLAG)
