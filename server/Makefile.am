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

distclean-local:
	rm -fv Makefile.in

$(aprinclude)/apr.h:
	rm -rf $(libdir)/apr/org
	rm -rf $(libdir)/apr/install
	tar xfz $(libdir)/apr/apr-1.4.8.tar.gz -C $(libdir)/apr
	mv $(libdir)/apr/apr-1.4.8 $(libdir)/apr/org
	$(libdir)/apr/build.sh

$(activemqinclude)/ActiveMQCPP.h:
	rm -rf $(libdir)/activemq-cpp-library/org
	rm -rf $(libdir)/activemq-cpp-library/install
	tar xfz $(libdir)/activemq-cpp-library/activemq-cpp-library-3.7.1-src.tar.gz -C $(libdir)/activemq-cpp-library
	mv $(libdir)/activemq-cpp-library/activemq-cpp-library-3.7.1 $(libdir)/activemq-cpp-library/org
	$(libdir)/activemq-cpp-library/build.sh

$(trdir)/libtrcpp.a:
	./configure
	make

if LINK_ACTIVEMQ
aprinclude=$(libdir)/apr/install/include/apr-1
aprldadd=$(libdir)/apr/install/lib/libapr-1.a -lssl -lcrypto
activemqinclude=$(libdir)/activemq-cpp-library/install/include/activemq-cpp-3.7.1
activemqldadd=$(libdir)/activemq-cpp-library/install/lib/libactivemq-cpp.a
aprheaders = $(aprinclude)/apr.h
activemqheaders = $(activemqinclude)/ActiveMQCPP.h
else !LINK_ACTIVEMQ
aprinclude=
aprldadd=
activemqinclude=
activemqldadd=
aprheaders =
activemqheaders =
endif !LINK_ACTIVEMQ

BUILT_SOURCES = \
$(aprheaders) \
$(activemqheaders)

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

NOSQL_SOURCES = \
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
$(srcdir)/base_container.cpp \
$(srcdir)/bit_array.cpp \
$(srcdir)/blob_processor.cpp \
$(srcdir)/btree_map.cpp \
$(srcdir)/checkpoint_file.cpp \
$(srcdir)/checkpoint_service.cpp \
$(srcdir)/chunk_manager.cpp \
$(srcdir)/cluster_event_type.cpp \
$(srcdir)/cluster_manager.cpp \
$(srcdir)/cluster_service.cpp \
$(srcdir)/collection.cpp \
$(srcdir)/config_table.cpp \
$(srcdir)/container_archive.cpp \
$(srcdir)/container_key.cpp \
$(srcdir)/data_store.cpp \
$(srcdir)/event_engine.cpp \
$(srcdir)/geometry_processor.cpp \
$(srcdir)/gs_error.cpp \
$(srcdir)/hash_map.cpp \
$(srcdir)/http.cpp \
$(srcdir)/json.cpp \
$(srcdir)/log_manager.cpp \
$(srcdir)/message_row_store.cpp \
$(srcdir)/message_schema.cpp \
$(srcdir)/meta_store.cpp \
$(srcdir)/object_allocator.cpp \
$(srcdir)/object_manager.cpp \
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
$(srcdir)/sub_time_series.cpp \
$(srcdir)/sync_manager.cpp \
$(srcdir)/sync_service.cpp \
$(srcdir)/system_service.cpp \
$(srcdir)/time_series.cpp \
$(srcdir)/transaction_context.cpp \
$(srcdir)/transaction_manager.cpp \
$(srcdir)/transaction_manager_authentication.cpp \
$(srcdir)/transaction_service.cpp \
$(srcdir)/transaction_service_user_management.cpp \
$(srcdir)/trigger_service.cpp \
$(srcdir)/uuid_utils.cpp \
$(srcdir)/value.cpp \
$(srcdir)/value_operator.cpp \
$(srcdir)/value_processor.cpp \
$(srcdir)/main.cpp

BASE_CPPFLAGS = \
-DGD_ENABLE_UNICAST_NOTIFICATION

NOSQL_CPPFLAGS = \
$(BASE_CPPFLAGS)

NOSQL_LDADD = \
$(QP_LDADD) \
$(utildir)/libutil.a \
$(libdir)/MessagePack/libmsgpack.a \
$(activemqldadd) \
$(aprldadd) \
-lpthread -lrt -ldl -lstdc++ -lz

NOSQL_INCLUDES = \
-I$(srcdir) \
-I$(utildir) \
-I$(libdir)/MessagePack/include \
-I$(libdir)/picojson/include \
-I$(libdir)/ebb \
-I$(libdir)/sha2 \
-I$(libdir)/omaha/omaha/base/security \
-I$(libdir)/uuid \
-I$(srcdir)/tr-cpp \
-I$(aprinclude) \
-I$(activemqinclude)

NOSQL_LDFLAG = \
-rdynamic

gsserver_SOURCES = $(NOSQL_SOURCES)
gsserver_CPPFLAGS = $(NOSQL_CPPFLAGS) $(NOSQL_INCLUDES)
gsserver_LDADD = $(NOSQL_LDADD)
gsserver_LDFLAGS = $(NOSQL_LDFLAG)
