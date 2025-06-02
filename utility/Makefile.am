noinst_LIBRARIES = libutil.a

srcdir = .

distclean-local:
	rm -fv Makefile.in

libutil_a_SOURCES = \
$(srcdir)/util/allocator.cpp \
$(srcdir)/util/allocator.h \
$(srcdir)/util/code.cpp \
$(srcdir)/util/code.h \
$(srcdir)/util/container.cpp \
$(srcdir)/util/container.h \
$(srcdir)/util/file.cpp \
$(srcdir)/util/file.h \
$(srcdir)/util/net.cpp \
$(srcdir)/util/net.h \
$(srcdir)/util/numeric.cpp \
$(srcdir)/util/numeric.h \
$(srcdir)/util/os.cpp \
$(srcdir)/util/os.h \
$(srcdir)/util/system.cpp \
$(srcdir)/util/system.h \
$(srcdir)/util/thread.cpp \
$(srcdir)/util/thread.h \
$(srcdir)/util/time.cpp \
$(srcdir)/util/time.h \
$(srcdir)/util/trace.cpp \
$(srcdir)/util/trace.h \
$(srcdir)/util/type.cpp \
$(srcdir)/util/type.h

libutil_a_CPPFLAGS = \
-I$(top_srcdir)/utility \
-I$(top_srcdir)/3rd_party/fletcher32_simd
