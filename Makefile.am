SUBDIRS = 3rd_party/MessagePack utility server

distclean-local:
	rm -rfv autom4te.cache
	rm -fv Makefile.in aclocal.m4 configure depcomp install-sh missing
	rm -fv AUTHORS ChangeLog COPYING INSTALL NEWS README
	rmdir -v bin 2> /dev/null || test 1
