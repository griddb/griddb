SUBDIRS = 3rd_party/MessagePack utility server/tr-cpp server

BINDIRS = bin
CONFDIRS = conf
OSSDIRS = 3rd_party

BINFILES = $(BINDIRS)/gridstore.jar \
	$(BINDIRS)/gridstore-conf.jar \
	$(BINDIRS)/gs_adduser \
	$(BINDIRS)/gs_deluser \
	$(BINDIRS)/gs_joincluster \
	$(BINDIRS)/gs_leavecluster \
	$(BINDIRS)/gs_passwd \
	$(BINDIRS)/gsserver \
	$(BINDIRS)/gs_startnode \
	$(BINDIRS)/gs_stat \
	$(BINDIRS)/gs_stopcluster \
	$(BINDIRS)/gs_stopnode \
	$(BINDIRS)/log.py \
	$(BINDIRS)/util.py
CONFFILES = $(CONFDIRS)/gs_cluster.json \
	$(CONFDIRS)/gs_node.json \
	$(CONFDIRS)/password
OSSFILES = $(OSSDIRS)/3rd_party.md \
	$(OSSDIRS)/Apache_License-2.0.txt \
	$(OSSDIRS)/BSD_License.txt \
	$(OSSDIRS)/MIT_License.txt \
	$(OSSDIRS)/MessagePack/COPYING \
	$(OSSDIRS)/activemq-cpp-library/org/NOTICE.txt \
	$(OSSDIRS)/apr/org/NOTICE \
	$(OSSDIRS)/ebb/LICENSE \
	$(OSSDIRS)/picojson/org/include/README.mkdn \
	$(OSSDIRS)/purewell/purewell.txt \
	$(OSSDIRS)/sha2/README \
	$(OSSDIRS)/slf4j/LICENSE.txt \
	$(OSSDIRS)/slf4j/slf4j-api-1.7.7.jar \
	$(OSSDIRS)/slf4j/slf4j-jdk14-1.7.7.jar \
	$(OSSDIRS)/yield/yield.txt \
	$(OSSDIRS)/json-simple/fangyidong/LICENSE.txt \
	$(OSSDIRS)/uuid/uuid/COPYING \
	$(OSSDIRS)/omaha/COPYING \
	$(OSSDIRS)/zigzag_encoding/LICENSE 

$(BINDIRS)/gridstore.jar:
	cd java_client;ant

dist_noinst_DATA = $(BINFILES) $(CONFFILES) $(OSSFILES) \
	README.md \
	docs/sample/program/Sample1.java

distclean-local:
	rm -rfv autom4te.cache
	rm -fv Makefile.in aclocal.m4 configure depcomp install-sh missing
	rm -fv AUTHORS ChangeLog COPYING INSTALL NEWS README
	rmdir -v bin 2> /dev/null || test 1
