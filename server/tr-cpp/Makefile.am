SUBDIRS = .
ACLOCAL_AMFLAGS=-I m4
TOP_DIR=../../

noinst_LIBRARIES = libtrcpp.a
libtrcpp_a_SOURCES = \
	internal.h \
	TrRect.cpp TrIndex.cpp \
	TrNode.cpp TrNodeSearch.cpp TrNodeInsert.cpp TrNodeDelete.cpp \
	TrNodeSplit.cpp TrPv3.cpp

libtrcpp_a_CPPFLAGS= -Wall -std=c++0x -I$(TOP_DIR)/server -I$(TOP_DIR)/utility/ -I$(TOP_DIR)/lib/MessagePack/include/ 
include_HEADERS = TrTree.h TrPv3.h
