AC_PREREQ(2.61)
AC_INIT([GridDB_NoSQL], [4.2.1])
AM_INIT_AUTOMAKE

CF_ORG="$CFLAGS"
CX_ORG="$CXXFLAGS"

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AM_PROG_INSTALL_STRIP
AC_PROG_RANLIB

CF_COMMON=" -Wall -finput-charset=UTF-8\
 -Wpointer-arith"
C_COMMON="$CF_COMMON  -Wmissing-prototypes"
CX_COMMON="$CF_COMMON -std=gnu++0x"

CF_OPTIMIZE=" -O3 -fno-tree-vectorize -DNDEBUG"
AC_ARG_ENABLE(debug, [  --enable-debug    turn on debugging],
[ test "${enableval}" = "yes" && CF_OPTIMIZE=" -gstabs+ -O0" ])
CX_OPTIMIZE="$CF_OPTIMIZE"

AS_IF([test "x$enable_debug" = xyes],
[ AM_CONDITIONAL(BUILD_SH_DEBUG, true) ],
[ AM_CONDITIONAL(BUILD_SH_DEBUG, false) ])

AC_ARG_ENABLE(activemq, [  --enable-activemq    link activemq], [], [ enable_activemq=no ])
AS_IF([test "x$enable_activemq" = xyes],
[ AM_CONDITIONAL(LINK_ACTIVEMQ, true)
AC_DEFINE(TRIGGER_CONNECT_ACTIVEMQ,1) ],
[ AM_CONDITIONAL(LINK_ACTIVEMQ, false) ])

CF_STRICT=""
CX_STRICT=""
AC_ARG_ENABLE(strict, [  --enable-strict    turn on strict compiling],
[ test "${enableval}" = "yes" &&
CF_STRICT=" -Wswitch-enum -Wfloat-equal -Wshadow -Winline\
 -Wstrict-aliasing=2 -Wundef" &&
CX_STRICT="$CF_STRICT -Weffc++ -Wsynth -Wold-style-cast" ])

CFLAGS="$CF_ORG $C_COMMON$CF_OPTIMIZE$CF_STRICT"
CXXFLAGS="$CX_ORG $CX_COMMON$CX_OPTIMIZE$CX_STRICT"

AC_CONFIG_FILES([Makefile 3rd_party/MessagePack/Makefile server/tr-cpp/Makefile utility/Makefile server/Makefile])
AC_OUTPUT
