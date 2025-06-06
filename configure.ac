AC_PREREQ(2.61)
AC_INIT([GridDB], [5.8.0])
AC_CONFIG_AUX_DIR([.])
AM_INIT_AUTOMAKE([subdir-objects])

CF_ORG="$CFLAGS"
CX_ORG="$CXXFLAGS"

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AM_PROG_INSTALL_STRIP
AC_PROG_RANLIB

AC_ARG_ENABLE(cxx11, [  --enable-cxx11    enable C++11], [], [ enable_cxx11=yes ])
AS_IF([test "x$enable_cxx11" = xyes],
[ CX_CXX11=-std=gnu++0x ],
[ CX_CXX11= ])

CF_COMMON=" -finput-charset=UTF-8"
CX_COMMON="$CF_COMMON $CX_CXX11"

CF_OPTIMIZE=" -O3 -fno-tree-vectorize -DNDEBUG"
AC_ARG_ENABLE(debug, [  --enable-debug    turn on debugging],
[ test "${enableval}" = "yes" && CF_OPTIMIZE=" -gstabs+ -O0" ])
CX_OPTIMIZE="$CF_OPTIMIZE"

AC_DEFINE(UTIL_DYNAMIC_LOAD_ENABLED,1)
AC_DEFINE(UTIL_EXT_LIBRARY_ENABLED,1)

AS_IF([test "x$enable_debug" = xyes],
[ AM_CONDITIONAL(BUILD_SH_DEBUG, true) ],
[ AM_CONDITIONAL(BUILD_SH_DEBUG, false) ])

CF_STRICT=""
CX_STRICT=""
AC_ARG_ENABLE(strict, [  --enable-strict    turn on strict compiling],
[ test "${enableval}" = "yes" &&
CF_STRICT=" -Wswitch-enum -Wfloat-equal -Wshadow -Winline\
 -Wstrict-aliasing=2 -Wundef" &&
CX_STRICT="$CF_STRICT -Weffc++ -Wsynth -Wold-style-cast" ])

CFLAGS="-D_DEFAULT_SOURCE $CF_ORG $C_COMMON$CF_OPTIMIZE$CF_STRICT"
CXXFLAGS="$CX_ORG $CX_COMMON$CX_OPTIMIZE$CX_STRICT"

AC_CHECK_DECL(
  FALLOC_FL_PUNCH_HOLE,
  [],
  [AC_MSG_ERROR([fallocate and FALLOC_FL_PUNCH_HOLE must be supported])],
  [[#include <linux/falloc.h>]])

AC_CONFIG_FILES([Makefile 3rd_party/MessagePack/Makefile server/tr-cpp/Makefile utility/Makefile server/Makefile])
AC_OUTPUT
