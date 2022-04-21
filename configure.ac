# -*- coding: utf8 -*-

AC_PREREQ([2.59])
AC_INIT([hdf5-ng], [0.0.1])

AC_CONFIG_MACRO_DIR([m4])

LT_PREREQ([2.4.6])
LT_INIT
AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_SRCDIR([src/h5ng.cxx])

# Checks for programs.
AC_PROG_CXX
AC_PROG_LIBTOOL

# check for c++14
AX_CXX_COMPILE_STDCXX_14(noext, mandatory)

AC_CONFIG_FILES([
  Makefile
  src/Makefile
])
AC_OUTPUT
