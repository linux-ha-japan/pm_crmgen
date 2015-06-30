#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(pm_crmgen, 2.1)
dnl Older distros may need: AM_INIT_AUTOMAKE($PACKAGE_NAME, $PACKAGE_VERSION)
AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_LN_S
AC_PREFIX_DEFAULT(/usr)

dnl ===============================================
dnl check for python
dnl ===============================================
AM_PATH_PYTHON(2.4,,:)
AC_PATH_PROGS(PYTHON, python)
AC_MSG_CHECKING(where is python installed)
if test "x${PYTHON}" = x; then
	PYTHON="/usr/bin/env python";
fi
AC_MSG_RESULT(using $PYTHON);

prefix=`eval echo "$prefix"`
case $prefix in
	NONE)	prefix=/usr;;
esac

AC_CONFIG_FILES(Makefile	\
	pm_crmgen.spec
)
AC_OUTPUT
