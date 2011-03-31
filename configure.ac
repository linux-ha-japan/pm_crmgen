#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(pm_crmgen, 1.1)
AM_INIT_AUTOMAKE(pm_crmgen, 1.1)

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

prefix_orig="$prefix"
prefix=`eval echo "$prefix"`
case $prefix in
	NONE)	prefix=/usr;;
esac
var(){
	case $1 in
		*'${'*)		res=`eval echo "$1"`;;
		*)		res="$1";;
	esac
	case "$res" in
		""|NONE)	echo "$2";;
		*)		echo "$res";;
	esac
}

grep "release 6" /etc/redhat-release -q
if test $? = 0; then
	rhelver=6
else
	rhelver=5
fi
RHEL_VER="$rhelver"
AC_SUBST(RHEL_VER)

AC_CONFIG_FILES(Makefile	\
	pm_crmgen.spec
)
AC_OUTPUT
