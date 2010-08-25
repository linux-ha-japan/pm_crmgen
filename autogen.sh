#!/bin/sh

aclocal
automake -a --foreign
autoconf
rm -rf autom4te.cache
