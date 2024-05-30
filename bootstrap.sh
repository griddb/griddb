#!/bin/sh

touch AUTHORS COPYING ChangeLog INSTALL NEWS README

AM_VER="$(automake --version | head -n1 | grep -Eo '[0-9\.]+')"
AM_VER_MAJOR="$(echo "$AM_VER" | cut -d. -f1)"
AM_VER_MINOR="$(echo "$AM_VER" | cut -d. -f2)"
AM_VER_MAJOR_MIN=1
AM_VER_MINOR_MIN=16

if [ "$AM_VER_MAJOR" -gt "$AM_VER_MAJOR_MIN" ] || ( [ "$AM_VER_MAJOR" -eq "$AM_VER_MAJOR_MIN" ] && [ "$AM_VER_MINOR" -ge "$AM_VER_MINOR_MIN" ] )
then
    AM_INIT_AUTOMAKE="AM_INIT_AUTOMAKE([subdir-objects])"
    echo "bootstrap.sh: automake version $AM_VER"
    cat ./configure.ac | sed "s/^AM_INIT_AUTOMAKE.*$/${AM_INIT_AUTOMAKE}/" > ./tmp_configure.ac
    mv ./tmp_configure.ac ./configure.ac
fi

aclocal
autoconf
automake -a -c
