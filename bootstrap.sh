#!/bin/sh

touch AUTHORS COPYING ChangeLog INSTALL NEWS README

aclocal
autoconf
automake -a -c
