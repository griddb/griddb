#!/bin/bash

cd $(dirname "$0")

LIB_DIR=`pwd`
LIB_ROOT_DIR=`dirname $LIB_DIR`
LIB_INSTALL_DIR=$LIB_DIR/install


NUM_JOB=$1

if [ -d $LIB_INSTALL_DIR ]
then
    echo "already installed at $LIB_INSTALL_DIR"
    exit 0
fi

SUCCESS=0

while :
do
    rm -rf install

    cd ./org
    ./buildconf || break
    ./configure --prefix=$LIB_INSTALL_DIR || break
    make -j ${NUM_JOB:=4}
    
    mkdir -p install

    make install || break

    SUCCESS=1
    break
done

cd ../..

if [ $SUCCESS -eq 1 ]
then
    echo "install succeeded."
    exit 0
else
    echo "install falied."
    exit 1
fi
