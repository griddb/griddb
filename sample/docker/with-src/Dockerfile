FROM centos:7.3.1611

RUN yum install -y zip unzip zlib-devel ant \
    && yum groupinstall -y "Development Tools"

RUN cd /root/ \
    && git clone https://github.com/griddb/griddb_nosql.git \
    && cd griddb_nosql \
    && ./bootstrap.sh \
    && ./configure \
    && make \
    && rm -rf *.o

VOLUME /root/griddb_nosql

ENV GS_HOME=/root/griddb_nosql
ENV GS_LOG=/root/griddb_nosql/log

