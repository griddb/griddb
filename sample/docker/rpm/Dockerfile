FROM centos:7

RUN yum install -y zip unzip zlib-devel ant \
    && yum groupinstall -y "Development Tools" \
    && yum clean all

RUN cd /root/ \
    && git clone https://github.com/griddb/griddb_nosql.git \
    && cd griddb_nosql \
    && ./bootstrap.sh \
    && ./configure \
    && make \
    && make dist-zip \
    && cp griddb_nosql-3.0.1.zip installer/SOURCES/ \
    && cd installer \
    && rpmbuild --define="_topdir `pwd`" -bb --clean SPECS/griddb.spec \
    && rm -rf *.o

VOLUME /root/griddb_nosql
