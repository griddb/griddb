FROM centos:7

WORKDIR /root/
RUN yum install -y wget java-1.8.0-openjdk-devel \
    && yum groupinstall -y "Development Tools" \
    && yum clean all \
    && wget -q https://github.com/griddb/griddb_nosql/releases/download/v3.0.1/griddb_nosql-3.0.1-1.linux.x86_64.rpm \
    && rpm -ivh griddb_nosql-3.0.1-1.linux.x86_64.rpm \
    && rm griddb_nosql-3.0.1-1.linux.x86_64.rpm

ENV GS_HOME /var/lib/gridstore
ENV GS_LOG $GS_HOME/log
ENV HOME $GS_HOME

RUN gs_passwd admin -p admin \
    && sed -i -e s/\"clusterName\":\"\"/\"clusterName\":\"dockerGridDB\"/g \
       /var/lib/gridstore/conf/gs_cluster.json

WORKDIR $HOME
USER gsadm
