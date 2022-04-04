#!/bin/sh -xe

# Pull and run image docker opensuse
sudo apt-get update
docker pull opensuse/leap:${OPENSUSE_VERSION}
docker run --name ${DOCKER_CONTAINER_NAME_OPENSUSE} -ti -d -v `pwd`:/griddb --env GS_LOG=/griddb/log --env GS_HOME=/griddb opensuse/leap:${OPENSUSE_VERSION}

# Install gcc-4.8 and g++-4.8
docker exec ${DOCKER_CONTAINER_NAME_OPENSUSE} /bin/bash -xec "zypper addrepo https://download.opensuse.org/repositories/devel:gcc/openSUSE_Leap_15.2/devel:gcc.repo \
&& zypper --non-interactive --no-gpg-checks --quiet ref \
&& zypper --non-interactive --no-gpg-checks --quiet install --auto-agree-with-licenses gcc48 \
&& zypper --non-interactive --no-gpg-checks --quiet install --auto-agree-with-licenses gcc48-c++"

# Install dependency, support for griddb server
docker exec ${DOCKER_CONTAINER_NAME_OPENSUSE} /bin/bash -xec "zypper install -y make automake autoconf libpng16-devel java-11-openjdk ant zlib-devel tcl net-tools python3"

# Create softlink gcc g++
docker exec ${DOCKER_CONTAINER_NAME_OPENSUSE} /bin/bash -xec "ln -sf /usr/bin/g++-4.8 /usr/bin/g++ \
&& ln -sf /usr/bin/gcc-4.8 /usr/bin/gcc"

# Config server
docker exec ${DOCKER_CONTAINER_NAME_OPENSUSE} /bin/bash -c "cd griddb \
&& ./bootstrap.sh \
&& ./configure \
&& make \
&& bin/gs_passwd ${GRIDDB_USERNAME} -p ${GRIDDB_PASSWORD} \
&& sed -i 's/\"clusterName\":\"\"/\"clusterName\":\"${GRIDDB_CLUSTER_NAME}\"/g' conf/gs_cluster.json"

# Start server with non-root user
docker exec -u 1001:1001 ${DOCKER_CONTAINER_NAME_OPENSUSE} bash -c "cd griddb \
&& bin/gs_startnode -u ${GRIDDB_USERNAME}/${GRIDDB_PASSWORD} -w \
&& bin/gs_joincluster -c ${GRIDDB_CLUSTER_NAME} -u ${GRIDDB_USERNAME}/${GRIDDB_PASSWORD} -w"

# Run sample
docker exec ${DOCKER_CONTAINER_NAME_OPENSUSE} /bin/bash -c "export CLASSPATH=${CLASSPATH}:/griddb/bin/gridstore.jar \
&& mkdir gsSample \
&& cp /griddb/docs/sample/program/Sample1.java gsSample/. \
&& javac gsSample/Sample1.java && java gsSample/Sample1 ${GRIDDB_NOTIFICATION_ADDRESS} ${GRIDDB_NOTIFICATION_PORT} ${GRIDDB_CLUSTER_NAME} ${GRIDDB_USERNAME} ${GRIDDB_PASSWORD}"

# Stop server with non-root user
docker exec -u 1001:1001 ${DOCKER_CONTAINER_NAME_OPENSUSE} bash -c "cd griddb \
&& bin/gs_stopcluster -u ${GRIDDB_USERNAME}/${GRIDDB_PASSWORD} -w \
&& bin/gs_stopnode -u ${GRIDDB_USERNAME}/${GRIDDB_PASSWORD} -w"
