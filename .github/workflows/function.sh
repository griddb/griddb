#!/bin/bash

cd java_client

check_file_exist() {
    local file_path=$1
    if [ ! -f "$file_path" ]; then
        echo "$file_path not found!"
    fi
}

# Check file
check_files() {
    check_file_exist "make_source_for_mvn.sh"
    check_file_exist "pom.xml"
    check_file_exist "../docs/sample/program/Sample1.java"
}

# Build maven package java client
build_java_client() {
    check_files
    ./make_source_for_mvn.sh
    mvn package
}

# Create and run griddb server
run_griddb_server() {
    docker run -d --network="host" -e GRIDDB_CLUSTER_NAME=${GRIDDB_SERVER_NAME} griddb/griddb:4.5.2-bionic
}

# Turn off firewall
firewall_disable() {
   sudo ufw disable
}

# Get version java client
get_version() {
    local java_client_version=$(cat pom.xml | grep "version" |  head -n 1 | cut -d ">" -f 2 | cut -d "<" -f 1)
    echo $java_client_version
}

# Get name package java client
get_name() {
    local package_name=$(cat pom.xml | grep "artifactId" |  head -n 1 | cut -d ">" -f 2 | cut -d "<" -f 1)
    echo $package_name
}

run_sample() {
    check_files
    local package_name=$(get_name)
    local java_client_version=$(get_version)
    export CLASSPATH=${CLASSPATH}:target/$package_name-$java_client_version.jar
    mkdir gsSample
    cp ../docs/sample/program/Sample1.java gsSample/.
    javac gsSample/Sample1.java
    java gsSample/Sample1 ${GRIDDB_NOTIFICATION_ADDRESS} ${GRIDDB_NOTIFICATION_PORT} ${GRIDDB_SERVER_NAME} ${GRIDDB_USERNAME} ${GRIDDB_PASSWORD}
}

