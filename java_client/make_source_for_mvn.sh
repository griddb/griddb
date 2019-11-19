#!/bin/bash

mkdir -p src/main/java
mkdir -p src/main/resources/com/toshiba/mwcloud/gs/common

./make_source_for_mvn.pl src src/main/java
cp src/com/toshiba/mwcloud/gs/common/LoggingUtils.properties src/main/resources/com/toshiba/mwcloud/gs/common
cp -r src_contrib/com src/main/java

