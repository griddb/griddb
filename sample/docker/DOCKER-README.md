## Quick start

### Building GridDB image

After getting Dockerfile, run docker build to build GridDB image:

    $ docker build -t griddb-centos7 .

### Running GridDB image

You can run container from IMAGE ID. After then, start GridDB server

    $ docker run --net=host -it <IMAGE ID> /bin/bash -l

    $ gs_startnode
    $ gs_joincluster -c dockerGridDB -u admin/admin

### Execute a sample program

    $ export CLASSPATH=${CLASSPATH}:/usr/share/java/gridstore.jar
    $ mkdir gsSample
    $ cp /usr/griddb-X.X.X/docs/sample/program/Sample1.java gsSample/.
    $ javac gsSample/Sample1.java
    $ java gsSample/Sample1 239.0.0.1 31999 dockerGridDB admin admin
      --> Person:  name=name02 status=false count=2 lob=[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]

    *: X.X.X is the GridDB version 
