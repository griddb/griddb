Put an archive file (griddb_nosql-2.8.0.zip) on SOURCES directory.

For example, input the following command:

    $ rpmbuild --define="_topdir `pwd`" -bb --clean SPECS/griddb.spec
    
You can get a RPM file on RPMS directory.
