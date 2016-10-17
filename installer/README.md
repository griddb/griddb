Put an archive file (griddb_nosql-X.X.X.zip) on SOURCES directory.

For example, input the following command:

    $ rpmbuild --define="_topdir `pwd`" -bb --clean SPECS/griddb.spec
    
You can get a RPM file on RPMS directory.
