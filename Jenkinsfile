node {
    stage('CentOS 6.8 Build') {
        docker.image('centos:6.8_dev').inside {
            git 'https://github.com/griddb/griddb_nosql.git'
            sh './bootstrap.sh; ./configure; make; make dist-zip; cp griddb-3.0.zip installer/SOURCES/griddb_nosql-3.0.0.zip; cd installer; rpmbuild --define="_topdir `pwd`" -bb --clean SPECS/griddb.spec; rm -rf *.o'
        }
    }

    stage('CentOS 7.2.1511 Build') {
        docker.image('centos:7.2.1511_dev').inside {
            git 'https://github.com/griddb/griddb_nosql.git'
            sh './bootstrap.sh; ./configure; make; make dist-zip; cp griddb-3.0.zip installer/SOURCES/griddb_nosql-3.0.0.zip; cd installer; rpmbuild --define="_topdir `pwd`" -bb --clean SPECS/griddb.spec; rm -rf *.o'
        }
    }
}
