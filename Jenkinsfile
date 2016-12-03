node {
    stage('CentOS 6.8 Build') {
        docker.image('centos:6.8_dev').inside {
//            git 'https://github.com/griddb/griddb_nosql.git'
            git 'https://github.com/nobusugi246/griddb_nosql'
            sh './bootstrap.sh; ./configure; make; make dist-zip; rm -rf *.o'
        }
    }

    stage('CentOS 7.2.1511 Build') {
        docker.image('centos:7.2.1511_dev').inside {
//            git 'https://github.com/griddb/griddb_nosql.git'
            git 'https://github.com/nobusugi246/griddb_nosql'
            sh './bootstrap.sh; ./configure; make; make dist-zip; rm -rf *.o'
        }
    }
}
