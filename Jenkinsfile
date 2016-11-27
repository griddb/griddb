node {
    stage('CentOS 6.8 Build') {
        docker.image('centos:6.8_dev').inside {
            git 'https://github.com/griddb/griddb_nosql.git'
            sh './bootstrap.sh; ./configure; make; rm -rf *.o'
        }
    }

    stage('CentOS 7.2.1511 Build') {
        docker.image('centos:7.2.1511_dev').inside {
            git 'https://github.com/griddb/griddb_nosql.git'
            sh './bootstrap.sh; ./configure; make; rm -rf *.o'
        }
    }
}
