## Quick start (Using RPM or DEB) with GridDB Service and CLI

  We have confirmed the operation on CentOS 7.9, Ubuntu 18.04 and openSUSE Leap 15.1.

Note:
- Please install Python3 in advance.
- When you install this package, a gsadm OS user are created in the OS.  
  Execute the operating command as the gsadm user.  
- You don't need to set environment vatiable GS_HOME and GS_LOG.
- There is Java client library (gridstore.jar) on /usr/share/java and a sample on /usr/gridb-XXX/docs/sample/programs.
- Default settings is for local connection.
- Cluster name and the password of GridDB Admin user are set. The cluster name is "myCluster" and the password of "admin" user is "admin".
- If old version has been installed, please uninstall and remove conf/ and data/ on /var/lib/gridstore.

### Install

    (CentOS)
    $ sudo rpm -ivh griddb-X.X.X-linux.x86_64.rpm

    (Ubuntu)
    $ sudo dpkg -i griddb_X.X.X_amd64.deb

    (openSUSE)
    $ sudo rpm -ivh griddb-X.X.X-opensuse.x86_64.rpm

    Note: X.X.X is the GridDB version.

### Start a server

    $ sudo systemctl start gridstore

### Start GridDB CLI

When you use SQL Interface, please install [GridDB JDBC Driver](https://github.com/griddb/jdbc).

    $ sudo wget https://repo1.maven.org/maven2/com/github/griddb/gridstore-jdbc/4.6.0/gridstore-jdbc-4.6.0.jar -O /usr/share/java/gridstore-jdbc.jar

And please install and start [GridDB CLI](https://github.com/griddb/cli).

    (CentOS)
    $ sudo rpm -ivh griddb-ce-cli-X.X.X-linux.x86_64.rpm
    $ sudo su - gsadm
    [gsadm]$ gs_sh

    (Ubuntu)
    $ sudo dpkg -i griddb-cli_X.X.X_amd64.deb
    $ sudo su - gsadm
    [gsadm]$ gs_sh

    (openSUSE Leap)
    $ sudo rpm -ivh griddb-cli-X.X.X-opensuse.x86_64.rpm
    $ sudo su - gsadm
    [gsadm]$ gs_sh

### Stop a server
    $ sudo systemctl stop gridstore
