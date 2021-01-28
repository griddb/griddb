%define griddb_name griddb
%define griddb_ver 4.5.3
%define griddb_instdir /usr/griddb-%{griddb_ver}
%define griddb_homedir /var/lib/gridstore
# do not strip
%define __spec_install_post /usr/lib/rpm/brp-compress

%define debug_package %{nil}

Name:           %{griddb_name}
Summary:        GridDB Community Edition
Version:        %{griddb_ver}
Release:        linux
Group:          Applications/Databases
Vendor:         Toshiba Digital Solutions Corporation
License:        AGPL-3.0 (and Apache-2.0)
Source:         %{name}-%{version}.zip

%description
GridDB is Database for IoT with both NoSQL interface and SQL Interface.

%prep
%setup -q

%build

%install
rm -rf %{buildroot}

# Install files
mkdir -p %{buildroot}%{griddb_instdir}/bin
mkdir -p %{buildroot}%{griddb_instdir}/conf
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/MessagePack
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/activemq-cpp-library
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/apr
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/ebb
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/picojson
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/purewell
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/sha2
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/slf4j
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/yield
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/json-simple
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/uuid
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/omaha
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/zigzag_encoding
mkdir -p %{buildroot}%{griddb_instdir}/docs
mkdir -p %{buildroot}%{griddb_instdir}/docs/sample
mkdir -p %{buildroot}%{griddb_instdir}/docs/sample/program
mkdir -p %{buildroot}%{griddb_homedir}
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/usr/share/java

install -c -m 750 bin/gsserver             %{buildroot}%{griddb_instdir}/bin

install -c -m 700 bin/gs_adduser           %{buildroot}%{griddb_instdir}/bin
install -c -m 700 bin/gs_deluser           %{buildroot}%{griddb_instdir}/bin
install -c -m 700 bin/gs_passwd            %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_joincluster       %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_leavecluster      %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_startnode         %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stat              %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stopcluster       %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stopnode          %{buildroot}%{griddb_instdir}/bin
install -c -m 640 bin/log.py               %{buildroot}%{griddb_instdir}/bin
install -c -m 640 bin/util.py              %{buildroot}%{griddb_instdir}/bin

install -c -m 644 bin/gridstore.jar        %{buildroot}%{griddb_instdir}/bin/gridstore-%{version}.jar
install -c -m 644 bin/gridstore-conf.jar   %{buildroot}%{griddb_instdir}/bin/gridstore-conf-%{version}.jar

install -c -m 640 conf/gs_cluster.json     %{buildroot}%{griddb_instdir}/conf
install -c -m 640 conf/gs_node.json        %{buildroot}%{griddb_instdir}/conf
install -c -m 640 conf/password            %{buildroot}%{griddb_instdir}/conf

install -c -m 640 3rd_party/3rd_party.md                        %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/Apache_License-2.0.txt              %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/BSD_License.txt                     %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/MIT_License.txt                     %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/MessagePack/COPYING                 %{buildroot}%{griddb_instdir}/3rd_party/MessagePack
install -c -m 640 3rd_party/activemq-cpp-library/org/NOTICE.txt %{buildroot}%{griddb_instdir}/3rd_party/activemq-cpp-library
install -c -m 640 3rd_party/apr/org/NOTICE                      %{buildroot}%{griddb_instdir}/3rd_party/apr
install -c -m 640 3rd_party/ebb/LICENSE                         %{buildroot}%{griddb_instdir}/3rd_party/ebb
install -c -m 640 3rd_party/picojson/org/include/README.mkdn    %{buildroot}%{griddb_instdir}/3rd_party/picojson
install -c -m 640 3rd_party/purewell/purewell.txt               %{buildroot}%{griddb_instdir}/3rd_party/purewell
install -c -m 640 3rd_party/sha2/README                         %{buildroot}%{griddb_instdir}/3rd_party/sha2
install -c -m 640 3rd_party/slf4j/LICENSE.txt                   %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/slf4j/slf4j-api-1.7.7.jar           %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/slf4j/slf4j-jdk14-1.7.7.jar         %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/yield/yield.txt                     %{buildroot}%{griddb_instdir}/3rd_party/yield
install -c -m 640 3rd_party/json-simple/fangyidong/LICENSE.txt  %{buildroot}%{griddb_instdir}/3rd_party/json-simple
install -c -m 640 3rd_party/uuid/uuid/COPYING                   %{buildroot}%{griddb_instdir}/3rd_party/uuid
install -c -m 640 3rd_party/omaha/COPYING                       %{buildroot}%{griddb_instdir}/3rd_party/omaha
install -c -m 640 3rd_party/zigzag_encoding/LICENSE             %{buildroot}%{griddb_instdir}/3rd_party/zigzag_encoding

install -c -m 640 README.md                                     %{buildroot}%{griddb_instdir}
install -c -m 644 docs/sample/program/Sample1.java              %{buildroot}%{griddb_instdir}/docs/sample/program

# Install symbolic links
ln -sf %{griddb_instdir}/bin/gsserver              %{buildroot}/usr/bin

ln -sf %{griddb_instdir}/bin/gs_adduser            %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_deluser            %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_passwd             %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_joincluster        %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_leavecluster       %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_startnode          %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_stat               %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_stopcluster        %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_stopnode           %{buildroot}/usr/bin

ln -sf %{griddb_instdir}/bin/gridstore-%{version}.jar         %{buildroot}/usr/share/java/gridstore.jar
ln -sf %{griddb_instdir}/bin/gridstore-conf-%{version}.jar    %{buildroot}/usr/share/java/gridstore-conf.jar

%pre

# Installation error if gsserver process exists
if [ "$1" = "2" ]; then
  PROC=`/usr/bin/pgrep gsserver `
  if [ x"${PROC}" != x"" ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "Installation Error:"
    echo "  GridDB server is running. Please stop GridDB server."
    echo "------------------------------------------------------------"
    echo ""
    exit 1
  fi
fi

# Register user and group
GROUPID=`/bin/awk -F: '{if ($1 == "gsadm") print $4}' < /etc/passwd`
if [ x"${GROUPID}" != x"" ]; then
  GROUPNAME=`/bin/egrep :+$GROUPID+: /etc/group|cut -f1 -d:`
  if [ x"${GROUPNAME}" != x"gridstore" ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "Installation Error:"
    echo "  Existing user gsadm is not in group gridstore."
    echo "  Please try again after you had registered user and group"
    echo "  correctly."
    echo "------------------------------------------------------------"
    echo ""
    exit 1
  else
    echo ""
    echo "------------------------------------------------------------"
    echo "Information:"
    echo "  User and group have already been registered correctly."
    echo "  GridDB uses existing user and group."
    echo "------------------------------------------------------------"
    echo ""
  fi
else
  GROUPNAME=`/bin/awk -F: '{if ( $1 == "gridstore" ) print $1}' < /etc/group`
  if [ x"${GROUPNAME}" != x"" ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "Installation Error:"
    echo "  Group gridstore has already been registered."
    echo "  Please try again after you had registered user and group"
    echo "  correctly."
    echo "------------------------------------------------------------"
    echo ""
    exit 1
  else
    groupadd -g 124 -o -r gridstore >/dev/null 2>&1 || :
    useradd -M -N -g gridstore -o -r -d %{griddb_homedir} -s /bin/bash \
		-c "GridDB" -u 124 gsadm >/dev/null 2>&1 || :
    echo ""
    echo "------------------------------------------------------------"
    echo "Information:"
    echo "  User gsadm and group gridstore have been registered."
    echo "  GridDB uses new user and group."
    echo "------------------------------------------------------------"
    echo ""
  fi
fi

%post
# Make home directories when not exists
if [ ! -e %{griddb_homedir}/log ]; then
  mkdir -p %{griddb_homedir}/log
  chown gsadm:gridstore %{griddb_homedir}/log
fi
if [ ! -e %{griddb_homedir}/data ]; then
  mkdir -p %{griddb_homedir}/data
  chown gsadm:gridstore %{griddb_homedir}/data
fi
if [ ! -e %{griddb_homedir}/backup ]; then
  mkdir -p %{griddb_homedir}/backup
  chown gsadm:gridstore %{griddb_homedir}/backup
fi
if [ ! -e %{griddb_homedir}/conf ]; then
  mkdir -p %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf
fi

# Copy definition files when not exists
if [ ! -e %{griddb_homedir}/conf/gs_cluster.json ]; then
  cp %{griddb_instdir}/conf/gs_cluster.json %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/gs_cluster.json
fi
if [ ! -e %{griddb_homedir}/conf/gs_node.json ]; then
  cp %{griddb_instdir}/conf/gs_node.json %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/gs_node.json
fi
if [ ! -e %{griddb_homedir}/conf/password ]; then
  cp %{griddb_instdir}/conf/password %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/password
fi

# Create .bash_profile for gsadm user
GSADMHOME=`/bin/awk -F: '{if ($1 == "gsadm") print $6}' < /etc/passwd`
BASHPROF=`/bin/ls -a ${GSADMHOME} | /bin/grep .bash_profile `
if [ x"${BASHPROF}" = x"" ]; then
 echo "# .bash_profile

[ -f /etc/profile ] && source /etc/profile

# GridDB specific environment variables
GS_LOG=/var/lib/gridstore/log
export GS_LOG
GS_HOME=/var/lib/gridstore
export GS_HOME

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

# User specific environment and startup programs

" >  /var/lib/gridstore/.bash_profile
 chown gsadm:gridstore /var/lib/gridstore/.bash_profile
fi

%files

%defattr(755,gsadm,gridstore)
%dir %{griddb_instdir}
%dir %{griddb_instdir}/bin
%dir %{griddb_instdir}/conf
%dir %{griddb_instdir}/3rd_party
%dir %{griddb_instdir}/3rd_party/MessagePack
%dir %{griddb_instdir}/3rd_party/activemq-cpp-library
%dir %{griddb_instdir}/3rd_party/apr
%dir %{griddb_instdir}/3rd_party/ebb
%dir %{griddb_instdir}/3rd_party/picojson
%dir %{griddb_instdir}/3rd_party/purewell
%dir %{griddb_instdir}/3rd_party/sha2
%dir %{griddb_instdir}/3rd_party/slf4j
%dir %{griddb_instdir}/3rd_party/yield
%dir %{griddb_instdir}/3rd_party/json-simple
%dir %{griddb_instdir}/3rd_party/uuid
%dir %{griddb_instdir}/3rd_party/omaha
%dir %{griddb_instdir}/3rd_party/zigzag_encoding
%dir %{griddb_instdir}/docs
%dir %{griddb_instdir}/docs/sample
%dir %{griddb_instdir}/docs/sample/program
%dir %{griddb_homedir}

%defattr(-,gsadm,gridstore)
%{griddb_instdir}/bin/gsserver
%{griddb_instdir}/bin/gs_adduser
%{griddb_instdir}/bin/gs_deluser
%{griddb_instdir}/bin/gs_passwd
%{griddb_instdir}/bin/gs_joincluster
%{griddb_instdir}/bin/gs_leavecluster
%{griddb_instdir}/bin/gs_startnode
%{griddb_instdir}/bin/gs_stat
%{griddb_instdir}/bin/gs_stopcluster
%{griddb_instdir}/bin/gs_stopnode
%{griddb_instdir}/bin/log.py
%{griddb_instdir}/bin/util.py
%{griddb_instdir}/bin/gridstore-%{version}.jar
%{griddb_instdir}/bin/gridstore-conf-%{version}.jar
%{griddb_instdir}/conf/gs_cluster.json
%{griddb_instdir}/conf/gs_node.json
%{griddb_instdir}/conf/password
%{griddb_instdir}/3rd_party/3rd_party.md
%{griddb_instdir}/3rd_party/Apache_License-2.0.txt
%{griddb_instdir}/3rd_party/BSD_License.txt
%{griddb_instdir}/3rd_party/MIT_License.txt
%{griddb_instdir}/3rd_party/MessagePack/COPYING
%{griddb_instdir}/3rd_party/activemq-cpp-library/NOTICE.txt
%{griddb_instdir}/3rd_party/apr/NOTICE
%{griddb_instdir}/3rd_party/ebb/LICENSE
%{griddb_instdir}/3rd_party/picojson/README.mkdn
%{griddb_instdir}/3rd_party/purewell/purewell.txt
%{griddb_instdir}/3rd_party/sha2/README
%{griddb_instdir}/3rd_party/slf4j/LICENSE.txt
%{griddb_instdir}/3rd_party/slf4j/slf4j-api-1.7.7.jar
%{griddb_instdir}/3rd_party/slf4j/slf4j-jdk14-1.7.7.jar
%{griddb_instdir}/3rd_party/yield/yield.txt
%{griddb_instdir}/3rd_party/json-simple/LICENSE.txt
%{griddb_instdir}/3rd_party/uuid/COPYING
%{griddb_instdir}/3rd_party/omaha/COPYING
%{griddb_instdir}/3rd_party/zigzag_encoding/LICENSE
%{griddb_instdir}/README.md
%{griddb_instdir}/docs/sample/program/Sample1.java
/usr/bin/gsserver
/usr/bin/gs_adduser
/usr/bin/gs_deluser
/usr/bin/gs_passwd
/usr/bin/gs_joincluster
/usr/bin/gs_leavecluster
/usr/bin/gs_startnode
/usr/bin/gs_stat
/usr/bin/gs_stopcluster
/usr/bin/gs_stopnode
/usr/share/java/gridstore.jar
/usr/share/java/gridstore-conf.jar

%changelog
* Thu Jan 28 2021 Toshiba Digital Solutions Corporation
- 4.5.3
