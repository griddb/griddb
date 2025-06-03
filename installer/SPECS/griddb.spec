%define griddb_name griddb
%define griddb_ver 5.8.0
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
mkdir -p %{buildroot}%{griddb_instdir}/conf_multicast
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/MessagePack
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
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/fletcher32_simd
mkdir -p %{buildroot}%{griddb_instdir}/3rd_party/zstd
mkdir -p %{buildroot}%{griddb_instdir}/docs
mkdir -p %{buildroot}%{griddb_instdir}/docs/sample
mkdir -p %{buildroot}%{griddb_instdir}/docs/sample/program
mkdir -p %{buildroot}%{griddb_instdir}/lib
mkdir -p %{buildroot}%{griddb_homedir}
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/usr/share/java
mkdir -p %{buildroot}/usr/griddb/bin
mkdir -p %{buildroot}/usr/lib/systemd/system

install -c -m 750 bin/gsserver                        %{buildroot}%{griddb_instdir}/bin
install -c -m 700 bin/gs_adduser                      %{buildroot}%{griddb_instdir}/bin
install -c -m 700 bin/gs_deluser                      %{buildroot}%{griddb_instdir}/bin
install -c -m 700 bin/gs_passwd                       %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_joincluster                  %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_leavecluster                 %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_startnode                    %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stat                         %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stopcluster                  %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_stopnode                     %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_checkpoint                   %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_config                       %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_logconf                      %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_logs                         %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_paramconf                    %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_partition                    %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_backup                       %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_restore                      %{buildroot}%{griddb_instdir}/bin
install -c -m 750 bin/gs_backuplist                   %{buildroot}%{griddb_instdir}/bin
install -c -m 640 bin/util_client.py                  %{buildroot}%{griddb_instdir}/bin
install -c -m 640 bin/log.py                          %{buildroot}%{griddb_instdir}/bin
install -c -m 640 bin/util.py                         %{buildroot}%{griddb_instdir}/bin
install -c -m 644 bin/gridstore.jar                   %{buildroot}%{griddb_instdir}/lib/gridstore-%{version}.jar
install -c -m 644 bin/gridstore-conf.jar              %{buildroot}%{griddb_instdir}/lib/gridstore-conf-%{version}.jar
install -c -m 750 service/bin/gridstore               %{buildroot}%{griddb_instdir}/bin/

install -c -m 640 conf/gs_cluster.json     %{buildroot}%{griddb_instdir}/conf_multicast
install -c -m 640 conf/gs_node.json        %{buildroot}%{griddb_instdir}/conf_multicast
install -c -m 640 conf/password            %{buildroot}%{griddb_instdir}/conf_multicast

install -c -m 640 installer/conf/gs_cluster.json     %{buildroot}%{griddb_instdir}/conf
install -c -m 640 installer/conf/gs_node.json        %{buildroot}%{griddb_instdir}/conf
install -c -m 640 installer/conf/password            %{buildroot}%{griddb_instdir}/conf
install -c -m 640 installer/conf/initGsh             %{buildroot}%{griddb_instdir}/conf
install -c -m 640 service/conf/gridstore.conf        %{buildroot}%{griddb_instdir}/conf
install -c -m 640 service/lib/systemd/system/gridstore.service                      %{buildroot}/usr/lib/systemd/system

install -c -m 640 3rd_party/3rd_party.md                        %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/Apache_License-2.0.txt              %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/BSD_License.txt                     %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/MIT_License.txt                     %{buildroot}%{griddb_instdir}/3rd_party
install -c -m 640 3rd_party/MessagePack/COPYING                 %{buildroot}%{griddb_instdir}/3rd_party/MessagePack
install -c -m 640 3rd_party/ebb/LICENSE                         %{buildroot}%{griddb_instdir}/3rd_party/ebb
install -c -m 640 3rd_party/purewell/purewell.txt               %{buildroot}%{griddb_instdir}/3rd_party/purewell
install -c -m 640 3rd_party/sha2/README                         %{buildroot}%{griddb_instdir}/3rd_party/sha2
install -c -m 640 3rd_party/slf4j/LICENSE.txt                   %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/slf4j/slf4j-api-1.7.7.jar           %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/slf4j/slf4j-jdk14-1.7.7.jar         %{buildroot}%{griddb_instdir}/3rd_party/slf4j
install -c -m 640 3rd_party/yield/yield.txt                     %{buildroot}%{griddb_instdir}/3rd_party/yield
install -c -m 640 3rd_party/omaha/COPYING                       %{buildroot}%{griddb_instdir}/3rd_party/omaha
install -c -m 640 3rd_party/zigzag_encoding/LICENSE             %{buildroot}%{griddb_instdir}/3rd_party/zigzag_encoding
install -c -m 640 3rd_party/picojson/org/include/README.mkdn                %{buildroot}%{griddb_instdir}/3rd_party/picojson
install -c -m 640 3rd_party/json-simple/fangyidong/LICENSE.txt              %{buildroot}%{griddb_instdir}/3rd_party/json-simple
install -c -m 640 3rd_party/uuid/uuid/COPYING                               %{buildroot}%{griddb_instdir}/3rd_party/uuid
install -c -m 640 3rd_party/fletcher32_simd/fletcher32_simd/LICENSE         %{buildroot}%{griddb_instdir}/3rd_party/fletcher32_simd
install -c -m 640 3rd_party/zstd/LICENSE                        %{buildroot}%{griddb_instdir}/3rd_party/zstd



install -c -m 640 README.md                                     %{buildroot}%{griddb_instdir}
install -c -m 644 docs/sample/program/Sample1.java              %{buildroot}%{griddb_instdir}/docs/sample/program
install -c -m 644 docs/sample/program/Sample2.java              %{buildroot}%{griddb_instdir}/docs/sample/program
install -c -m 644 docs/sample/program/Sample3.java              %{buildroot}%{griddb_instdir}/docs/sample/program
install -c -m 644 docs/sample/program/SampleFetchAll.java              %{buildroot}%{griddb_instdir}/docs/sample/program
install -c -m 644 docs/sample/program/SampleMultiGet.java              %{buildroot}%{griddb_instdir}/docs/sample/program
install -c -m 644 docs/sample/program/SampleMultiPut.java              %{buildroot}%{griddb_instdir}/docs/sample/program


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
ln -sf %{griddb_instdir}/bin/gs_checkpoint         %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_config             %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_logconf            %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_logs               %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_paramconf          %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_partition          %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_backup             %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_restore            %{buildroot}/usr/bin
ln -sf %{griddb_instdir}/bin/gs_backuplist         %{buildroot}/usr/bin

ln -sf %{griddb_instdir}/bin/log.py                %{buildroot}%{griddb_instdir}/bin/logs.py
ln -sf %{griddb_instdir}/bin/util_client.py        %{buildroot}%{griddb_instdir}/bin/util_server.py
ln -sf %{griddb_instdir}/bin/gridstore             %{buildroot}/usr/griddb/bin
ln -sf %{griddb_instdir}/lib/gridstore-%{version}.jar         %{buildroot}/usr/share/java/gridstore.jar
ln -sf %{griddb_instdir}/lib/gridstore-conf-%{version}.jar    %{buildroot}/usr/share/java/gridstore-conf.jar


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
    groupadd -g 1224 -o -r gridstore >/dev/null 2>&1 || :
    useradd -M -N -g gridstore -o -r -d %{griddb_homedir} -s /bin/bash \
               -c "GridDB" -u 1224 gsadm >/dev/null 2>&1 || :
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
if [ ! -e /etc/sysconfig/gridstore ]; then
  mkdir -p /etc/sysconfig/gridstore
  chown gsadm:gridstore /etc/sysconfig/gridstore
fi

# Copy definition files when not exists
#%{griddb_instdir}/conf/changeCluster.sh %{griddb_instdir}
if [ ! -e %{griddb_homedir}/conf/gs_cluster.json ]; then
  cp %{griddb_instdir}/conf/gs_cluster.json %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/gs_cluster.json
fi



if [ ! -e %{griddb_homedir}/conf/gs_node.json ]; then
  cp %{griddb_instdir}/conf/gs_node.json %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/gs_node.json
fi

# Generate /var/lib/gridstore/.gsshrc file in case the python3 installed 
if [ -f /usr/bin/python3 ]; then
  /usr/bin/python3 %{griddb_instdir}/conf/initGsh
  if [ -f /var/lib/gridstore/.gsshrc ]; then
    chown gsadm:gridstore /var/lib/gridstore/.gsshrc
  fi
fi

if [ ! -e /etc/sysconfig/gridstore/gridstore.conf ]; then
  cp %{griddb_instdir}/conf/gridstore.conf /etc/sysconfig/gridstore/gridstore.conf
  chown gsadm:gridstore /etc/sysconfig/gridstore/gridstore.conf
fi

if [ ! -e %{griddb_homedir}/conf/password ]; then
  cp %{griddb_instdir}/conf/password %{griddb_homedir}/conf
  chown gsadm:gridstore %{griddb_homedir}/conf/password
fi

# Enabled and reload gridstore service 
systemctl enable gridstore.service

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

%preun
if [ "$1" = "0" ]; then
  # Uninstallation
  # If gsserver process exists
  SVR_PROC=`/usr/bin/pgrep gsserver`
  if [ x"${SVR_PROC}" != x"" ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "Uninstallation Error:"
    echo "  GridDB server is running. Please stop GridDB server."
    echo "------------------------------------------------------------"
    echo ""
    exit 1
  fi
  /usr/bin/systemctl disable gridstore
elif [ "$1" = "1" ]; then
  # Upgrade or Update
  echo ""
fi

%files

%defattr(755,gsadm,gridstore)
%dir %{griddb_instdir}
%dir %{griddb_instdir}/bin
%dir %{griddb_instdir}/conf
%dir %{griddb_instdir}/conf_multicast
%dir %{griddb_instdir}/3rd_party
%dir %{griddb_instdir}/3rd_party/MessagePack
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
%dir %{griddb_instdir}/3rd_party/fletcher32_simd
%dir %{griddb_instdir}/3rd_party/zstd
%dir %{griddb_instdir}/docs
%dir %{griddb_instdir}/docs/sample
%dir %{griddb_instdir}/docs/sample/program
%dir %{griddb_instdir}/lib
%dir /usr/griddb/bin
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
%{griddb_instdir}/bin/gs_checkpoint
%{griddb_instdir}/bin/gs_config
%{griddb_instdir}/bin/gs_logconf
%{griddb_instdir}/bin/gs_logs
%{griddb_instdir}/bin/gs_paramconf
%{griddb_instdir}/bin/gs_partition
%{griddb_instdir}/bin/gs_backup
%{griddb_instdir}/bin/gs_restore
%{griddb_instdir}/bin/gs_backuplist
%{griddb_instdir}/bin/logs.py
%{griddb_instdir}/bin/util_client.py
%{griddb_instdir}/bin/util_server.py
%{griddb_instdir}/bin/log.py
%{griddb_instdir}/bin/util.py
%{griddb_instdir}/bin/gridstore
%{griddb_instdir}/lib/gridstore-%{version}.jar
%{griddb_instdir}/lib/gridstore-conf-%{version}.jar
%{griddb_instdir}/conf/gs_cluster.json
%{griddb_instdir}/conf/gs_node.json
%{griddb_instdir}/conf/password
%{griddb_instdir}/conf_multicast/gs_cluster.json
%{griddb_instdir}/conf_multicast/gs_node.json
%{griddb_instdir}/conf_multicast/password
%{griddb_instdir}/3rd_party/3rd_party.md
%{griddb_instdir}/3rd_party/Apache_License-2.0.txt
%{griddb_instdir}/3rd_party/BSD_License.txt
%{griddb_instdir}/3rd_party/MIT_License.txt
%{griddb_instdir}/3rd_party/MessagePack/COPYING
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
%{griddb_instdir}/3rd_party/fletcher32_simd/LICENSE
%{griddb_instdir}/3rd_party/zstd/LICENSE

%{griddb_instdir}/README.md
%{griddb_instdir}/docs/sample/program/Sample1.java
%{griddb_instdir}/docs/sample/program/Sample2.java
%{griddb_instdir}/docs/sample/program/Sample3.java
%{griddb_instdir}/docs/sample/program/SampleFetchAll.java
%{griddb_instdir}/docs/sample/program/SampleMultiGet.java
%{griddb_instdir}/docs/sample/program/SampleMultiPut.java
%{griddb_instdir}/conf/initGsh
%{griddb_instdir}/conf/gridstore.conf
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
/usr/bin/gs_checkpoint
/usr/bin/gs_config
/usr/bin/gs_logconf
/usr/bin/gs_logs
/usr/bin/gs_paramconf
/usr/bin/gs_partition
/usr/bin/gs_backup
/usr/bin/gs_restore
/usr/bin/gs_backuplist
/usr/share/java/gridstore.jar
/usr/share/java/gridstore-conf.jar
/usr/lib/systemd/system/gridstore.service
/usr/griddb/bin/gridstore

%changelog
* Wed Nov 13 2024 Toshiba Digital Solutions Corporation
- 5.7.0
* Thu May 30 2024 Toshiba Digital Solutions Corporation
- 5.6.0
* Wed Feb 07 2024 Toshiba Digital Solutions Corporation
- 5.5.0
* Mon Nov 13 2023 Toshiba Digital Solutions Corporation
- 5.3.1
* Mon Jun 12 2023 Toshiba Digital Solutions Corporation
- 5.3.0
* Tue Oct 25 2022 Toshiba Digital Solutions Corporation
- 5.1.0
* Thu Apr 21 2022 Toshiba Digital Solutions Corporation
- 5.0.0
* Tue Aug 26 2021 Toshiba Digital Solutions Corporation
- 4.6.1
* Thu Feb 25 2021 Toshiba Digital Solutions Corporation
- 4.6.0
