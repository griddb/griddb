#!/bin/bash
#Function update gs_cluster.json file for initial set update
  griddb_instdir=$1
  gs_cluster_json_file=$griddb_instdir/conf/gs_cluster.json
  temp_file=$griddb_instdir/conf/temp.json
  
  touch $griddb_instdir/conf/temp.json
  
  #check GDB_CLUSTER_NAME
  cluster=""
  if [[ ${GDB_CLUSTER_NAME} ]]; then
     cluster=${GDB_CLUSTER_NAME}
  else
     cluster=myCluster
  fi
  
  
  #check GDB_FIXED_IPADDR, if it is set, add fixed list property with default port to gs_cluster.json
  property=""
  notificationMember=""
  if [[ ${GDB_FIXED_IPADDR} ]]; then
    property=${GDB_FIXED_IPADDR}
    ipList=(${property//,/ })
    len=${#ipList[@]}
    if [[ $len > 0 ]]; then
     notificationMember+="\"notificationMember\":["
     for i in "${ipList[@]}"
        do
           temp="{\"cluster\":{\"address\":\"$i\",\"port\":10010},
              \"sync\":{\"address\":\"$i\",\"port\":10020},
              \"system\":{\"address\":\"$i\",\"port\":10040},
              \"transaction\":{\"address\":\"$i\",\"port\":10001},
              \"sql\":{\"address\":\"$i\",\"port\":20001}
               }"
           if [[ $i != ${ipList[-1]} ]]; then
             notificationMember+="$temp, "
          else
             notificationMember+="$temp"
          fi
        done
     notificationMember+="],"
  fi
fi
 #change gs_cluster.json file
 temp=""
 while IFS= read -r line; do
  if [[ $line == *"clusterName"* ]]; then
    temp="\"clusterName\":\"${cluster}\","
  elif [[ $line == *'replicationNum'* ]] && [[ $notificationMember != "" ]]; then
    temp=$notificationMember
  elif [[ $line == *'notification'* ]]; then
     if [[ $notificationMember != "" ]]; then
        temp=""
     else
        temp=$line
     fi
   else
     temp=$line
   fi
  echo $temp >> $temp_file
 done < $gs_cluster_json_file
 rm $gs_cluster_json_file
 chmod u+x $griddb_instdir/conf/json_pretty.sh
 chmod u+x $temp_file
 cat $temp_file | $griddb_instdir/conf/json_pretty.sh >> $gs_cluster_json_file
 rm $temp_file

