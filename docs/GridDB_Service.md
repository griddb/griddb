<a id="griddb_service"></a>
# Service

<a id="preparing_to_use_the_service"></a>
## Preparing to use the service

The procedure to use and install GridDB service is as follows.

1.  Install GridDB server package and client package.
2.  Configure the respective GridDB nodes that constitute a GridDB cluster.
3.  Configure the start configuration file.

The table above shows the kinds of files used in GridDB services.

| Type | Meaning                                          |
|--------------------|----------------------------------------------|
| systemd unit file | systemd unit definition file. It is installed in /usr/lib/systemd/system/gridstore.service by the server package of GridDB and  registered on the system as GridDB service. |
| Service script | Script file is executed automatically during OS startup. <br />It is installed in /usr/griddb/bin/gridstore by the server package of GridDB. |
| PID file | File containing only the process ID (PID) of the gsserver process. This is created in $GS_HOME/conf/gridstore.pid when the gsserver process is started.     |
| Start configuration file | File containing the parameters that can be set while in service. <br />Depending on the GridDB server package, it is installed in /etc/sysconfig/gridstore/gridstore.conf. |


## Parameter setting

A list of parameters is available to control the GridDB service operations. A list of the parameters is given below.

| Property       | Default                          | Note                           |
|--------------|-------------------------------------|--------------------------------|
| GS_USER       | admin                            | GridDB user name               |
| GS_PASSWORD   | admin                            | `GS_USER` password         |
| CLUSTER_NAME  | myCluster | Cluster name to join              |
| MIN_NODE_NUM | 1                                | Number of nodes constituting a cluster   |

To change the parameters, edit the start configuration file (`/etc/sysconfig/gridstore/gridstore.conf` ).

When a server package is updated or uninstalled, the start configuration file will not be overwritten or uninstalled.

[Notes]
-   Do not directly edit a parameter described in service script. The edited file will be lost when the server package is uninstalled or updated.  When changing the parameters, edit the start configuration file.

## Log

See the boot log( `/var/log/boot.log` ) and operating command log(`$GS_HOME/log` ) for details of the service log.


## Command

GridDB service commands are shown below.

### start

Action:

-   Start a node and join to a cluster.

``` example
$ sudo systemctl start gridstore
```

-   This function executes gs_startnode command to start a node and gs_joincluster command to join to a cluster.
-   When the gs_startnode command is executed, the system waits for the recovery process to end.
-   When the gs_joincluster command is executed, the system doesn't wait for the cluster to start operation.
-   `Set the cluster name in `CLUSTER_NAME` .
-   `Set the number of nodes constituting a cluster in `MIN_NODE_NUM` .

[Notes]
-   If an error occurs in the middle of a cluster operation, the gsserver process will be stopped.

### stop

Action:

-   Leave from a cluster and stop a node.

``` example
$ sudo systemctl stop gridstore
```

-   End if there are no more processes, and error if the timeout time has passed (termination code 150).
-   If there are no processes started by the service, termination code 0 will be returned.
-   This function executes gs_leavecluster command to leave a node from a cluster before stopping a node.
-   This function executes gs_leavecluster command to leave a node from a cluster before stopping a node.
-   When the gs_leavecluster command is executed, the system waits for the node to leave from the cluster.
-   A node stopping process will be performed. regardless of the termination code of the gs_leavecluster command.

[Notes]
-   **When stopping the cluster, execute the gs_stopcluster command and leave/stop each node by a service stop. If you do not stop the cluster with the gs_stopcluster command, autonomous data arrangement may occur due to node leaving. If data relocation happens frequently, network or disk I/O may become a load. If you leave the node after stopping the cluster, data arrangement will not occur. To prevent unnecessary data arrangement, be sure to stop the cluster. To stop the cluster, execute an operating command gs_stopcluster, integrated operation control gs_admin, gs_sh, etc.
-   A node started by an operating command or command interpreter (gs_sh) cannot be stopped by a service stop.  Use the respective tools to stop the node.

### status

Action:

-   Display whether the node process is under execution or not.

``` example
$ sudo systemctl status gridstore
```

### restart

Action:

-   Stop and start continuously.

### condrestart

Action:

-   Restart if there is a lock file.


## Error message list

Service error messages are as shown below.

| Code   | Message                     | Meaning                                  |
|--------|-----------------------------|--------------------------------------|
| F00003 | Json load error             | Reading of definition file failed.     |
| F01001 | Stop service timed out      | Stop node process timed out.   |
| F01002 | Startnode error             | An error occurred in the node startup process.   |
| F01003 | Startnode timed out         | Start node process timed out.   |
| F01004 | Joincluster error           | An error occurred in the join cluster process. |
| F01005 | Joincluster timed out       | Join cluster process timed out. |
| F01006 | Leavecluster error          | An error occurred in the leave cluster process. |
| F02001 | Command execution error     | An error occurred in the command execution.     |
| F02002 | Command execution timed out | Command execution timed out.     |

[Memo]
-   If an error occurs with each command execution, the operating command error will be displayed and recorded at the same time.  Refer to the item on operating commands (gs_startnode, gs_joincluster, and gs_leavecluster) as well when troubleshooting errors.
