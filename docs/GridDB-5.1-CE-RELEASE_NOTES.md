# GridDB CE 5.1

## Changes in V5.1

Main changes in GridDB CE V5.1 are as follows:

- Configuration for communication path between client and server

    - Multiple communication paths can now be configured from the GridDB client and server. The new setting makes it possible for GridDB clients to configure access to GridDB cluster, allowing communication via the private and public networks in parallel. (This feature is currently supported by the Java/C client, JDBC driver.)

Note: Until now, the server setting has limited the client to a single communication route, either the internal or external communication route, and the client could not individually select which communication route to use.

---

## Configuration for communication path between client and server

### Settings for GridDB Server

When you use both the external and internal communication,
edit the node definition file and the cluster definition file.

Set the following parameters in the node definition file (gs_node.json).
  * /transaction/publicServiceAddress (New): external communication address for accessing transaction services
  * /sql/publicServiceAddress (New): external communication address for accessing SQL services

Note: /transaction/localServiceAddress, /sql/localServiceAddress have been removed.

Example:

```
{
                 :
                 :
    "transaction":{
        "serviceAddress":"172.17.0.44",
        "publicServiceAddress":"10.45.1.10",        
        "servicePort":10001
    },      
    "sql":{
      "serviceAddress":"172.17.0.44",
      "publicServiceAddress":"10.45.1.10",      
      "servicePort":20001
    },
                 :
                 : 
```

Set the following parameters in the cluster definition file (gs_cluster.json) with fixed list method.
  * /transactionPublic (New): external communication address and port number for accessing transaction services
  * /sqlPublic (New): external communication address and port number for accessing SQL services

Example:

```
{
                             :
                             :
    "cluster":{
        "clusterName":"yourClusterName",
        "replicationNum":2,
        "heartbeatInterval":"5s",
        "loadbalanceCheckInterval":"180s",
        "notificationMember": [
            {
                "cluster": {"address":"172.17.0.44", "port":10010},
                "sync": {"address":"172.17.0.44", "port":10020},
                "system": {"address":"172.17.0.44", "port":10040},
                "transaction": {"address":"172.17.0.44", "port":10001},
                "sql": {"address":"172.17.0.44", "port":20001},
                "transactionPublic": {"address":"10.45.1.10", "port":10001},
                "sqlPublic": {"address":"10.45.1.10", "port":20001}
            }
        ]
    },
                             :
                             :
}
```

### Settings for GridDB Client

When multiple communication routes are configured for the GridDB cluster (server side), the communication route can be selected.
- The internal communication is selected by default.
- When you use the external communication, set "PUBLIC" in the connectionRoute property (New).

Example: 

(Java client)
```Java
Properies prop = new Properties();
props.setProperty("notificationMember", "10.45.1.10:10001");
props.setProperty("connectionRoute", "PUBLIC");
...
GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
```

(JDBC driver)
```
url = "jdbc:gs:///yourClusterName/?notificationMember=10.45.1.10:20001&connectionRoute=PUBLIC"
```
