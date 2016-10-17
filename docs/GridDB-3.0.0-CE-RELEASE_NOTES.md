## GridDB CE 3.0.0

1. Added two new cluster configuration methods (fixed list and provider)
2. Added block data compression

## 1. New cluster configuration methods (fixed list and provider)

GridDB provides two new cluster configuration methods for configuring addresses list.
The different cluster configuration methods can be used depending on the environment or use case. Connection method of client or operational tool is also different depending on the configuration methods.

With using a fixed list method or provider method enables cluster configuration and client connection on the environment where multi-cast is not applicable.

  * Multi-cast method  
    A multi-cast method performs a node of discovery in multi-cast to automatically configure the address list.

  * Fixed list method  
    A fixed list method uses the lists by starting with giving a fixed address list to the cluster definition file. Each GridDB server reads the list only once at node start-up.

  * Provider method  
    A provider method acquires the address lists from the provider and use.
    An address provider can be configured as a web service or a static content.
    No cluster restart is needed for node addition, so provider method is suitable when it is impossible to estimate size. Note that the address provider is required ensuring availability. 

#### Configuration method

The same setting of cluster configuration method in the cluster definition file (gs_cluster.json) needs to be made in all the nodes constituting the cluster.

  * Fixed list method  
    When a fixed address list is given to start a node, the list is used to compose the cluster. 
    When composing a cluster using the fixed list method, configure the parameters in the cluster definition file.   

    /cluster/notificationMember (String) : 
    Specify the address list when using the fixed list method as the cluster configuration method.

    A configuration example of a cluster definition file is shown below. 

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
            "transaction": {"address":"172.17.0.44", "port":10001}
         },
         {
            "cluster": {"address":"172.17.0.45", "port":10010},
            "sync": {"address":"172.17.0.45", "port":10020},
            "system": {"address":"172.17.0.45", "port":10040},
            "transaction": {"address":"172.17.0.45", "port":10001}
         },
         {
            "cluster": {"address":"172.17.0.46", "port":10010},
            "sync": {"address":"172.17.0.46", "port":10020},
            "system": {"address":"172.17.0.46", "port":10040},
            "transaction": {"address":"172.17.0.46", "port":10001}
          }
        ]
      },
                                 :
                                 :
    }
    ```

  * Provider method  
    Get the address list supplied by the address provider to perform cluster configuration. 
    When composing a cluster using the provider method, configure the parameters in the cluster definition file.  

    /cluster/notificationProvider/url (String) : 
    Specify the URL of the address provider when using the provider method as the cluster configuration method.  
    /cluster/notificationProvider/updateInterval (String) : 
    Specify the interval to get the list from the address provider. Specify a value that is 1s or higher and less than 2^31s.

    A configuration example of a cluster definition file is shown below. 

    ```
    {
                                 :
                                 :
      "cluster":{
        "clusterName":"yourClusterName",
        "replicationNum":2,
        "heartbeatInterval":"5s",
        "loadbalanceCheckInterval":"180s",
        "notificationProvider":{
          "url":"http://example.com/notification/provider",
          "updateInterval":"30s"
        }
      },
                                 :
                                 :
    }
    ```

    The address provider can be configured as a Web service or as a static content. The specifications below need to be satisfied.
    * Compatible with the GET method. 
    * When accessing the URL, the node address list of the cluster containing the cluster definition file in which the URL is written is returned as a response.  
      * Response body: Same JSON as the contents of the node list specified in the fixed list method  
      * Response header: Including Content-Type:application/json 

    An example of a response sent from the address provider is as follows. 

    ```
    $ curl http://example.com/notification/provider
    [
      {
        "cluster": {"address":"172.17.0.44", "port":10010},
        "sync": {"address":"172.17.0.44", "port":10020},
        "system": {"address":"172.17.0.44", "port":10040},
        "transaction": {"address":"172.17.0.44", "port":10001},
      },
      {
        "cluster": {"address":"172.17.0.45", "port":10010},
        "sync": {"address":"172.17.0.45", "port":10020},
        "system": {"address":"172.17.0.45", "port":10040},
        "transaction": {"address":"172.17.0.45", "port":10001}
      },
      {
        "cluster": {"address":"172.17.0.46", "port":10010},
        "sync": {"address":"172.17.0.46", "port":10020},
        "system": {"address":"172.17.0.46", "port":10040},
        "transaction": {"address":"172.17.0.46", "port":10001}
      }
    ]
    ```

Note: 
  * Specify the serviceAddress and servicePort of the node definition file in each module (cluster,sync etc.) for each address and port. 
  * Set either the /cluster/notificationAddress, /cluster/notificationMember, /cluster/notificationProvider in the cluster definition file to match the cluster configuration method used. 

#### Connection method of client

Set GridStoreFactory parameter of each client for fixed list or provider method.

  * Fixed list method  
    notificationMember : 
    A list of addresses and port pairs in cluster. It is used to connect to cluster which is configured with fixed list mode, and specified as follows.   
    `(Address1):(Port1),(Address2):(Port2),...`  
This property cannot be specified with neither notificationAddress nor notificationProvider properties at the same time.

  * Provider method  
    notificationProvider : 
    The URL of an address provider. It is used to connect to cluster which is configured with provider mode. This property cannot be specified with neither notificationAddress nor notificationMember properties at the same time.

## 2. Block data compression

When GridDB writes in-memory data to the database file, a large capacity database independent to the memory size can be realized. However, cost of storage will rise. To reduce this cost, block data compression function with the ability to compress database file (checkpoint file) is used. Compared to HDD, a flash memory with a high price per unit of capacity can be utilized more efficiently.

#### Compression method

When exporting in-memory data to the database file (checkpoint file), compression is performed to each block of GridDB write unit. The vacant area of Linux's file space due to compression can be deallocated, thereby reducing disk usages.

#### Configuration method

The compression function needs to be configured in every nodes.

Set the following string in the node definition file (gs_node.json)/datastore/storeCompressionMode.
  * To disable compression functionality: NO-COMPRESSION (default)
  * To enable compression functionality: COMPRESSION.

The settings will be applied after GridDB node is restarted.
By restarting GridDB node, enable/disable operation of the compression function can be changed.

Please pay attention to the following.
  * Block data compression can only be applied to checkpoint file. Transaction log files, backup file, and GridDB`s in-memory data are not subject to compression.
  * Due to block data compression, checkpoint file will become sparse file.
  * Even if the compression function is changed effectively, data already written to the checkpoint file cannot be compressed. 

#### Verified Environment

Its operation was verified in the following environment.

  * OS: CentOS 7.2
  * File system: XFS
  * File system block size: 4 KB


Copyright @ 2016 TOSHIBA CORPORATION
