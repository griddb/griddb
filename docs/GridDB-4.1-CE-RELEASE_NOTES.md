# GridDB CE 4.1

## Changes

Main changes in GridDB CE v4.1 are as follows:

1. Online expansion
    - Node addition and detachment can be performed without stopping the cluster.

2. Geometry data
    - Geometry data type has been added. Spatial indexes can also be used.

3. Time-series compression
    - Time-series data compression for use within time-series containers has been added.

4. Expanding the upper limit of the number of columns
    - The upper limit of the number of columns that can be handled on containers. It was 1024; however, it becomes 1024-32000 from V4.1. (It depends on the setting of a block size or the column type of a container.)
5. Improving a dynamic schema change (column addition)
    - It becomes possible to access a container when a column is added to the end of it. It is not possible to access it when the schema of it is changed in versions earlier than V4.1. Moreover, the process of adding columns to the end of a container becomes faster.

---

### Online expansion

When a node is separated online due to maintenance and other work during cluster operation, it can be incorporated after the maintenance work ends. Furthermore, nodes can be added online to reinforce the system. 

The operation commands gs_appendcluster/gs_leavecluster command are used. 

### Geometry data

GEOMETRY data are widely used in map information systems, etc. 

For GEOMETRY, data is written in WKT (Well-known text). WKT is formulated by the Open Geospatial Consortium (OGC), a nonprofit organization promoting standardization of information on geospatial information. 

The following WKT format data can be stored in the GEOMETRY column. 
- POINT 
  + Point represented by two or three-dimensional coordinate. 
  + Example) POINT(0 10 10) 
- LINESTRING 
  + Set of straight lines in two or three-dimensional space represented by two or more points. 
  + Example) LINESTRING(0 10 10, 10 10 10, 10 10 0) 
- POLYGON 
  + Closed area in two or three-dimensional space represented by a set of straight lines. 
  + Example) POLYGON((0 0,10 0,10 10,0 10,0 0)), POLYGON((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30)) 
- POLYHEDRALSURFACE 
  + Area in the three-dimensional space represented by a set of the specified area. 
  + Example) POLYHEDRALSURFACE(((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 0 1 0, 0 1 1, 0 0 1, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)), ((1 1 1, 1 0 1, 0 0 1, 0 1 1, 1 1 1)), ((1 1 1, 1 0 1, 1 0 0, 1 1 0, 1 1 1)), ((1 1 1, 1 1 0, 0 1 0, 0 1 1, 1 1 1))) 

Operations using GEOMETRY can be executed with API or TQL. 

With TQL, management of two or three-dimensional spatial structure is possible. Generating and judgement function are also provided. 

    SELECT * WHERE ST_MBRIntersects(geom, ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))

### Time-series compression

In timeseries container, data can be compressed and held. Data compression can improve memory usage efficiency. Compression options can be specified when creating a timeseries container. 

The following compression types are supported: 
- HI: thinning out method with error value 
- SS: thinning out method without error value 
- NO: no compression. 

However, the following row operations cannot be performed on a timeseries container for which compression options are specified. 
- Updating a specified row. 
- Deleting a specified row. 
- Inserting a new row when there is a row at a later time than the specified time. 


---

Copyright (C) 2019 TOSHIBA Digital Solutions Corporation
