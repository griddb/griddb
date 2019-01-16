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

Copyright (C) 2019 TOSHIBA Digital Solutions Corporation
