# GridDB CE 4.2

## Changes

Main changes in GridDB CE v4.2 are as follows:

1. C Client for Windows environment
    - You can use the C Client library on the Windows environment.

2. Designating application names
    - The application name (applicationName) is added to the connection property. It is output to event logs, and so on. It is useful for specifying the application with problems.

3. Controlling buffers for search
    - The amount of used buffers are controlled monitoring the amount of swap read of a job of queries. It reduces the performance degradation of a register process caused by the swap out of queries.

