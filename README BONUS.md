### Project 3 Bonus Report

**Members:**

Sai Chandra Sekhar Devarakonda (UFID: 9092-2981)

Sumanth Chowdary Lavu (UFID: 5529-6647)

**Objective**: The objective of this project is to implement the Pastry protocol and its performance comparison for different number of nodes and requests as well as failure handling.

**Introduction**: Pastry is an overlay network and routing network for the implementation of a distributed hash table (DHT). There will not be a single point of failure as the key-value pairs are stored redundantly. If there is a failure it will be immediately fixed. Hence, there will not be a data loss.

**Failure Nodes:**

If there is a failure, the network should be brought back to normal to ensure no data loss. The active nodes in the network check respective routing tables in search of a node which shares a longer prefix with destination address than the node itself. Then, the failure node’s ID will be replaced by those of active nodes.

**Execution:**

	dotnet fsi --langversion:preview project3.fsx numNodes numRequests numFailures

For example: dotnet fsi --langversion:preview project3.fsx 100 10 10

The complete functionality of join and routing aspects of pastry algorithm is working along with failure nodes.
The largest network we managed to deal with is of size 10000 nodes.

**Results:**
|Number of Nodes|Number of Requests|No.of Failure Nodes|Average hops per route|
|--|--|--|--|
|100|10|10|7.05|
|500|20|30|35.32|
|1000|60|50|75.64|
|5000|100|100|20.28|
|10000|200|150|29.45|


