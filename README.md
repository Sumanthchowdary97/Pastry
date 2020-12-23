### Project 3 Report

**Members:**

Sai Chandra Sekhar Devarakonda (UFID: 9092-2981)

Sumanth Chowdary Lavu (UFID: 5529-6647)

**Objective**: The objective of this project is to implement the Pastry protocol and its performance comparison for different number of nodes and requests.

**Introduction**: Pastry is an overlay network and routing network for the implementation of a distributed hash table (DHT). There will not be a single point of failure as the key-value pairs are stored redundantly. If there is a failure it will be immediately fixed. Hence, there will not be a data loss.

**Execution:**

dotnet fsi --langversion:preview project3.fsx numNodes numRequests

For example: dotnet fsi --langversion:preview project3.fsx 100 10

The complete functionality of join and routing aspects of pastry algorithm is working.
The largest network we managed to deal with is of size 10000 nodes.
 
**Results:**
|Number of Nodes|Number of Requests|Average hops per route|
|--|--|--|
|100|10|1.57|
|500|20|2.38|
|1000|60|2.74|
|2500|100|3.25|
|5000|100|3.62|
|10000|200|4.01|


