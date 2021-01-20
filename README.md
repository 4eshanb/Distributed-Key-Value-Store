# Distributed-Key-Value-Store

Built a REST API with Flask to create a, sharded, replicated, Fault-Tolerant Key-Value Store. Causal Consistency through Vector Clocks, so each message will be delivered in the correct order.
Docker instances were used as virtual nodes that communicate over a docker network.
Implemented Fault-Tolerance with sharding, so if one node fails, it fails independently. In this case, the Key-Value Store redistributes data among the remaining machines in that shard. Shards had at least two nodes.
Distributed Keys with Consistent hashing; a hash function is used to split key-values received.
Supports view operations, sharded operations, and key-value operations.


## Hash Function
![Alt text](Diagrams/HashFunction.png?raw=true "Hash Function")