# Distributed-Key-Value-Store

Built a REST API with Flask to create a, sharded, replicated, Fault-Tolerant Key-Value Store, that enforces Causal consistency through Vector Clocks.
Implemented Fault-Tolerance with consistent hashing and sharding, so if one node fails, it fails independently. In this case, the Key-Value Store redistribute data among the remaining machines.
Supports view operations, sharded operations, and key-value operations.
