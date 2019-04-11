# Distributed Systems
## Project Descript
An available fault tolerant key-value store distributed system that provides the following attributes:

* **Eventual Consistency**. 
* **Causal Consistency**.
* **Resilience**. Use replication in order to make it fault tolerant to node crashes and network partitions 
* **Speedup & Scaleout**. Divide items into shards or replica groups with modulo hashing, allowing data storage to be independent and parallel. Adding shards increases performance by increasing capacity and adding replicas to shards increases fault tolerance. 

## Prerequisites
```
* download Docker
* build Docker image
* create Docker network
```
## Execution Commands

## Contributions
### Michelle Jin
* Vector Clocks
* Gossip
* Modulo Hashing
* Key-Value Store
    * get 
    * put
    * delete
* Shard Endpoints
    * /shard/my_id
    * /shard/all_ids
    * /shard/count
    * /shard/members
    * /shard/changeShardNumber

### Karen Cariaga
* Key-Value Store
    * view

### Kevin Serrano
* docker-machine set up

