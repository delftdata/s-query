# S-Query-examples
This repo hosts some examples that query the internal state of stateful operators in S-Query. (Based on Hazelcast Jet)

S-Query is an adaptation of Hazelcast Jet which supports querying the internal state.

## Structure
The most important are the `dh`, `benchmark-getter-job` and `generic-query`/`generic-inc-query` modules.

The `dh` module contains the Jet jobs for the Delivery Hero order tracking application in the S-Query paper.

Submodules:
- `dh-job` Job which contains the stateful operations happening on the stream and measures the time it takes to go from source to sink.
- `dh-direct-query` Job that queries the state of a given stateful operator using the direct object interface.
- `dh-queries` Job which queries the state with one of 4 complex SQL queries
- `dh-query-benchmark` Job which queries one of 4 complex SQL queries continuously and measures the latencies.

The `benchmark-getter-job` is a Jet job which gets the 2PC times of S-Query/Jet. Internally (in S-Query) the time it takes to complete a 2 phase commit is measured and put to a List.
This job gets that list and prints the latencies to the console (in ns). Both phase 1 and phase 2 are individually measured.

`generic-query` Is a helper job which can query any table with any type of query with the latest snapshot ID.

`generic-inc-query` Is similar to `generic-query` except it will query incremental snapshots, again with the latest snapshot id.

The `scripts` folder contains several scripts for transferring the jars to a remote machine/multiple remote machines.

