# S-Query

S-Query is a novel system for querying the state in stateful stream processors. S-Query is implemented on top of the Hazelcast Jet stream processing system.

This monorepo contains several projects, for more details view the README.md inside the relevent project. Below a general overview of the projects.

| Project              | Description                                                                                                                                                                   |                      License                       |
| :------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------: |
| `hazelcast`          | Fork of the [hazelcast/hazelcast](https://github.com/hazelcast/hazelcast) repository. This contains technologies like IMap and distributed objects, that are used in S-Query. | Apache License 2.0 and Hazelcast Community License |
| `hazelcast-jet`      | Fork of the [hazelcast/hazelcast-jet](https://github.com/hazelcast/hazelcast-jet) repository. This contains the modified stream processor for S-Query.                        | Apache License 2.0 and Hazelcast Community License |
| `S-Query-examples`   | Example jobs to run with S-Query, also used for the evaluation                                                                                                                |                 Apache License 2.0                 |
| `S-Query-evaluation` | Raw data + visualization code used for the results                                                                                                                            |                 Apache License 2.0                 |
