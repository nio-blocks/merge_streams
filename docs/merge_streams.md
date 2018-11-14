MergeStreams
============
The MergeStreams block takes signals from *input_1* and *input_2* and emits them as one signal. If the signals have matching attributes (that aren't the group_by property) then the input_2 signal's attribute will take priority.

Properties
---
- **Group By**: Signals from the two inputs are merged based on matching group attributes.

Advanced Properties
---
- **Stream Expiration**: Length of time to store an incoming signal and wait for an incoming signal to the other input before dropping the signal.
- **Notify Once**: If true (checked), remove a signal from block after it is merged and emitted.
- **Load From Persistence**: If `True`, the block’s state will be saved when the block is stopped, and reloaded once the block is restarted.

Inputs
---
- **INPUT_1**: Any list of signals.
- **INPUT_2**: Any list of signals.

Outputs
---
- A new signal that is the merged version of one signal from INPUT_1 and one signal from INPUT_2.

Commands
---
- **groups**: Display all the current groupings of the signals.

Examples
---
With the default configuration, the block needs a signal to be processed by each input, and then it will emit a single signal that contains the attributes of both input signals. Because **notify Once** is checked (True), until a new signal is received by both inputs no signals will be emitted. The last signal received is stored internally until the **Stream Expiration**, if applicable.
 ```
{"letters": "A"}         {"numbers": 0}
  none                    {"numbers": 1}
{"letters": "B"}           none
{"letters": "C"}           none
  ...                      ...
            |             |
            |             |
            V             V
       +----1-------------2----+
       | MergeStreams          |
       |   Notify Once: True   |
       |                       |
       +-----------O-----------+
                   |
                   |
                   V
      {"letters": "A", "numbers": 0}
       none
      {"letters": "B", "numbers": 1}
       none
       ...
```
In the case that both input signals have a common attribute, the value from _input_2_ will overwrite the value from _input_1_. By Setting **Notify Once** False (unchecked), every signal received at either input will have the last signal received by the *other* input merged into it. See also: [_AppendState_](https://blocks.n.io/AppendState)
 ```
{"letters": "A"}         {"numbers": 0}
  none                    {"numbers": 1}
{"letters": "B"}           none
{"letters": "C"}           none
  ...                      ...
            |             |
            |             |
            V             V
       +----1-------------2----+
       | MergeStreams          |
       |   Notify Once: False  |
       |                       |
       +-----------O-----------+
                   |
                   |
                   V
     {"letters": "A", "numbers": 0}
     {"letters": "A", "numbers": 1}
     {"letters": "B", "numbers": 1}
     {"letters": "C", "numbers": 1}
       ...
```
In this example, a **Goup By** expression is used to distinguish two different sources of data by using the incoming signals' `id` attribute. The **Stream Expiration** means that, even though **Notify Once** is False, if one of the streams "goes dry" (stops receiving new signals) its last signal is discarded. This is useful for ensuring that each source of data is working and up to date before using that data. To take action in the case of a dry stream, see [Monitoring Streams.](https://docs.n.io/service-design-patterns/monitor_stream.html)
```
{"id": "A", "foo": 1}     {"id": "A", "bar": -1}
{"id": "B", "foo": 1}     {"id": "B", "bar": -1}
  (1 sec delay)             (1 sec delay)
{"id": "A", "foo": 2}     {"id": "B", "bar": -2}
{"id": "B", "foo": 2}
  (1 sec delay)             (1 sec delay)
{"id": "B", "foo": 3}     {"id": "B", "bar": -3}
  (1 sec delay)             (1 sec delay)
  ...                            ...
            |                   |
            |                   |
            V                   V
       +----1-------------------2----+
       | MergeStreams                |
       |   Notify Once: False        |
       |   Stream Expiration: 2 sec  |
       |   Group By: {{ $id }}       |
       +--------------O--------------+
                      |
                      |
                      V
       {"id": "A", "foo": 1, "bar": -1}
       {"id": "B", "foo": 1, "bar": -1}
         (1 sec delay)
       {"id": "A", "foo": 2, "bar": -1}
       {"id": "B", "foo": 2, "bar": -2}
         (1 sec delay)
       {"id": "B", "foo": 3, "bar": -3}
```

Persistence
---
Persist signals only when no expiration (ttl) is configured. Signals at each input will be persisted between block restarts except when an expiration is configured. For more information on the nio Persistence mixin, see [Persistence.](https://docs.n.io/data/persistence.html)
