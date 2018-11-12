MergeStreams
============
The MergeStreams block takes signals from *input_1* and *input_2* and emits them as one signal.  If the signals have matching attributes (that aren't the group_by property) then the input_2 signal's attribute will take priority.

Properties
----------
- **backup_interval**: An interval of time that specifies how often persisted data is saved.
- **expiration**: Length of time to store an incoming signal and wait for an incoming signal to the other input before dropping the signal.
- **group_by**: Signals from the two inputs are merged based on matching group attributes.
- **load_from_persistence**: If `True`, the block’s state will be saved when the block is stopped, and reloaded once the block is restarted.
- **notify_once**: If true (checked), remove a signal from block after it is merged and emitted.

Inputs
------
- **input_1**: Any list of signals.
- **input_2**: Any list of signals.

Outputs
-------
- **default**: A new signal that is the merged version of one signal from input_1 and one signal from input_2.

Commands
--------
- **groups**: Display all the current groupings of the signals.

Dependencies
------------
None

Output Examples
---------------
A new signal that is the merged version of one signal from input 1 and one signal from input 2.
- example (with no expiration and notify once is True)
  - signal A enters input 1
  - signal B enters input 2 - notify AB
  - signal C enters input 1
  - signal D enters input 1
  - signal E enters input 2 - notify DE
- example (with no expiration and notify once is False)
  - signal A enters input 1
  - signal B enters input 2 - notify AB
  - signal C enters input 1 - notify CB
  - signal D enters input 1 - notify DB
  - signal E enters input 2 - notify DE
- example (with expiration and notify once is True)
  - signal A enters input 1
  - signal A expires
  - signal B enters input 2
  - signal C enters input 1 - notify CB
  - signal D enters input 1
  - signal E enters input 2 - notify DE
- example (with expiration and notify once is False)
  - signal A enters input 1
  - signal A expires
  - signal B enters input 2
  - signal C enters input 1 - notify CB
  - signal D enters input 1 - notify DB
  - signal E enters input 2 - notify DE
If the signals from input_1 and input_2 share an attribute, the merged signal takes the value from input_2.

Persistence
-----------
Persist signals only when no expiration (ttl) is configured.
Signals at each input will be persisted between block restarts except when an expiration is configured. TODO: Improve this feature so signals are always persisted and then properly removed after loaded and the expiration has passed.
