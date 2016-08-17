MergeStreams
============

Take two input streams and combine signals together

Properties
----------
- **expiration** (timedelta) - length of time to store signal before dropping it
- **notify_once** (bool) - remove signal from block after it is notified
- **group_by** (str) - signals from the two inputs are merged by group

Dependencies
------------
None

Commands
--------
None

Input
-----
Any list of signals to either input.

Output
------
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
