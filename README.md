MergeStreams
============

Take two input streams and combine signals together

Properties
----------
- time to live (timedelta) - time to live (ttl) is the length of time to store signal before dropping it
- notify once (bool) - remove signal from block after it is notified
- group by (str) - signals from the two inputs are merged by group

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

- example (with no ttl and notify once is True)
  - signal A enters input 1
  - signal B enters input 2 - notify AB
  - signal C enters input 1
  - signal D enters input 1
  - signal E enters input 2 - notify DE
- example (with no ttl and notify once is False)
  - signal A enters input 1
  - signal B enters input 2 - notify AB
  - signal C enters input 1 - notify CB
  - signal D enters input 1 - notify DB
  - signal E enters input 2 - notify DE
- example (with ttl and notify once is True)
  - signal A enters input 1
  - signal A expires
  - signal B enters input 2
  - signal C enters input 1 - notify CB
  - signal D enters input 1
  - signal E enters input 2 - notify DE
- example (with ttl and notify once is False)
  - signal A enters input 1
  - signal A expires
  - signal B enters input 2
  - signal C enters input 1 - notify CB
  - signal D enters input 1 - notify DB
  - signal E enters input 2 - notify DE

If the signals from input\_1 and input\_2 share an attribute, the merged signal takes the value from input\_2.

MergeDynamicStream
==================

Take one input and merge signals together based on *stream*.

A dynamic numbers of streams can be created as signals flow into the block. If *notify once* is True, it is actually still possible for a signal to be notified more than once, unlike the regular MergeStreams block. Any time a new stream is established, a merged signal is created and notified from the last known signal from each stream, even if that signal has already been merged and notified. TTL still applies.

Properties
----------
- time to live (timedelta) - time to live (ttl) is the length of time to store signal before dropping it
- notify once (bool) - remove signal from block after it is notified. unlike the regular MergeStreams block, a signal will be notified more than once as it will be merged for each new stream that is created.
- stream (str) - the expression used to determine what stream a signal is from
- group by (str) - signals from the two inputs are merged by group

Dependencies
------------
None

Commands
--------
None

Input
-----
Any list of signals.

Output
------
A new signal that is the merged version of all the *streams*.

- example (with no ttl and notify once is True)
  - signal A with stream 1 - notify A
  - signal B with stream 2 - notify AB
  - signal C with stream 1
  - signal D with stream 1
  - signal E with stream 2 - notify DE
  - signal F with stream 3 - notify DEF
- example (with no ttl and notify once is False)
  - signal A with stream 1 - notify A
  - signal B with stream 2 - notify AB
  - signal C with stream 1 - notify CB
  - signal D with stream 1 - notify DB
  - signal E with stream 2 - notify DE
  - signal F with stream 3 - notify DEF
- example (with ttl and notify once is True)
  - signal A with stream 1 - notify A
  - signal A expires
  - signal B with stream 2
  - signal C with stream 1 - notify CB
  - signal D with stream 1
  - signal E with stream 2 - notify DE
  - signal F with stream 3 - notify DEF
- example (with ttl and notify once is False)
  - signal A with stream 1 - notify A
  - signal A expires
  - signal B with stream 2
  - signal C with stream 1 - notify CB
  - signal D with stream 1 - notify DB
  - signal E with stream 2 - notify DE
  - signal F with stream 3 - notify DEF

If the signals from from multiple streams share an attribute, the merged signal takes one of them at random.
