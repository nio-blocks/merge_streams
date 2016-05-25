from collections import defaultdict
from nio.block.base import Block
from nio.util.discovery import discoverable
from nio.signal.base import Signal
from nio.properties import VersionProperty, TimeDeltaProperty, BoolProperty, \
    Property
from nio.modules.scheduler import Job

from nio.block.mixins.group_by.group_by import GroupBy


@discoverable
class MergeDynamicStreams(GroupBy, Block):

    """ Merge a dynamic number of streams and combine signals together. """

    ttl = TimeDeltaProperty(default={}, title="Time to Live")
    notify_once = BoolProperty(default=True, title="Notify Once")
    stream = Property(title="Stream Name", default=None, allow_none=True)
    version = VersionProperty('0.1.0')

    def __init__(self):
        super().__init__()
        # Keep track of the last signal for each stream in each group.
        # Signals are removed from here when ttl expires.
        # Stream names are never removed.
        self._last_signal = defaultdict(lambda: defaultdict(lambda: None))
        # Keep track of the last signal for each stream in each group.
        # Signals are removed from here when ttl expires and when notified and
        # notify_once is True. Stream names are never removed.
        self._signals = defaultdict(lambda: defaultdict(lambda: None))
        # Store jobs that remove signals after ttl.
        self._expiration_jobs = defaultdict(lambda: defaultdict(lambda: None))

    def process_group_signals(self, signals, group, input_id):
        """Return the merged signals to be notified for this group"""
        output_signals = []
        for signal in signals:
            stream = self.stream(signal)
            output_signals.extend(
                self.process_stream_signals([signal], group, stream))
        return output_signals

    def process_stream_signals(self, signals, group, stream):
        """Return the merged signals to be notified for this stream"""
        merged_signals = []
        for signal in signals:
            new_stream = False
            if stream not in self._last_signal[group]:
                # Trigger merge streams if this is a new one
                new_stream = True
            # Save new signal
            self._signals[group][stream] = signal
            self._last_signal[group][stream] = signal
            # Merge all streams for this group
            merged_signals.extend(self._merge_streams(group, new_stream))
        if self.ttl():
            self._schedule_signal_expiration_job(group, stream)
        return merged_signals

    def _merge_streams(self, group, new_stream):
        """Merge signals from each stream and clear if notify once."""
        if new_stream and self.notify_once():
            # Use last signal signal even if it was already notified
            streams = self._last_signal[group]
        else:
            # Otherwise use only the signals still available
            streams = self._signals[group]
        if not [streams[s] for s in streams if streams[s] is None]:
            # Only merge streams if we have a signal for every stream
            merged_signal_dict = {}
            for stream in streams:
                signal_dict = streams[stream].to_dict()
                self._fix_to_dict_hidden_attr_bug(signal_dict)
                merged_signal_dict.update(signal_dict)
                # Clear signal from memory if only notifying once
                if self.notify_once():
                    self._signals[group][stream] = None
            return [Signal(merged_signal_dict)]
        else:
            return []

    def _fix_to_dict_hidden_attr_bug(self, signal_dict):
        """Remove special attributes from dictionary.

        n.io has a bug when using Signal.to_dict(hidden=True). It should
        include private attributes (i.e. attributes starting withe '_') but not
        special attributes (i.e. attributes starting with '__').

        """
        for key in list(signal_dict.keys()):
            if key.startswith('__'):
                del signal_dict[key]

    def _schedule_signal_expiration_job(self, group, stream):
        """ Schedule expiration job, cancelling existing job first """
        if self._expiration_jobs[group][stream]:
            self._expiration_jobs[group][stream].cancel()
        self._expiration_jobs[group][stream] = Job(
            self._signal_expiration_job, self.ttl(), False,
            group, stream)

    def _signal_expiration_job(self, group, stream):
        self._last_signal[group][stream] = None
        self._signals[group][stream] = None
        self._expiration_jobs[group][stream] = None
