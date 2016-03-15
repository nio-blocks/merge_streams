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

    expiration = TimeDeltaProperty(default={})
    version = VersionProperty('0.1.0')
    stream = Property(name="Stream", default=None, allow_none=True)

    def __init__(self):
        super().__init__()
        self._signals = defaultdict(lambda: defaultdict(Signal))
        self._expiration_jobs = defaultdict(lambda: defaultdict(None))

    def process_group_signals(self, signals, group, input_id):
        output_signals = []
        streams = defaultdict(list)
        for signal in signals:
            streams[self.stream(signal)].append(signal)
        for stream in streams:
            output_signals.extend(
                self.process_stream_signals(streams[stream], group, stream))
        return output_signals

    def process_stream_signals(self, signals, group, stream):
        merged_signals = []
        for signal in signals:
            # Save new signal
            self._signals[group][stream] = signal
            # Merge all signals for this group
            merged_signals.append(self._merge_signals(group))
        if self.expiration():
            self._schedule_signal_expiration_job(group, stream)
        return merged_signals

    def _merge_signals(self, group):
        """ Merge signals 1 and 2 and clear from memory if only notify once """
        merged_signal_dict = {}
        for stream in self._signals[group]:
            signal_dict = self._signals[group][stream].to_dict()
            self._fix_to_dict_hidden_attr_bug(signal_dict)
            merged_signal_dict.update(signal_dict)
        return Signal(merged_signal_dict)

    def _fix_to_dict_hidden_attr_bug(self, signal_dict):
        """ Remove special attributes from dictionary

        n.io has a bug when using Signal.to_dict(hidden=True). It should
        include private attributes (i.e. attributes starting withe '_') but not
        special attributes (i.e. attributes starting with '__').

        """
        for key in list(signal_dict.keys()):
            if key.startswith('__'):
                del signal_dict[key]

    def _schedule_signal_expiration_job(self, group, stream):
        """ Schedule expiration job, cancelling existing job first """
        if self._expiration_jobs[group].get(stream):
            self._expiration_jobs[group][stream].cancel()
        self._expiration_jobs[group][stream] = Job(
            self._signal_expiration_job, self.expiration(), False,
            group, stream)

    def _signal_expiration_job(self, group, stream):
        del self._signals[group][stream]
        del self._expiration_jobs[group][stream]
