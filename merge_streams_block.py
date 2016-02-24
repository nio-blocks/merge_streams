from collections import defaultdict
from nio.common.block.attribute import Input
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.signal.base import Signal
from nio.metadata.properties import VersionProperty, TimeDeltaProperty, \
    BoolProperty
from nio.modules.scheduler import Job

from .mixins.group_by.group_by_block import GroupBy


@Input('input_2')
@Input('input_1')
@Discoverable(DiscoverableType.block)
class MergeStreams(GroupBy, Block):

    """ Take two input streams and combine signals together. """

    expiration = TimeDeltaProperty(default={})
    notify_once = BoolProperty(default=True)
    version = VersionProperty('0.1.0')

    def _default_signals_dict(self):
        return { "input_1": {}, "input_2": {} }

    def _default_expiration_jobs_dict(self):
        return { "input_1": None, "input_2": None }

    def __init__(self):
        super().__init__()
        self._signals = defaultdict(self._default_signals_dict)
        self._expiration_jobs = defaultdict(self._default_expiration_jobs_dict)

    def process_signals(self, signals, input_id='input_1'):
        self.for_each_group(self._process_group, signals,
                            kwargs={'input_id': input_id})

    def _process_group(self, signals, group, input_id):
        merged_signals = []
        for signal in signals:
            self._signals[group][input_id] = signal
            if self._signals[group]["input_1"] and \
                    self._signals[group]["input_2"]:
                merged_signals.append(self._merge_signals(group))
        if self.expiration:
            self._schedule_signal_expiration_job(group, input_id)
        if merged_signals:
            self.notify_signals(merged_signals)

    def _merge_signals(self, group):
        """ Merge signals 1 and 2 and clear from memory if only notify once """
        sig_1_dict = self._signals[group]["input_1"].to_dict(hidden=True)
        sig_2_dict = self._signals[group]["input_2"].to_dict(hidden=True)
        self._fix_to_dict_hidden_attr_bug(sig_1_dict)
        self._fix_to_dict_hidden_attr_bug(sig_2_dict)
        merged_signal_dict = {}
        merged_signal_dict.update(sig_1_dict)
        merged_signal_dict.update(sig_2_dict)
        if self.notify_once:
            self._signals[group]["input_1"] = {}
            self._signals[group]["input_2"] = {}
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

    def _schedule_signal_expiration_job(self, group, input_id):
        """ Schedule expiration job, cancelling existing job first """
        if self._expiration_jobs[group][input_id]:
            self._expiration_jobs[group][input_id].cancel()
        self._expiration_jobs[group][input_id] = Job(
            self._signal_expiration_job, self.expiration, False,
            group, input_id)

    def _signal_expiration_job(self, group, input_id):
        self._signals[group][input_id] = {}
        self._expiration_jobs[group][input_id] = None
