from collections import defaultdict
from nio.block.terminals import input
from nio.block.base import Block
from nio.util.discovery import discoverable
from nio.signal.base import Signal
from nio.properties import VersionProperty, TimeDeltaProperty, \
    BoolProperty
from nio.modules.scheduler import Job
from nio.block.mixins.group_by.group_by import GroupBy
from nio.block.mixins.persistence.persistence import Persistence


@input('input_2')
@input('input_1', default=True)
@discoverable
class MergeStreams(Persistence, GroupBy, Block):

    """ Take two input streams and combine signals together. """

    expiration = TimeDeltaProperty(default={}, title="Stream Expiration")
    notify_once = BoolProperty(default=True, title="Notify Once?")
    version = VersionProperty('0.1.0')

    def _default_signals_dict(self):
        return { "input_1": {}, "input_2": {} }

    def _default_expiration_jobs_dict(self):
        return { "input_1": None, "input_2": None }

    def __init__(self):
        super().__init__()
        self._signals = defaultdict(self._default_signals_dict)
        self._expiration_jobs = defaultdict(self._default_expiration_jobs_dict)

    def persisted_values(self):
        """Persist signals only when no expiration (ttl) is configured.

        Signals at each input will be persisted between block restarts except
        when an expiration is configured. TODO: Improve this feature so signals
        are always persisted and then properly removed after loaded and the
        expiration has passed.
        """
        if self.expiration():
            return []
        else:
            return ["_signals"]

    def process_group_signals(self, signals, group, input_id):
        merged_signals = []
        for signal in signals:
            self._signals[group][input_id] = signal
            if self._signals[group]["input_1"] and \
                    self._signals[group]["input_2"]:
                merged_signals.append(self._merge_signals(group))
        if self.expiration():
            self._schedule_signal_expiration_job(group, input_id)
        if merged_signals:
            self.notify_signals(merged_signals)

    def _helper_merge_signals(self, sig_1_dict, sig_2_dict):
        ''' Recurively merge the signals passed into _merge_signals() '''
        dict1_key = list(sig_1_dict.keys())[0]
        dict2_key = list(sig_2_dict.keys())[0]

        merged_signal_dict = {}

        # Base Case: Both signals have different keys and/or at least one of the values is not a dictionary
        if dict1_key != dict2_key or type(sig_1_dict[dict1_key]) != dict or type(sig_2_dict[dict2_key]) != dict:
            merged_signal_dict.update(sig_1_dict)
            merged_signal_dict.update(sig_2_dict)

        # Recursive Case: Both signals have the same key and
        # both entries are dictionaries
        else:
            merged_signal_dict[dict1_key] = self._helper_merge_signals(sig_1_dict[dict1_key], sig_2_dict[dict2_key])

        return merged_signal_dict

    def _merge_signals(self, group):
        """ Merge signals 1 and 2 and clear from memory if only notify once """
        sig_1_dict = self._signals[group]["input_1"].to_dict()
        sig_2_dict = self._signals[group]["input_2"].to_dict()
        self._fix_to_dict_hidden_attr_bug(sig_1_dict)
        self._fix_to_dict_hidden_attr_bug(sig_2_dict)

        merged_signal_dict = self._helper_merge_signals(sig_1_dict, sig_2_dict)
        
        if self.notify_once():
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
            self._signal_expiration_job, self.expiration(), False,
            group, input_id)

    def _signal_expiration_job(self, group, input_id):
        self._signals[group][input_id] = {}
        self._expiration_jobs[group][input_id] = None
