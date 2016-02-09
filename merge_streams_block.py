from nio.common.block.attribute import Input
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.signal.base import Signal
from nio.metadata.properties import VersionProperty, TimeDeltaProperty, \
    BoolProperty
from nio.modules.scheduler import Job


@Input('input_2')
@Input('input_1')
@Discoverable(DiscoverableType.block)
class MergeStreams(Block):

    """ Take two input streams and combine signals together. """

    expiration = TimeDeltaProperty(default={})
    notify_once = BoolProperty(default=True)
    version = VersionProperty('0.1.0')

    def __init__(self):
        super().__init__()
        self._signals = { "input_1": {}, "input_2": {} }
        self._expiration_jobs = { "input_1": None, "input_2": None }

    def process_signals(self, signals, input_id='default'):
        merged_signals = []
        for signal in signals:
            self._signals[input_id] = signal
            if self._signals["input_1"] and self._signals["input_2"]:
                merged_signals.append(self._merge_signals())
        if self.expiration:
            self._schedule_signal_expiration_job(input_id)
        if merged_signals:
            self.notify_signals(merged_signals)

    def _merge_signals(self):
        """ Merge signals 1 and 2 and clear from memory if only notify once """
        merged_signal = self._signals["input_1"].to_dict()
        merged_signal.update(self._signals["input_2"].to_dict())
        if self.notify_once:
            self._signals["input_1"] = {}
            self._signals["input_2"] = {}
        return Signal(merged_signal)

    def _schedule_signal_expiration_job(self, input_id):
        """ Schedule expiration job, cancelling existing job first """
        if self._expiration_jobs[input_id]:
            self._expiration_jobs[input_id].cancel()
        self._expiration_jobs[input_id] = Job(
            self._signal_expiration_job, self.expiration, False, input_id)

    def _signal_expiration_job(self, input_id):
        self._signals[input_id] = {}
