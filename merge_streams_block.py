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
        self._signal_1 = None
        self._signal_2 = None

    def process_signals(self, signals, input_id='default'):
        for signal in signals:
            if input_id == "input_1":
                #TODO: deepcopy signals?
                self._signal_1 = signal
                if self._signal_2:
                    self.notify_signals(
                        [self._merge_signals(signal, self._signal_2)])
                    if self.notify_once:
                        self._signal_1 = None
                        self._signal_2 = None
            else:
                self._signal_2 = signal
                if self._signal_1:
                    self.notify_signals(
                        [self._merge_signals(signal, self._signal_1)])
                    if self.notify_once:
                        self._signal_1 = None
                        self._signal_2 = None
        if self.expiration:
            self._schedule_signal_expiration_job(input_id)

    def _merge_signals(self, signal_1, signal_2):
        merged_signal = signal_1.to_dict()
        merged_signal.update(signal_2.to_dict())
        return Signal(merged_signal)

    def _schedule_signal_expiration_job(self, input_id):
        Job(self._signal_expiration_job,
            self.expiration,
            False,
            input_id)

    def _signal_expiration_job(self, input_id):
        if input_id == "input_1":
            self._signal_1 = None
        else:
            self._signal_2 = None
