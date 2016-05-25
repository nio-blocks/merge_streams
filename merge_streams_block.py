from collections import defaultdict
from nio.block.terminals import input
from nio.util.discovery import discoverable
from nio.properties import Property
from .merge_dynamic_streams_block import MergeDynamicStreams


@input('input_2')
@input('input_1', default=True)
@discoverable
class MergeStreams(MergeDynamicStreams):

    """ Take two input streams and combine signals together. """

    # Remove unused property from base class
    stream = Property(
        title="Stream Name", default=None, allow_none=True, visible=False)

    def _default_signals_dict(self):
        return { "input_1": None, "input_2": None }

    def __init__(self):
        super().__init__()
        # Override these dictionaries from the base block so that they only
        # have streams "input_1" and "input_2".
        self._last_signal = defaultdict(self._default_signals_dict)
        self._signals = defaultdict(self._default_signals_dict)

    def process_group_signals(self, signals, group, input_id):
        """Return the merged signals to be notified for this group"""
        merged_signals = []
        for signal in signals:
            merged_signals.extend(
                self.process_stream_signals(signals, group, input_id))
        return merged_signals
