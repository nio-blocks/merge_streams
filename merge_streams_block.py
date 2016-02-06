from nio.common.block.attribute import Input
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import VersionProperty, TimeDeltaProperty, \
    BoolProperty


@Input('input_2')
@Input('input_1')
@Discoverable(DiscoverableType.block)
class MergeStreams(Block):

    """ Take two input streams and combine signals together. """

    expiration = TimeDeltaProperty(default={})
    notify_once = BoolProperty(default=True)
    version = VersionProperty('0.1.0')

    def process_signals(self, signals, input_id='default'):
        for signal in signals:
            pass
        self.notify_signals(signals, output_id='default')
