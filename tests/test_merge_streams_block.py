from collections import defaultdict
from time import sleep
from unittest.mock import MagicMock

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase

from ..merge_streams_block import MergeStreams


class TestMergeStreams(NIOBlockTestCase):

    def setUp(self):
        super().setUp()
        # This will keep a list of signals notified for each output
        self.last_notified = defaultdict(list)

    def signals_notified(self, signals, output_id='default'):
        self.last_notified[output_id].extend(signals)

    def process_test_signals(self, blk):
        blk.process_signals([Signal({"A": "a"})], input_id='input_1')
        sleep(0.2)
        blk.process_signals([Signal({"B": "b"})], input_id='input_2')
        blk.process_signals([Signal({"C": "c"})], input_id='input_1')
        blk.process_signals([Signal({"D": "d"})], input_id='input_1')
        blk.process_signals([Signal({"E": "e"})], input_id='input_2')

    def test_no_expiration_and_notify_once_is_true(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "expiration": {},
            "notify_once": True
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(2)
        self.assertDictEqual(self.last_notified['default'][0].to_dict(),
                             {"A": "a", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][1].to_dict(),
                             {"D": "d", "E": "e"})

    def test_no_expiration_and_notify_once_is_false(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "expiration": {},
            "notify_once": False
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(4)
        self.assertDictEqual(self.last_notified['default'][0].to_dict(),
                             {"A": "a", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][1].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][2].to_dict(),
                             {"D": "d", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][3].to_dict(),
                             {"D": "d", "E": "e"})

    def test_with_expiration_and_notify_once_is_true(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "expiration": {"seconds": 0.1},
            "notify_once": True
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(2)
        self.assertDictEqual(self.last_notified['default'][0].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][1].to_dict(),
                             {"D": "d", "E": "e"})

    def test_with_expiration_and_notify_once_is_false(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "expiration": {"seconds": 0.1},
            "notify_once": False
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(3)
        self.assertDictEqual(self.last_notified['default'][0].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][1].to_dict(),
                             {"D": "d", "B": "b"})
        self.assertDictEqual(self.last_notified['default'][2].to_dict(),
                             {"D": "d", "E": "e"})

    def test_reset_expiration_job_on_new_signal_input_1(self):
        """ Signal expiration job is not called if new signals come in """
        blk = MergeStreams()
        blk._signal_expiration_job = MagicMock()
        self.configure_block(blk, {
            "expiration": {"seconds": 0.1},
            "notify_once": False
        })
        blk.start()
        blk.process_signals([Signal({"A": "a"})], input_id='input_1')
        sleep(0.05)
        blk.process_signals([Signal({"B": "b"})], input_id='input_1')
        sleep(0.05)
        blk.process_signals([Signal({"C": "c"})], input_id='input_1')
        sleep(0.05)
        blk.stop()
        self.assertEqual(blk._signal_expiration_job.call_count, 0)

    def test_reset_expiration_job_on_new_signal_input_2(self):
        """ Signal expiration job is not called if new signals come in """
        blk = MergeStreams()
        blk._signal_expiration_job = MagicMock()
        self.configure_block(blk, {
            "expiration": {"seconds": 0.1},
            "notify_once": False
        })
        blk.start()
        blk.process_signals([Signal({"A": "a"})], input_id='input_2')
        sleep(0.05)
        blk.process_signals([Signal({"B": "b"})], input_id='input_2')
        sleep(0.05)
        blk.process_signals([Signal({"C": "c"})], input_id='input_2')
        sleep(0.05)
        blk.stop()
        self.assertEqual(blk._signal_expiration_job.call_count, 0)
