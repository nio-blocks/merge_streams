from collections import defaultdict
from time import sleep
from unittest.mock import MagicMock

from nio.block.terminals import DEFAULT_TERMINAL
from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase

from ..merge_streams_block import MergeStreams


class TestMergeStreams(NIOBlockTestCase):

    def process_test_signals(self, blk):
        blk.process_signals([Signal({"A": "a"})], input_id='input_1')
        sleep(0.2)
        blk.process_signals([Signal({"B": "b"})], input_id='input_2')
        blk.process_signals([Signal({"C": "c"})], input_id='input_1')
        blk.process_signals([Signal({"D": "d"})], input_id='input_1')
        blk.process_signals([Signal({"E": "e"})], input_id='input_2')

    def test_default_input(self):
        blk = MergeStreams()
        blk.start()
        self.configure_block(blk, {})
        signal = Signal({"A": "a"})
        blk.process_signals([signal], input_id='input_1')
        blk.stop()
        self.assertEqual(blk._signals[None]["input_1"], signal)

    def test_group_by(self):
        blk = MergeStreams()
        blk.start()
        self.configure_block(blk, {"group_by": "{{ $group }}"})
        signal = Signal({"A": "a", "group": 1})
        blk.process_signals([signal], input_id='input_1')
        signal = Signal({"B": "b", "group": 2})
        blk.process_signals([signal], input_id='input_1')
        signal = Signal({"C": "c", "group": 1})
        blk.process_signals([signal], input_id='input_2')
        blk.stop()
        self.assert_num_signals_notified(1)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"A": "a", "C": "c", "group": 1})

    def test_no_ttl_and_notify_once_is_true(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "ttil": {},
            "notify_once": True
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(2)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"A": "a", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"D": "d", "E": "e"})

    def test_no_ttl_and_notify_once_is_false(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "ttl": {},
            "notify_once": False
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(4)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"A": "a", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
                             {"D": "d", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][3].to_dict(),
                             {"D": "d", "E": "e"})

    def test_with_ttl_and_notify_once_is_true(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "ttl": {"seconds": 0.1},
            "notify_once": True
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(2)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"D": "d", "E": "e"})

    def test_with_ttl_and_notify_once_is_false(self):
        blk = MergeStreams()
        self.configure_block(blk, {
            "ttl": {"seconds": 0.1},
            "notify_once": False
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(3)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"C": "c", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"D": "d", "B": "b"})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
                             {"D": "d", "E": "e"})

    def test_reset_expiration_job_on_new_signal_input_1(self):
        """ Signal expiration job is not called if new signals come in """
        blk = MergeStreams()
        blk._signal_expiration_job = MagicMock()
        self.configure_block(blk, {
            "ttl": {"seconds": 0.1},
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
            "ttl": {"seconds": 0.1},
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
