from collections import defaultdict
from time import sleep
from unittest.mock import MagicMock

from nio.block.terminals import DEFAULT_TERMINAL
from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase

from ..merge_dynamic_streams_block import MergeDynamicStreams


class TestMergeDynamicStreams(NIOBlockTestCase):

    def process_test_signals(self, blk):
        blk.process_signals([Signal({"A": "a", "stream": 1})])
        sleep(0.2)
        blk.process_signals([Signal({"B": "b", "stream": 2})])
        blk.process_signals([Signal({"C": "c", "stream": 1})])
        blk.process_signals([Signal({"D": "d", "stream": 1})])
        blk.process_signals([Signal({"E": "e", "stream": 2})])

    def assertDictContainsSubset(self, dict, subset):
        self.assertTrue(set(subset.items()).issubset(set(dict.items())))

    def test_default_input(self):
        blk = MergeDynamicStreams()
        blk.start()
        self.configure_block(blk, {})
        signal = Signal({"A": "a"})
        blk.process_signals([signal])
        blk.stop()
        self.assertEqual(blk._signals[None][None], signal)

    def test_group_by(self):
        blk = MergeDynamicStreams()
        blk.start()
        self.configure_block(blk, {
            "stream": "{{ $stream }}",
            "group_by": "{{ $group }}"
        })
        signal = Signal({"A": "a", "group": 1, "stream": 1})
        blk.process_signals([signal])
        signal = Signal({"B": "b", "group": 2, "stream": 1})
        blk.process_signals([signal])
        signal = Signal({"C": "c", "group": 1, "stream": 2})
        blk.process_signals([signal])
        blk.stop()
        self.assert_num_signals_notified(3)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"A": "a", "group": 1, "stream": 1})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"B": "b", "group": 2, "stream": 1})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
            {"A": "a", "C": "c", "group": 1})

    def test_merge_signals_with_duplicate_attributes(self):
        """ input_2 attributes override input_1 attributes """
        blk = MergeDynamicStreams()
        signal_1 = Signal({"A": 1, "stream": 1})
        signal_2 = Signal({"A": 2, "stream": 2})
        blk._signals[None][1] = signal_1
        blk._signals[None][2] = signal_2
        merged_signal = blk._merge_signals(group=None)
        self.assertDictEqual(merged_signal.to_dict(), signal_2.to_dict())

    def test_no_expiration(self):
        blk = MergeDynamicStreams()
        self.configure_block(blk, {
            "stream": "{{ $stream }}",
            "expiration": {},
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(5)
        self.assertDictEqual(
            self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
            {"A": "a", "stream": 1})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
            {"A": "a", "B": "b"})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
            {"C": "c", "B": "b"})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][3].to_dict(),
            {"D": "d", "B": "b"})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][4].to_dict(),
            {"D": "d", "E": "e"})

    def test_with_expiration(self):
        blk = MergeDynamicStreams()
        self.configure_block(blk, {
            "stream": "{{ $stream }}",
            "expiration": {"seconds": 0.1},
        })
        blk.start()
        self.process_test_signals(blk)
        blk.stop()
        self.assert_num_signals_notified(5)
        self.assertDictEqual(
            self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
            {"A": "a", "stream": 1})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
            {"B": "b", "stream": 2})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
            {"C": "c", "B": "b"})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][3].to_dict(),
            {"D": "d", "B": "b"})
        self.assertDictContainsSubset(
            self.last_notified[DEFAULT_TERMINAL][4].to_dict(),
            {"D": "d", "E": "e"})

    def test_signal_expiration_job(self):
        blk = MergeDynamicStreams()
        blk._signals["group"]["stream"] = Signal()
        blk._expiration_jobs["group"]["stream"] = MagicMock()
        self.assertTrue("stream" in blk._signals["group"])
        self.assertTrue("stream" in blk._expiration_jobs["group"])
        blk._signal_expiration_job("group", "stream")
        self.assertFalse("stream" in blk._signals["group"])
        self.assertFalse("stream" in blk._expiration_jobs["group"])

    def test_reset_expiration_job_on_new_signal_input_1(self):
        """ Signal expiration job is not called if new signals come in """
        blk = MergeDynamicStreams()
        blk._signal_expiration_job = MagicMock()
        self.configure_block(blk, {
            "stream": "{{ $stream }}",
            "expiration": {"seconds": 0.1},
        })
        blk.start()
        blk.process_signals([Signal({"A": "a", "stream": 1})])
        sleep(0.05)
        blk.process_signals([Signal({"B": "b", "stream": 1})])
        sleep(0.05)
        blk.process_signals([Signal({"C": "c", "stream": 1})])
        sleep(0.05)
        blk.stop()
        self.assertEqual(blk._signal_expiration_job.call_count, 0)

    def test_reset_expiration_job_on_new_signal_input_2(self):
        """ Signal expiration job is not called if new signals come in """
        blk = MergeDynamicStreams()
        blk._signal_expiration_job = MagicMock()
        self.configure_block(blk, {
            "stream": "{{ $stream }}",
            "expiration": {"seconds": 0.1},
        })
        blk.start()
        blk.process_signals([Signal({"A": "a", "stream": 2})])
        sleep(0.05)
        blk.process_signals([Signal({"B": "b", "stream": 2})])
        sleep(0.05)
        blk.process_signals([Signal({"C": "c", "stream": 2})])
        sleep(0.05)
        blk.stop()
        self.assertEqual(blk._signal_expiration_job.call_count, 0)
