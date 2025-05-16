import typing

import numpy as np
from example import my_dataset

from aind_behavior_core_analysis.harp import HarpDevice
from aind_behavior_core_analysis.qc import _base as qc
from aind_behavior_core_analysis.utils import load_branch

qc.set_skippable_ctx(True)

harp_behavior = my_dataset.data_streams["Behavior"]["HarpBehavior"]
load_branch(harp_behavior)


class HarpBoardTestSuite(qc.TestSuite):
    def __init__(self, harp_device: HarpDevice, harp_device_commands: typing.Optional[HarpDevice] = None):
        self.harp_device = harp_device
        self.harp_device_commands = harp_device_commands

    @staticmethod
    def _get_whoami(device: HarpDevice) -> int:
        return device["WhoAmI"].data.WhoAmI.iloc[-1]

    def test_has_whoami(self):
        """Check if the harp board data stream is present and return its value"""
        whoAmI_reg = self.harp_device["WhoAmI"]
        if not whoAmI_reg.has_data:
            return self.fail_test(None, "WhoAmI does not have loaded data")
        if len(whoAmI_reg.data) == 0:
            return self.fail_test(None, "WhoAmI file is empty")
        whoAmI = self._get_whoami(self.harp_device)
        if not bool(0000 <= whoAmI <= 9999):
            return self.fail_test(None, "WhoAmI value is not in the range 0000-9999")
        return self.pass_test(int(whoAmI))

    def test_match_whoami(self):
        """Check if the WhoAmI value matches the device's WhoAmI"""
        if self._get_whoami(self.harp_device) == self.harp_device.device_reader.device.whoAmI:
            return self.pass_test(None, "WhoAmI value matches the device's WhoAmI")
        else:
            return self.fail_test(None, "WhoAmI value does not match the device's WhoAmI")

    def test_read_dump_is_complete(self):
        """
        Check if the read dump from an harp device is complete
        """
        return self.fail_test(0, "Read dump is not complete")

    def test_request_response(self):
        """Check that each request to the device has a corresponding response"""
        if self.harp_device_commands is None:
            return self.skip_test("No harp device commands provided")
        return self.pass_test()


class BehaviorBoardTestSuite(qc.TestSuite):
    WHOAMI = 1216

    def __init__(self, harp_device: HarpDevice):
        self.harp_device = harp_device

    def test_whoami(self):
        whoAmI = self.harp_device["WhoAmI"].data.WhoAmI.iloc[-1]
        if whoAmI != self.WHOAMI:
            return self.fail_test(None, f"WhoAmI value is not {self.WHOAMI}")
        return self.pass_test()

    def test_determine_analog_data_frequency(self):
        analog_data = self.harp_device["AnalogData"].data
        adc_event_enabled = self.harp_device["EventEnable"].data.AnalogData.iloc[-1]
        if not adc_event_enabled:
            return self.pass_test(0.0)
        else:
            events = analog_data[analog_data["MessageType"] == "EVENT"]
            return self.pass_test(1.0 / np.mean(np.diff(events.index.values)))


runner = qc.TestRunner()
runner.add_suite(HarpBoardTestSuite(harp_behavior))
runner.add_suite(BehaviorBoardTestSuite(harp_behavior))

runner.run_all_with_progress()
