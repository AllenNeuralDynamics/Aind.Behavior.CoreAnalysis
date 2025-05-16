import typing

from example import my_dataset

from aind_behavior_core_analysis.harp import HarpDevice
from aind_behavior_core_analysis.qc import _base as qc
from aind_behavior_core_analysis.utils import load_branch
import numpy as np

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

    @qc.wrap_test(
        message="WhoAmI: {result} found.",
        description="Check if the harp board data stream is present and return its value",
    )
    def test_has_whoami(self) -> int:
        whoAmI_reg = self.harp_device["WhoAmI"]
        if not whoAmI_reg.has_data:
            raise qc.FailTest(None, "WhoAmI does not have loaded data")
        if len(whoAmI_reg.data) == 0:
            raise qc.FailTest(None, "WhoAmI file is empty")
        whoAmI = self._get_whoami(self.harp_device)
        if not bool(0000 <= whoAmI <= 9999):
            raise qc.FailTest(None, "WhoAmI value is not in the range 0000-9999")
        return int(whoAmI)
    
    @qc.wrap_test(
        message="WhoAmI value matches.",
        description="Check if the WhoAmI value matches the device's WhoAmI",
    )
    def test_match_whoami(self) -> None:
        if self._get_whoami(self.harp_device) == self.harp_device.device_reader.device.whoAmI:
            return
        else:
            raise qc.FailTest(None, "WhoAmI value does not match the device's WhoAmI")

    @qc.wrap_test
    def test_read_dump_is_complete(self) -> bool:
        """
        Check if the read dump from an harp device is complete
        """
        raise qc.FailTest(0, "Read dump is not complete")

    @qc.wrap_test(description="Check that each request to the device has a corresponding response")
    def test_request_response(self) -> None:
        if self.harp_device_commands is None:
            raise qc.SkipTest("No harp device commands provided")



class BehaviorBoardTestSuite(qc.TestSuite):

    WHOAMI = 1216

    def __init__(self, harp_device: HarpDevice):
        self.harp_device = harp_device

    @qc.wrap_test
    def test_whoami(self) -> None:
        whoAmI = self.harp_device["WhoAmI"].data.WhoAmI.iloc[-1]
        if whoAmI != self.WHOAMI:
            raise qc.FailTest(None, f"WhoAmI value is not {self.WHOAMI}")

    @qc.wrap_test
    def determine_analog_data_frequency(self) -> float:
        analog_data = self.harp_device["AnalogData"].data
        adc_event_enabled = self.harp_device["EventEnable"].data.AnalogData.iloc[-1]

        if not adc_event_enabled:
            return 0.0
        else:
            events = analog_data[analog_data["MessageType"] == "EVENT"]
            return 1.0 / np.mean(np.diff(events.index.values))


runner = qc.TestRunner()
runner.add_suite(BehaviorBoardTestSuite(harp_behavior))
runner.add_suite(HarpBoardTestSuite(harp_behavior))

runner.run_all_with_progress()
