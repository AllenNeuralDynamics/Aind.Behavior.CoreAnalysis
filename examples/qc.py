import typing

import numpy as np
import pandas as pd
from example import my_dataset

from aind_behavior_core_analysis.contract.harp import HarpDevice, HarpRegister
from aind_behavior_core_analysis.contract.utils import load_branch
from aind_behavior_core_analysis.qc import _base as qc

harp_behavior = my_dataset.data_streams["Behavior"]["HarpBehavior"]
load_branch(harp_behavior)


class HarpBoardTestSuite(qc.Suite):
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

    def test_implicit_null(self):
        """Check if the test is null"""
        return

    @staticmethod
    def _get_last_read(harp_register: HarpRegister) -> typing.Optional[pd.DataFrame]:
        if not harp_register.has_data:
            raise ValueError(f"Harp register: <{harp_register.name}> does not have loaded data")
        reads = harp_register.data[harp_register.data["MessageType"] == "READ"]
        return reads.iloc[-1] if len(reads) > 0 else None

    def test_yield(self):
        """Check if the test yields"""
        for i in range(10):
            yield self.fail_test(f"yield{i}", "Yield test")

    @qc.implicit_pass
    def test_read_dump_is_complete(self):
        """
        Check if the read dump from an harp device is complete
        """
        regs = self.harp_device.device_reader.device.registers.keys()
        ds = list(self.harp_device.walk_data_streams())
        has_read_dump = [(self._get_last_read(r) is not None) for r in ds if r.name in regs]
        is_all = all(has_read_dump)
        missing_regs = [r.name for r in ds if r.name in regs and self._get_last_read(r) is None]
        return (
            self.pass_test(None, "Read dump is complete")
            if is_all
            else self.fail_test(None, "Read dump is not complete", context={"missing_registers": missing_regs})
        )

    @qc.implicit_pass
    def test_request_response(self):
        """Check that each request to the device has a corresponding response"""
        if self.harp_device_commands is None:
            return self.skip_test("No harp device commands provided")
        return "yup"


class BehaviorBoardTestSuite(qc.Suite):
    WHOAMI = 1216

    def __init__(self, harp_device: HarpDevice):
        self.harp_device = harp_device

    def test_whoami(self):
        """Check if the WhoAmI value is correct"""
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


with qc.allow_null_as_pass():
    with qc.allow_skippable(False):
        runner = qc.Runner()
        runner.add_suite(HarpBoardTestSuite(harp_behavior))
        runner.add_suite(BehaviorBoardTestSuite(harp_behavior))

        results = runner.run_all_with_progress()
