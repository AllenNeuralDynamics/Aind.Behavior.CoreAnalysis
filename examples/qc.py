import typing

import pandas as pd
from example import my_dataset

from aind_behavior_core_analysis.harp import HarpDevice
from aind_behavior_core_analysis.qc import _base as qc
from aind_behavior_core_analysis.utils import load_branch

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
        message=lambda r: "WhoAmI present" if r else "WhoAmI is missing",
        description="Check if the harp board data stream is loaded",
    )
    def test_has_whoami(self) -> bool:
        """
        Check if the harp board data stream is loaded
        """

        whoAmI = self.harp_device["WhoAmI"]
        if not whoAmI.has_data:
            return False
        if len(whoAmI.data) == 0:
            return False
        if not isinstance(whoAmI.data, pd.DataFrame):
            return False
        return bool(0000 < self._get_whoami(self.harp_device) <= 9999)

    @qc.wrap_test(
        message=lambda r: "WhoAmI matches" if r else "WhoAmI does not match",
        description="Check if the WhoAmI value matches the device's WhoAmI",
    )
    def test_match_whoami(self) -> bool:
        return bool(self._get_whoami(self.harp_device) == self.harp_device.device_reader.device.whoAmI)

    @qc.wrap_test
    def test_read_dump_is_complete(self) -> bool:
        """
        Check if the read dump from an harp device is complete
        """
        raise qc.TestFailure(0, "Read dump is not complete")


[print(test) for test in HarpBoardTestSuite(harp_behavior).run_all()]
