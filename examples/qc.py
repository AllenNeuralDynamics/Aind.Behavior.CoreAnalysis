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
        return bool(0000 < whoAmI.data.WhoAmI.iloc[-1] <= 9999)

    @qc.wrap_test(
        message=lambda r: "WhoAmI matches" if r else "WhoAmI does not match",
        description="Check if the WhoAmI value matches the device's WhoAmI",
    )
    def test_match_whoami(self) -> bool:
        return bool(self.harp_device["WhoAmI"].data.WhoAmI.iloc[-1] == self.harp_device.device_reader.device.whoAmI + 1)


[print(test.description) for test in HarpBoardTestSuite(harp_behavior).run_all()]
