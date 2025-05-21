import typing as t

import pandas as pd

from ..contract.harp import HarpDevice, HarpRegister
from .base import Suite


class HarpDeviceTestSuite(Suite):
    """Test suite for the a generic Harp device. All harp devices are expected to
    pass these tests."""

    def __init__(self, harp_device: HarpDevice, harp_device_commands: t.Optional[HarpDevice] = None):
        """
        Initialize the Harp device test suite.
        Parameters
        ----------
        harp_device : HarpDevice
            The HarpDevice data stream to test.
        harp_device_commands : HarpDevice data stream corresponding to the commands sent to the device, optional
            If None, tests requiring the commands will be skipped.
        """

        self.harp_device = harp_device
        self.harp_device_commands = harp_device_commands

    # Helper functions
    # ----------------
    @staticmethod
    def _get_whoami(device: HarpDevice) -> int:
        return device["WhoAmI"].data.WhoAmI.iloc[-1]

    @staticmethod
    def _get_last_read(harp_register: HarpRegister) -> t.Optional[pd.DataFrame]:
        if not harp_register.has_data:
            raise ValueError(f"Harp register: <{harp_register.name}> does not have loaded data")
        reads = harp_register.data[harp_register.data["MessageType"] == "READ"]
        return reads.iloc[-1] if len(reads) > 0 else None

    # Tests
    # -----
    def test_has_whoami(self):
        """Check if the harp board data stream is present and return its value"""
        who_am_i_reg: HarpRegister = self.harp_device["WhoAmI"]
        if not who_am_i_reg.has_data:
            return self.fail_test(None, "WhoAmI does not have loaded data")
        if len(who_am_i_reg.data) == 0:
            return self.fail_test(None, "WhoAmI file is empty")
        who_am_i = self._get_whoami(self.harp_device)
        if not bool(0000 <= who_am_i <= 9999):
            return self.fail_test(who_am_i, "WhoAmI value is not in the range 0000-9999")
        return self.pass_test(int(who_am_i))

    def test_match_whoami_to_yml(self):
        """Check if the WhoAmI value matches the device's WhoAmI"""
        if self._get_whoami(self.harp_device) == self.harp_device.device_reader.device.whoAmI:
            return self.pass_test(True, "WhoAmI value matches the device's WhoAmI")
        else:
            return self.fail_test(False, "WhoAmI value does not match the device's WhoAmI")

    def test_read_dump_is_complete(self):
        """
        Check if the read dump from an harp device is complete
        """
        regs = self.harp_device.device_reader.device.registers.keys()
        ds = list(self.harp_device.walk_data_streams())
        has_read_dump = [(self._get_last_read(r) is not None) for r in ds if r.name in regs]
        missing_regs = [reg.name for reg in ds if reg.name in regs and self._get_last_read(reg) is None]
        return (
            self.pass_test(True, "Read dump is complete")
            if all(has_read_dump)
            else self.fail_test(False, "Read dump is not complete", context={"missing_registers": missing_regs})
        )

    def test_request_response(self):
        """Check that each request to the device has a corresponding response"""
        if self.harp_device_commands is None:
            return self.skip_test("No harp device commands provided")

        op_ctr: pd.DataFrame = self.harp_device_commands["OperationControl"].data
        op_ctr = op_ctr[op_ctr["MessageType"] == "WRITE"]
        op_ctr = op_ctr.index.values[0]

        reg_error = []
        for req_reg in self.harp_device_commands.walk_data_streams():
            if req_reg.has_data:  # Only data streams with data can be checked
                # Only "Writes" will be considered, but in theory we could also check "Reads"
                requests: pd.DataFrame = req_reg.data[req_reg.data["MessageType"] == "WRITE"]
                rep_reg = self.harp_device[req_reg.name]
                replies: pd.DataFrame = rep_reg.data[rep_reg.data["MessageType"] == "WRITE"]

                # All responses must, by definition, be timestamped AFTER the request
                if len(requests) > 0:
                    requests = requests[requests.index >= op_ctr]
                    replies = replies[replies.index >= op_ctr]
                    if len(requests) != len(replies):
                        reg_error.append(
                            {"register": req_reg.name, "requests": len(requests), "responses": len(replies)}
                        )

        if len(reg_error) == 0:
            return self.pass_test(
                None,
                "Request/Response check passed. All requests have a corresponding response.",
            )
        else:
            return self.fail_test(
                None,
                "Request/Response check failed. Some requests do not have a corresponding response.",
                context={"register_errors": reg_error},
            )

    def test_monotonicity(self):
        """
        Check that the harp device registers' timestamps are monotonic
        """
        reg_errors = []
        reg: HarpRegister
        for reg in self.harp_device.walk_data_streams():
            for message_type, reg_type_data in reg.data.groupby("MessageType", observed=True):
                if not reg_type_data.index.is_monotonic_increasing:
                    reg_errors.append(
                        {
                            "register": reg.name,
                            "message_type": message_type,
                            "is_monotonic": reg.data.index.is_monotonic_increasing,
                        }
                    )
        if len(reg_errors) == 0:
            return self.pass_test(
                None,
                "Monotonicity check passed. All registers are monotonic.",
            )
        else:
            return self.fail_test(
                None,
                "Monotonicity check failed. Some registers are not monotonic.",
                context={"register_errors": reg_errors},
            )
