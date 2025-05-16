import itertools
import typing
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# uv pip install git+https://github.com/AllenNeuralDynamics/aind-data-schema@release-v2.0.0
from aind_data_schema.core.quality_control import QCEvaluation, QCMetric, QCStatus, QualityControl, Status
from aind_data_schema_models.modalities import Modality
from example import my_dataset
from rich import print_json

from aind_behavior_core_analysis.harp import HarpDevice, HarpRegister
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

    @staticmethod
    def _get_last_read(harp_register: HarpRegister) -> typing.Optional[pd.DataFrame]:
        if not harp_register.has_data:
            raise ValueError(f"Harp register: <{harp_register.name}> does not have loaded data")
        reads = harp_register.data[harp_register.data["MessageType"] == "READ"]
        return reads.iloc[-1] if len(reads) > 0 else None

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

results = runner.run_all_with_progress()


t = datetime(2022, 11, 22, 0, 0, 0, tzinfo=timezone.utc)

s = QCStatus(evaluator="Automated", status=Status.PASS, timestamp=t)
sp = QCStatus(evaluator="", status=Status.PENDING, timestamp=t)


def result_to_qc_metric(result: qc.TestResult) -> typing.Optional[QCMetric]:
    if result.status in (qc.TestStatus.PASSED, qc.TestStatus.SKIPPED):
        status = QCStatus(evaluator="Automated", status=Status.PASS, timestamp=t)
    elif result.status == qc.TestStatus.FAILED:
        status = QCStatus(evaluator="Automated", status=Status.FAIL, timestamp=t)
    else:
        return None

    return QCMetric(
        name=result._test_reference.__name__ if result._test_reference is not None else "Unknown",
        description=result.description,
        value=result.result,
        status_history=[status],
    )


def to_ads(results: list[qc.TestResult]) -> QualityControl:
    groupby_test_suite = itertools.groupby(results, lambda x: x.suite_name)
    evals = []
    for suite_name, test_results in groupby_test_suite:
        _test_results = list(test_results)
        evals.append(
            QCEvaluation(
                modality=Modality.BEHAVIOR,
                stage="Raw data",
                description="todo",
                name=suite_name,
                created=t,
                notes="",
                metrics=[result_to_qc_metric(r) for r in _test_results if result_to_qc_metric(r) is not None],
            )
        )
    return QualityControl(evaluations=evals)


ads = to_ads(results)
print_json(ads.model_dump_json(indent=2))
