import os
from pathlib import Path
from typing import Type, TypeVar
from aind_data_schema.core.session import Session

from aind_behavior_vr_foraging.rig import AindVrForagingRig
from aind_behavior_vr_foraging.session import AindBehaviorSessionModel
from aind_behavior_vr_foraging.task_logic import AindVrForagingTaskLogic
from aind_behavior_services.base import SchemaVersionedModel


session_root = Path(r"C:\Users\bruno.cruz\OneDrive - Allen Institute\Desktop\Config")
session_path = session_root / "session_input.json"
task_logic_path = session_root / "tasklogic_input.json"
rig_path = session_root / "rig_input.json"


TSchema = TypeVar("TSchema", bound=SchemaVersionedModel)


def model_from_json(json_path: os.PathLike | str, model_class: Type[TSchema]) -> TSchema:
    with open(json_path, encoding="utf-8") as f:
        return model_class.model_validate_json(f.read())


session_parsed = model_from_json(session_path, AindBehaviorSessionModel)
task_logic_parsed = model_from_json(task_logic_path, AindVrForagingTaskLogic)
rig_parsed = model_from_json(rig_path, AindVrForagingRig)

ads_session = Session(experimenter_full_name=["NA"],
                      session_start_time=session_parsed.date,
                      session_type=session_parsed.experiment,
                      rig_id=rig_parsed.rig_name,
                      subject_id=session_parsed.subject,
                      data_streams=[],
                      mouse_platform_name="Mouse platform",
                      active_mouse_platform=True,
                      )
