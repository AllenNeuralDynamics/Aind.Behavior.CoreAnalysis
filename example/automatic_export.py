import harp
from pathlib import Path
import re
import pynwb as nwb
import datetime

ROOT = Path(r"example/Behavior.harp")
PREFIX = "Register__"
device_reader = harp.create_reader(ROOT / "device.yml")


available_registers = ROOT.glob("*.bin")

nwbfile = nwb.NWBFile(
    session_description="foo",
    identifier="foo",  # required
    session_start_time=datetime.datetime.now(),  # required
    session_id="foo",  
    experimenter=[
        "baz",
    ],  
    lab="bar",  
    institution="University of baz",  
    experiment_description="baz bar foo",  
    related_publications="bazbar",
)


nwbfile.subject = nwb.file.Subject(
    subject_id="foo",
    description="baz",
    species="bar",
    weight=0.0,
)


behavior_module = nwbfile.create_processing_module(name="behavior", description="Processed behavioral data")


for r in available_registers: 
    fname = r
    register_name = re.sub(rf"^{re.escape(PREFIX)}|\.bin$", "", r.name)
    if register_name in device_reader.registers:
        try:
            _this_reader = device_reader.registers[register_name]
            _data = _this_reader.read(fname)
            for col in _data:
                time_series = nwb.TimeSeries(
                    name=f"{register_name}.{col}",
                    data=_data[col].values,
                    timestamps=_data.index.values,
                    description=device_reader.registers[register_name].register.description,
                    unit="",
                )
                behavioral_events = nwb.behavior.BehavioralEvents(
                    time_series=time_series, name=f"{register_name}.{col}"
                )
                behavior_module.add(behavioral_events)
        except ValueError as e:  # todo fix this parser
            if register_name != "DeviceName":
                raise (e)
        except KeyError as e:
            if register_name != "DeviceName":
                raise (e)

print(nwbfile)

with nwb.NWBHDF5IO("test.hdf5", mode="w") as write_io:
    write_io.write(nwbfile)

with nwb.NWBHDF5IO("test.hdf5", mode='r') as read_io:
    nwbfile = read_io.read()
    print(nwbfile)