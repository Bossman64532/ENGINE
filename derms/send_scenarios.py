#derms
import sys

sys.path.insert(0, '/root/ENGINE/ENGINE/Server/gridappsd-2030_5/ieee_2030_5')
sys.path.insert(0, '/root/ENGINE/ENGINE/Server/gridappsd-2030_5/ieee_2030_5/models/')
import argparse
import dataclasses

import time

from pathlib import Path
# from typing import Union, get_args, get_origin, get_type_hints

import requests
import yaml

import ieee_2030_5.models as m
from ieee_2030_5.utils import dataclass_to_xml
from ieee_2030_5.config import InvalidConfigFile, ServerConfiguration
import xml.dom.minidom  


from derms_config import get_base_url

parser = argparse.ArgumentParser(
    description="Send IEEE 2030.5 events from a YAML file"
)
parser.add_argument("yml_file",
                    help="Path to the YAML events file")
parser.add_argument("--url",      default="http://127.0.0.1:5002",
                    help="Base server URL")
parser.add_argument("--cert",     default="/root/tls/combined/admin-combined.pem",
                    help="Path to client certificate PEM")
parser.add_argument("--endpoint", default="/derms/events",
                    help="Server endpoint path")
parser.add_argument("--dry-run",  action="store_true",
                    help="Print XML only, do not send")
opts = parser.parse_args()

cfg_dict = yaml.safe_load(Path(opts.yml_file).read_text())
url = f"{opts.url}{opts.endpoint}"


now = int(time.time())
duration = 3600  # 1 hour

#print(cfg_dict)
control_list = cfg_dict['controls']

for i in range(len(control_list)):
    xml_event = f"""<?xml version="1.0" encoding="UTF-8"?>
<DERControl xmlns="urn:ieee:std:2030.5:ns">
<mRID>A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4</mRID>"""
    control_dict = control_list[i]
    keys_set = control_dict.keys()
    for key in keys_set:
        if key == 'description':
            xml_event += f"\n<description>{control_dict[key]}</description>"
            xml_event += f"\n<interval>\n<duration>{duration}</duration>\n<start>{now}</start>\n</interval>"
            xml_event += f"\n<randomizeDuration>0</randomizeDuration>\n<randomizeStart>0</randomizeStart>"
        if key == 'base':
            xml_event += f"\n<DERControlBase>"
            DERControlBase = control_dict[key]
            for base_key in DERControlBase.keys():
                xml_event += f"\n<{base_key}>{DERControlBase[base_key]}</{base_key}>"
            xml_event += f"\n</DERControlBase>"
    xml_event += f"\n</DERControl>"
    
    # Parse the raw flat XML string
    dom = xml.dom.minidom.parseString(xml_event)
    # Re-render it with 2 spaces of indentation, ignoring extra text spaces
    xml_event = dom.toprettyxml(indent="  ")
    # Clean up empty lines that minidom sometimes introduces
    xml_event = "\n".join([line for line in xml_event.splitlines() if line.strip()])
    
    
    if opts.dry_run:
        print(xml_event)
    else:
        response = requests.post(
        url,
        data=xml_event,
        headers={"Content-Type": "application/sep+xml"},
        cert='/home/engine/tls/combined/admin-combined.pem'
        )

        print("Status:", response.status_code)
        print("Response:", response.text)
                    


# xml_event = f"""<?xml version="1.0" encoding="UTF-8"?>
# <DERControl xmlns="urn:ieee:std:2030.5:ns">
#   <mRID>A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3E5</mRID>
#   <description>DR Event - Limit Power</description>
#   <interval>
#     <duration>{duration}</duration>
#     <start>{now}</start>
#   </interval>
#   <randomizeDuration>0</randomizeDuration>
#   <randomizeStart>0</randomizeStart>
#   <DERControlBase>
#     <opModMaxLimW multiplier="0">100</opModMaxLimW>
#     <opModConnect>true</opModConnect>
#   </DERControlBase>
# </DERControl>"""

#response = requests.post(
#    url,
#    data=xml_event,
#    headers={"Content-Type": "application/sep+xml"},
#    cert='/home/engine/tls/combined/admin-combined.pem'
#)

#print("Status:", response.status_code)
#print("Response:", response.text)

