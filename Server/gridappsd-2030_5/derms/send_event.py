import requests
import time

<<<<<<< HEAD
url = "http://192.168.110.128:5002/derms/events"

=======
url = "http://192.168.110.128:5002/admin/derp/1/derc"
>>>>>>> Joon_Engine
now = int(time.time())
duration = 3600  # 1 hour

xml_event = f"""<?xml version="1.0" encoding="UTF-8"?>
<DERControl xmlns="urn:ieee:std:2030.5:ns">
  <mRID>A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4</mRID>
  <description>DR Event - Limit Power</description>
  <interval>
    <duration>{duration}</duration>
    <start>{now}</start>
  </interval>
  <randomizeDuration>0</randomizeDuration>
  <randomizeStart>0</randomizeStart>
  <DERControlBase>
<<<<<<< HEAD
    <opModMaxLimW multiplier="0">2000</opModMaxLimW>
=======
    <opModMaxLimW multiplier="0">148</opModMaxLimW>
>>>>>>> Joon_Engine
    <opModConnect>true</opModConnect>
  </DERControlBase>
</DERControl>"""

response = requests.post(
    url,
    data=xml_event,
    headers={"Content-Type": "application/sep+xml"}
)
<<<<<<< HEAD

=======
>>>>>>> Joon_Engine
print("Status:", response.status_code)
print("Response:", response.text)
