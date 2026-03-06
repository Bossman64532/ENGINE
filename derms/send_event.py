import requests
import time

url = "http://192.168.40.128:5002/derms/events"

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
    <opModMaxLimW multiplier="0">2000</opModMaxLimW>
    <opModConnect>true</opModConnect>
  </DERControlBase>
</DERControl>"""

response = requests.post(
    url,
    data=xml_event,
    headers={"Content-Type": "application/sep+xml"}
)

print("Status:", response.status_code)
print("Response:", response.text)
