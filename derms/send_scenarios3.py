import requests
import time


from derms_config import get_base_url


base_url = "http://192.168.149.137:5002" #get_base_url()
url = f"{base_url}/derms/events"


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
    <opModMaxLimW multiplier="0">100</opModMaxLimW>
    <opModConnect>true</opModConnect>
    <opModEnergize>true</opModEnergize>
  </DERControlBase>
</DERControl>"""

response = requests.post(
    url,
    data=xml_event,
    headers={"Content-Type": "application/sep+xml"},
    cert='/home/engine/tls/combined/admin-combined.pem'
)

print("Status:", response.status_code)
print("Response:", response.text)

