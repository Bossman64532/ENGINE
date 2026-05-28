import yaml
import requests
import time
import uuid
import os

# Configuration
CONFIG_FILE = 'derms/each_der_control.yml'
BASE_URL = "http://192.168.149.137:5002"
CERT_PATH = '/home/engine/tls/combined/admin-combined.pem'

def load_config(path):
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return None
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def bool_to_xml(val):
    """Converts Python boolean to XML 'true'/'false' string."""
    return str(val).lower()

def generate_xml_payload(control_data):
    """
    Maps YAML control dictionary to IEEE 2030.5 XML.
    """
    # Generate unique ID for this specific event
    m_rid = uuid.uuid4().hex.upper()
    now = int(time.time())
    duration = 3600 # Default 1 hour for test scenarios
    
    description = control_data.get('description', 'Unknown Control')
    
    # Extract top-level params (Ramp rates, Delays)
    # These sit directly under <DERControl> in IEEE 2030.5
    top_level_tags = ""
    for key, value in control_data.items():
        if key not in ['base', 'description']:
            # Simple tag generation: <key>value</key>
            top_level_tags += f"    <{key}>{value}</{key}>\n"

    # Extract Base params (OpMods)
    # These sit inside <DERControlBase>
    base_data = control_data.get('base', {})
    base_tags = ""
    
    if 'opModMaxLimW' in base_data:
        # Special handling for Power Limit to include multiplier
        val = base_data['opModMaxLimW']
        base_tags += f"        <opModMaxLimW multiplier=\"0\">{val}</opModMaxLimW>\n"
    
    if 'opModConnect' in base_data:
        val = bool_to_xml(base_data['opModConnect'])
        base_tags += f"        <opModConnect>{val}</opModConnect>\n"
        
    if 'opModEnergize' in base_data:
        val = bool_to_xml(base_data['opModEnergize'])
        base_tags += f"        <opModEnergize>{val}</opModEnergize>\n"

    # Construct full XML
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DERControl xmlns="urn:ieee:std:2030.5:ns">
    <mRID>{m_rid}</mRID>
    <description>{description}</description>
    <interval>
        <duration>{duration}</duration>
        <start>{now}</start>
    </interval>
    <randomizeDuration>0</randomizeDuration>
    <randomizeStart>0</randomizeStart>
{top_level_tags}
    <DERControlBase>
{base_tags}    </DERControlBase>
</DERControl>"""
    return xml

def send_event(xml_payload, name):
    url = f"{BASE_URL}/derms/events"
    try:
        response = requests.post(
            url, 
            data=xml_payload, 
            headers={"Content-Type": "application/sep+xml"},
            cert=CERT_PATH,
            verify=False # Set to True if you have the server CA
        )
        print(f"[{response.status_code}] Sent: {name}")
        if response.status_code != 201 and response.status_code != 200:
            print(f"Error Response: {response.text}")
    except Exception as e:
        print(f"Failed to send {name}: {e}")

def main():
    config = load_config(CONFIG_FILE)
    if not config:
        return

    # Navigate: devices -> dev1 -> programs -> program 1 -> controls
    # This path depends on your exact YAML structure. 
    # Based on your provided file, we grab the first device's first program.
    try:
        controls = config['devices'][0]['programs'][0]['controls']
    except (KeyError, IndexError) as e:
        print("Error parsing YAML structure. Ensure devices -> programs -> controls exists.")
        return

    print(f"Found {len(controls)} controls. Sending scenarios...\n")

    for i, control in enumerate(controls):
        desc = control.get('description', f'Scenario {i}')
        print(f"--- Processing: {desc} ---")
        
        payload = generate_xml_payload(control)
        
        # Debug: Print XML to verify structure before sending
        # print(payload) 
        
        send_event(payload, desc)
        
        # Wait 5 seconds between events to allow server to process
        time.sleep(5) 

if __name__ == "__main__":
    main()

