import time
import requests

from derms_config import get_base_url


BASE_URL = get_base_url()
POST_URL = f"{BASE_URL}/derms/events"
GET_URL = f"{BASE_URL}/events"


def build_xml(mrid: str, description: str, start: int, duration: int, max_lim_w: int) -> str:
    return f"""<DERControl xmlns="urn:ieee:std:2030.5:ns">
    <mRID>{mrid}</mRID>
    <description>{description}</description>
    <interval>
        <start>{start}</start>
        <duration>{duration}</duration>
    </interval>
    <DERControlBase>
        <opModMaxLimW>{max_lim_w}</opModMaxLimW>
        <opModConnect>true</opModConnect>
    </DERControlBase>
</DERControl>
"""


def post_event(mrid: str, description: str, start: int, duration: int, max_lim_w: int):
    xml_data = build_xml(mrid, description, start, duration, max_lim_w)
    response = requests.post(
        POST_URL,
        data=xml_data,
        headers={"Content-Type": "application/xml"},
        timeout=10,
    )
    print(f"POST {mrid}")
    print("Status:", response.status_code)
    print("Response:", response.text)
    response.raise_for_status()


def cancel_event(mrid: str):
    response = requests.post(f"{POST_URL}/{mrid}/cancel", timeout=10)
    print(f"POST {mrid}/cancel")
    print("Status:", response.status_code)
    print("Response:", response.text)
    response.raise_for_status()


def fetch_events():
    response = requests.get(GET_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def find_event(events_payload: dict, mrid: str):
    for event in events_payload.get("events", []):
        if event.get("mRID") == mrid:
            return event
    return None


def main():
    now = int(time.time())
    start = now + 60
    duration = 300
    mrid = f"TEST-PR3-CANCEL-{now}"

    post_event(
        mrid=mrid,
        description="PR3 Cancel Test",
        start=start,
        duration=duration,
        max_lim_w=1000,
    )

    time.sleep(1)

    cancel_event(mrid)

    time.sleep(1)

    payload = fetch_events()
    event = find_event(payload, mrid)

    if not event:
        print("FAIL: event not found")
        return

    print("currentStatus:", event.get("currentStatus"))
    print("cancelled:", event.get("cancelled"))

    if event.get("currentStatus") == "cancelled" and event.get("cancelled") is True:
        print("PASS")
    else:
        print("FAIL")


if __name__ == "__main__":
    main()
