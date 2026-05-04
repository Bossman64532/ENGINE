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
    start = now + 5
    duration = 30
    mrid = f"TEST-PR2-SCHED-{now}"

    post_event(
        mrid=mrid,
        description="PR2 Scheduling Accuracy Test",
        start=start,
        duration=duration,
        max_lim_w=1000,
    )

    deadline = time.time() + 30
    event = None

    while time.time() < deadline:
        payload = fetch_events()
        event = find_event(payload, mrid)

        if event and event.get("currentStatus") == "active" and event.get("executedAt") is not None:
            break

        time.sleep(0.25)

    if not event:
        print("FAIL: Event not found in /events response")
        return

    executed_at = event.get("executedAt")
    start_error_sec = event.get("startErrorSec")

    print("scheduled start:", start)
    print("executedAt:", executed_at)
    print("startErrorSec:", start_error_sec)

    if start_error_sec is None:
        print("FAIL")
        return

    print("PASS" if abs(start_error_sec) <= 1 else "FAIL")


if __name__ == "__main__":
    main()
