import time
import requests
import json

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
    print(f"\nPOST {mrid}")
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


def print_result(label: str, event: dict | None, expected_status: str):
    print(f"\n[{label}]")
    if not event:
        print("Event not found in /events response")
        return False

    actual_status = event.get("currentStatus")
    is_active_now = event.get("isActiveNow")
    start = event.get("interval", {}).get("start")
    end = event.get("interval", {}).get("end")
    now = int(time.time())

    print("mRID:", event.get("mRID"))
    print("description:", event.get("description"))
    print("start:", start)
    print("end:", end)
    print("server check time:", now)
    print("currentStatus:", actual_status)
    print("isActiveNow:", is_active_now)
    print("expected:", expected_status)

    if start is not None and end is not None and start > end:
        print("FAIL: start > end")
        return False

    if actual_status == "active" and not is_active_now:
        print("FAIL: currentStatus says active but isActiveNow is false")
        return False

    passed = actual_status == expected_status
    print("PASS" if passed else "FAIL")
    return passed


def main():
    now = int(time.time())
    run_id = str(now)

    tests = [
        {
            "label": "Future Event",
            "mrid": f"TEST-FUT-{run_id}",
            "description": "Future Event",
            "start": now + 600,
            "duration": 300,
            "max_lim_w": 1000,
            "expected_status": "upcoming",
        },
        {
            "label": "Current Event",
            "mrid": f"TEST-CUR-{run_id}",
            "description": "Current Event",
            "start": now - 5,
            "duration": 10,
            "max_lim_w": 2000,
            "expected_status": "active",
        },
        {
            "label": "Past Event",
            "mrid": f"TEST-PST-{run_id}",
            "description": "Past Event",
            "start": now - 600,
            "duration": 120,
            "max_lim_w": 500,
            "expected_status": "expired",
        },
        {
            "label": "Immediate Start Event",
            "mrid": f"TEST-IMM-{run_id}",
            "description": "Immediate Start Event",
            "start": now + 6,
            "duration": 60,
            "max_lim_w": 1500,
            "expected_status": "upcoming",
            "recheck_after_sec": 7,
            "expected_after": "active",
        },
        {
            "label": "Just Expired Event",
            "mrid": f"TEST-JXP-{run_id}",
            "description": "Just Expired Event",
            "start": now - 80,
            "duration": 20,
            "max_lim_w": 750,
            "expected_status": "expired",
        },
    ]

    print(f"Testing with local time epoch: {now}")
    for test in tests:
        post_event(
            mrid=test["mrid"],
            description=test["description"],
            start=test["start"],
            duration=test["duration"],
            max_lim_w=test["max_lim_w"],
        )

    payload = fetch_events()
    print("\n/events response:")
    print(json.dumps(payload, indent=2))

    results = []

    for test in tests:
        event = find_event(payload, test["mrid"])
        passed = print_result(test["label"], event, test["expected_status"])
        results.append((test["label"], passed))

        if test.get("recheck_after_sec"):
            wait_sec = test["recheck_after_sec"]
            print(f"\nWaiting {wait_sec} seconds for transition check...")
            time.sleep(wait_sec)

            payload2 = fetch_events()
            event2 = find_event(payload2, test["mrid"])
            passed2 = print_result(
                f"{test['label']} after wait",
                event2,
                test["expected_after"],
            )
            results.append((f"{test['label']} after wait", passed2))

    print("\n=== Summary ===")
    passed_count = 0
    for label, passed in results:
        print(f"{label}: {'PASS' if passed else 'FAIL'}")
        if passed:
            passed_count += 1

    print(f"\nPassed {passed_count}/{len(results)} checks")


if __name__ == "__main__":
    main()
