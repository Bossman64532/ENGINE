"""
test_client.py  –  IEEE 2030.5 client with DR event polling and decision tree.

ALL communication is over HTTPS port 8443 with mutual TLS.
No custom endpoints, no plain HTTP, no JSON.

Events are read as m.DERControl objects from the standard DERControlList
endpoint the client is already polling.  Responses are sent back as
DERControlResponse XML to /rsps over the same TLS connection.

Decision tree:
    1. override?           1: report reason, post OVERRIDE status, stop
                           0: continue
    2. EMS enabled?        0: post REJECT, stop
                           1: continue
    3. Constraints allow?  0: post REJECT, stop
                           1: post ACCEPT, execute event
"""
from __future__ import annotations

import time

import ieee_2030_5.models as m
import ieee_2030_5.utils as utils
from ieee_2030_5.client.client import IEEE2030_5_Client

# Certificate paths
CA   = "/home/engine/tls/certs/ca.crt"
CERT = "/home/engine/tls/certs/dev1.crt"
KEY  = "/home/engine/tls/private/dev1.pem"

SERVER_HOST = "192.168.110.129"
HTTPS_PORT  = 8443

EMS_ENABLED     = True   # Is the local EMS active?
DEVICE_MAX_W    = 295    # rtgMaxW from config.yml – hard physical rated limit
OVERRIDE_ACTIVE = False  # Set True to simulate a local device override

# IEEE 2030.5 Table 27 response status codes for DERControl
STATUS_EVENT_RECEIVED              = 1
STATUS_EVENT_STARTED               = 2
STATUS_EVENT_COMPLETED             = 3
STATUS_EVENT_REJECTED_NOT_APPLICABLE    = 7
STATUS_EVENT_REJECTED_UNMET_CONSTRAINT  = 8



# Client creation

def make_client() -> IEEE2030_5_Client:
    """Create and initialise a TLS client, fetch /dcap to bootstrap links."""
    c = IEEE2030_5_Client(
        cafile=CA,
        server_hostname=SERVER_HOST,
        keyfile=KEY,
        certfile=CERT,
        server_ssl_port=HTTPS_PORT,
        debug=False,
    )
    c.device_capability("/dcap")
    return c


# Fetch DERControl events over HTTPS, no custom endpoints

def fetch_der_controls(client: IEEE2030_5_Client,
                       derc_href: str) -> list[m.DERControl]:
    """
    GET the DERControlList from the server over HTTPS mutual TLS.
    Returns a list of m.DERControl objects (empty list if none).
    """
    result = client.request(derc_href)
    if result and hasattr(result, "DERControl") and result.DERControl:
        return result.DERControl
    return []


# Post a DERControlResponse back to the server over HTTPS

def post_response(client: IEEE2030_5_Client,
                  event: m.DERControl,
                  status_code: int,
                  device_lfdi: str) -> None:
    """
    POST a DERControlResponse to /rsps – the standard 2030.5 way for a
    device to report its decision back to the server.

    status_code  : one of the STATUS_* constants above
    device_lfdi  : this device's LFDI hex string
    """
    try:
        mrid_bytes = bytes.fromhex(event.mRID) if isinstance(event.mRID, str) else event.mRID
        lfdi_bytes = bytes.fromhex(device_lfdi) if isinstance(device_lfdi, str) else device_lfdi

        resp_obj = m.DERControlResponse(
            createdDateTime=int(time.time()),
            endDeviceLFDI=lfdi_bytes,
            status=status_code,
            subject=mrid_bytes,
        )
        xml_body = utils.dataclass_to_xml(resp_obj)
        client.request("/rsps", body=xml_body, method="POST")
        print(f"  [response] status={status_code} posted to /rsps for "
              f"mRID {event.mRID[:8] if isinstance(event.mRID, str) else event.mRID.hex()[:8]}...")

    except Exception as e:
        print(f"  [response] Failed to post DERControlResponse: {e}")

# Decision tree

def evaluate_event(event: m.DERControl) -> tuple[str, str, int]:
    """
    Run the decision tree against a single m.DERControl event.

    Returns (decision, reason, status_code):
        decision    : "ACCEPT" | "REJECT" | "OVERRIDE"
        reason      : human-readable explanation (empty string for ACCEPT)
        status_code : IEEE 2030.5 Table 27 status to report back
    """
    mrid = event.mRID if isinstance(event.mRID, str) else event.mRID.hex()
    desc = event.description or "(no description)"

    # Extract DERControlBase values
    max_lim_w   = None
    op_connect  = None
    op_energize = None
    if event.DERControlBase:
        max_lim_w   = event.DERControlBase.opModMaxLimW
        op_connect  = event.DERControlBase.opModConnect
        op_energize = event.DERControlBase.opModEnergize

    print(f"\n  ┌─ Evaluating: {mrid[:16]}... '{desc}'")
    if max_lim_w is not None:
        print(f"  │  opModMaxLimW  = {max_lim_w} W  (device max: {DEVICE_MAX_W} W)")
    if op_connect is not None:
        print(f"  │  opModConnect  = {op_connect}")
    if op_energize is not None:
        print(f"  │  opModEnergize = {op_energize}")

    # Node 1: override?
    if OVERRIDE_ACTIVE:
        reason = "Local device override is active – cannot execute remote event"
        print(f"  │  override=1  →  OVERRIDE")
        print(f"  │  reason: {reason}")
        print(f"  └─ Decision: OVERRIDE")
        return "OVERRIDE", reason, STATUS_EVENT_REJECTED_NOT_APPLICABLE

    print(f"  │  override=0  →  continue")

    # Node 2: EMS enabled?
    if not EMS_ENABLED:
        reason = "EMS is disabled on this device"
        print(f"  │  EMS enabled=0  →  REJECT")
        print(f"  │  reason: {reason}")
        print(f"  └─ Decision: REJECT")
        return "REJECT", reason, STATUS_EVENT_REJECTED_NOT_APPLICABLE

    print(f"  │  EMS enabled=1  →  continue")

    # Node 3: constraints allow?
    constraint_ok     = True
    constraint_reason = ""

    # Requested limit cannot exceed the device's physical rated maximum
    if max_lim_w is not None and max_lim_w > DEVICE_MAX_W:
        constraint_ok     = False
        constraint_reason = (
            f"opModMaxLimW={max_lim_w} W exceeds device rated max {DEVICE_MAX_W} W"
        )

    # Negative limit is physically meaningless
    elif max_lim_w is not None and max_lim_w < 0:
        constraint_ok     = False
        constraint_reason = f"opModMaxLimW={max_lim_w} W is negative – invalid"

    # Refuse a grid-disconnect while EMS is actively managing load
    elif op_connect is False and EMS_ENABLED:
        constraint_ok     = False
        constraint_reason = "Disconnect requested while EMS is actively managing load"

    if not constraint_ok:
        print(f"  │  constraints allow=0  →  REJECT")
        print(f"  │  reason: {constraint_reason}")
        print(f"  └─ Decision: REJECT")
        return "REJECT", constraint_reason, STATUS_EVENT_REJECTED_UNMET_CONSTRAINT

    print(f"  │  constraints allow=1  →  ACCEPT")
    print(f"  └─ Decision: ACCEPT")
    return "ACCEPT", "", STATUS_EVENT_RECEIVED


# Apply an accepted event to the local device / EMS


def apply_event(event: m.DERControl) -> None:
    """
    Act on an ACCEPTED DERControl event.
    Replace the print stubs below with real hardware / EMS API calls.
    """
    base = event.DERControlBase
    if base is None:
        return

    if base.opModMaxLimW is not None:
        print(f"  → [ACTION] Curtail inverter output to {base.opModMaxLimW} W")
        # Todo: write to inverter via Modbus / SunSpec / local EMS API

    if base.opModConnect is False:
        print(f"  → [ACTION] Disconnect from grid")
        # Todo: open contactor / trip breaker

    if base.opModEnergize is False:
        print(f"  → [ACTION] De-energise inverter")

# Main
def main():
    print("\n" + "=" * 60)
    print("IEEE 2030.5 CLIENT  –  HTTPS port 8443, mutual TLS")
    print("=" * 60)

    # Part 1: standard 2030.5 resource walkthrough
    client = make_client()

    print("\n=== DEVICE CAPABILITY ===")
    print(client.device_capability("/dcap"))

    print("\n=== TIME ===")
    print(client.time())

    print("\n=== END DEVICES ===")
    edev_list = client.end_devices()
    for ed in edev_list.EndDevice:
        print(f"  sFDI: {ed.sFDI}  href: {ed.href}  lFDI: {ed.lFDI}")

    # Base href for this device (e.g. "/edev_38879")
    base = edev_list.EndDevice[0].href if edev_list.EndDevice else "/edev_38879"

    for label, suffix in [
        ("DEVICE CONFIGURATION",     "_cfg"),
        ("DEVICE INFORMATION",       "_di"),
        ("DEVICE STATUS",            "_dstat"),
        ("POWER STATUS",             "_ps"),
        ("REGISTRATION",             "_rg"),
        ("FUNCTION SET ASSIGNMENTS", "_fsa"),
        ("DER LIST",                 "_der"),
    ]:
        print(f"\n=== {label} ===")
        print(client.request(base + suffix))

    for label, path in [
        ("DER PROGRAMS",        "/derp"),
        ("DER PROGRAM 0",       base + "_fsa_0_derp_0"),
        ("DEFAULT DER CONTROL", base + "_fsa_0_derp_0_dderc"),
        ("DER CONTROL LIST",    base + "_fsa_0_derp_0_derc"),   # ← events live here
        # ("ACTIVE DER CONTROLS", base + "_fsa_0_derp_0_derca"),  # 500 - server bug in enddevicesfs.py
        # ("LOG EVENTS",          base + "_lel"),                  # 500 - server bug in enddevicesfs.py
        ("DER CURVES",          "/dc"),
        ("MIRROR USAGE POINTS", "/mup"),
        ("USAGE POINTS",        "/upt"),
    ]:
        print(f"\n=== {label} ===")
        print(client.request(path))

    # Paths we'll poll for events
    derc_href  = base + "_fsa_0_derp_0_derc"   # all scheduled controls
    # derca_href = base + "_fsa_0_derp_0_derca"  # currently active — 500 server bug

    # Get device LFDI for response messages
    device_lfdi = None
    if edev_list.EndDevice:
        raw = edev_list.EndDevice[0].lFDI
        device_lfdi = raw.hex() if isinstance(raw, bytes) else str(raw)

    client.disconnect()

    # Part 2: DR event polling and decision tree
    print("\n" + "=" * 60)
    print("DR EVENT POLLING  –  every 5 s, 6 rounds")
    print(f"  Scheduled : {derc_href}")
    # print(f"  Active    : {derca_href}")  # skipped — server bug on _derca
    print(f"  EMS       : {'enabled' if EMS_ENABLED else 'DISABLED'}")
    print(f"  Device max: {DEVICE_MAX_W} W")
    print(f"  Override  : {'ACTIVE' if OVERRIDE_ACTIVE else 'off'}")
    print("=" * 60)

    seen_mrids: set[str] = set()  # avoid re-evaluating the same event

    for poll_num in range(1, 7):
        print(f"\n--- Poll #{poll_num} at {time.strftime('%H:%M:%S')} ---")

        try:
            c = make_client()

            # Server time
            t = c.time()
            print(f"  Server time : {t.currentTime}")

            # Fetch scheduled controls only (_derca active endpoint has server bug)
            scheduled = fetch_der_controls(c, derc_href)
            print(f"  Scheduled DERControls : {len(scheduled)}")
            # active    = fetch_der_controls(c, derca_href)  # skipped — 500 server bug
            # print(f"  Active DERControls    : {len(active)}")

            # Evaluate any new scheduled events
            for event in scheduled:
                mrid = event.mRID if isinstance(event.mRID, str) else event.mRID.hex()

                if mrid in seen_mrids:
                    print(f"  [skip] Already evaluated {mrid[:16]}...")
                    continue
                seen_mrids.add(mrid)

                # Run the decision tree
                decision, reason, status_code = evaluate_event(event)

                # Report decision back to server over HTTPS
                if device_lfdi:
                    post_response(c, event, status_code, device_lfdi)

                # Act on decision
                if decision == "ACCEPT":
                    apply_event(event)
                elif decision == "REJECT":
                    print(f"  → [REJECTED] {reason}")
                elif decision == "OVERRIDE":
                    print(f"  → [OVERRIDE] {reason}")

            c.disconnect()

        except Exception as e:
            print(f"  ERROR during poll: {e}")

        time.sleep(5)

    print("\nDone.")


if __name__ == "__main__":
    main()
