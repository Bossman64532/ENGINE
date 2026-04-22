"""
test_client.py  –  IEEE 2030.5 client with DR event polling, decision tree,
                   and DERStatus reporting.

ALL communication is over HTTPS port 8443 with mutual TLS.
No custom endpoints, no plain HTTP, no JSON.

Events are read as m.DERControl objects from the standard DERControlList
endpoint the client is already polling.  Responses are sent back as
DERControlResponse XML to /rsps over the same TLS connection.

After every event decision, a DERStatus is PUT to:
    /edev_{edev_index}_der_{der_index}_ders
so the server records the inverter's current operating state.

Decision tree:
    1. override?           1: report reason, post OVERRIDE status, stop
                           0: continue
    2. EMS enabled?        0: post REJECT, stop
                           1: continue
    3. Constraints allow?  0: post REJECT, stop
                           1: post ACCEPT, execute event, PUT DERStatus

Status reporting back to utility:
    - POST /rsps  status=2 (started)    when event is accepted and applied
    - POST /rsps  status=3 (completed)  when event ends (disappears from server)
    - POST /rsps  status=7 or 8        when event is rejected
    - PUT  /ders                        after every decision with current inverter state
"""
from __future__ import annotations

import time

import ieee_2030_5.models as m
import ieee_2030_5.utils as utils
from ieee_2030_5.client.client import IEEE2030_5_Client

# Certificate paths
CA   = "/home/engine/tls/certs/ca.crt"
CERT = "/home/engine/tls/combined/dev1-combined.pem"
KEY  = "/home/engine/tls/combined/dev1-combined.pem"

SERVER_HOST = "192.168.110.129"
HTTPS_PORT  = 8443

EMS_ENABLED     = True   # Is the local EMS active?
DEVICE_MAX_W    = 295    # rtgMaxW from config.yml – hard physical rated limit
OVERRIDE_ACTIVE = False  # Set True to simulate a local device override

# IEEE 2030.5 Table 27 response status codes for DERControl
STATUS_EVENT_RECEIVED                   = 1
STATUS_EVENT_STARTED                    = 2
STATUS_EVENT_COMPLETED                  = 3
STATUS_EVENT_REJECTED_NOT_APPLICABLE    = 7
STATUS_EVENT_REJECTED_UNMET_CONSTRAINT  = 8

# InverterStatusType values (from models/sep.py)
INVERTER_STATUS_OFF        = 1
INVERTER_STATUS_STARTING   = 3
INVERTER_STATUS_MPPT       = 4   # normal solar tracking — what we're at on full output
INVERTER_STATUS_DERATED    = 5   # forced power reduction / curtailment active
INVERTER_STATUS_FAULTED    = 7

# LocalControlModeStatusType values
LOCAL_CONTROL_REMOTE = 1   # server is in control (normal for IEEE 2030.5)
LOCAL_CONTROL_LOCAL  = 0   # device is in local override

# OperationalModeStatusType values
OP_MODE_OFF         = 1
OP_MODE_OPERATIONAL = 2
OP_MODE_TEST        = 3

# ConnectStatusType bitmap values (1-byte, sent as hex)
# bit 0 = Connected, bit 1 = Available, bit 2 = Operating
CONNECT_CONNECTED_OPERATING = bytes([0b00000101])  # connected + operating
CONNECT_DISCONNECTED        = bytes([0b00000000])


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


# Fetch DERControl events over HTTPS

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


# Post a DERControlResponse back to the server

def post_response(client: IEEE2030_5_Client,
                  event: m.DERControl,
                  status_code: int,
                  device_lfdi: str) -> None:
    """
    POST a DERControlResponse to /rsps.

    This is the primary mechanism for reporting status back to the utility.
    The utility reads these responses to know what the device did with the event.

    status_code : one of the STATUS_* constants above
    device_lfdi : this device's LFDI hex string
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


# PUT DERStatus to server so it records the inverter's current state

def put_der_status(client: IEEE2030_5_Client,
                   ders_href: str,
                   inverter_status_value: int,
                   op_mode_value: int,
                   connected: bool) -> None:
    """
    PUT a DERStatus object to /edev_{n}_der_{n}_ders.

    This is the secondary mechanism for reporting status back to the utility.
    It tells the utility what physical state the inverter is actually in,
    independent of event decisions. Utility operators can query this at any time.

    inverter_status_value : one of the INVERTER_STATUS_* constants
    op_mode_value         : one of the OP_MODE_* constants
    connected             : True if inverter is grid-connected
    """
    now = int(time.time())

    connect_value = CONNECT_CONNECTED_OPERATING if connected else CONNECT_DISCONNECTED

    status = m.DERStatus(
        readingTime=now,

        # Is inverter grid-connected and operating?
        genConnectStatus=m.ConnectStatusType(
            dateTime=now,
            value=connect_value,
        ),

        # What is the inverter doing right now?
        inverterStatus=m.InverterStatusType(
            dateTime=now,
            value=inverter_status_value,
        ),

        # Who is in control — remote (server) or local override?
        localControlModeStatus=m.LocalControlModeStatusType(
            dateTime=now,
            value=LOCAL_CONTROL_LOCAL if OVERRIDE_ACTIVE else LOCAL_CONTROL_REMOTE,
        ),

        # Overall operational mode
        operationalModeStatus=m.OperationalModeStatusType(
            dateTime=now,
            value=op_mode_value,
        ),
    )

    try:
        xml_body = utils.dataclass_to_xml(status)
        client.request(ders_href, body=xml_body, method="PUT")
        print(f"  [status]   PUT DERStatus to {ders_href}")
        print(f"             inverterStatus={inverter_status_value}  "
              f"opMode={op_mode_value}  connected={connected}")
    except Exception as e:
        print(f"  [status]   Failed to PUT DERStatus: {e}")


# Decision tree

def evaluate_event(event: m.DERControl) -> tuple[str, str]:
    """
    Run the decision tree against a single m.DERControl event.

    Returns (decision, reason):
        decision : "ACCEPT" | "REJECT" | "OVERRIDE"
        reason   : human-readable explanation (empty string for ACCEPT)
    """
    mrid = event.mRID if isinstance(event.mRID, str) else event.mRID.hex()
    desc = event.description or "(no description)"

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
        return "OVERRIDE", reason

    print(f"  │  override=0  →  continue")

    # Node 2: EMS enabled?
    if not EMS_ENABLED:
        reason = "EMS is disabled on this device"
        print(f"  │  EMS enabled=0  →  REJECT")
        print(f"  │  reason: {reason}")
        print(f"  └─ Decision: REJECT")
        return "REJECT", reason

    print(f"  │  EMS enabled=1  →  continue")

    # Node 3: constraints allow?
    constraint_ok     = True
    constraint_reason = ""

    if max_lim_w is not None and max_lim_w > DEVICE_MAX_W:
        constraint_ok     = False
        constraint_reason = (
            f"opModMaxLimW={max_lim_w} W exceeds device rated max {DEVICE_MAX_W} W"
        )
    elif max_lim_w is not None and max_lim_w < 0:
        constraint_ok     = False
        constraint_reason = f"opModMaxLimW={max_lim_w} W is negative – invalid"
    elif op_connect is False and EMS_ENABLED:
        constraint_ok     = False
        constraint_reason = "Disconnect requested while EMS is actively managing load"

    if not constraint_ok:
        print(f"  │  constraints allow=0  →  REJECT")
        print(f"  │  reason: {constraint_reason}")
        print(f"  └─ Decision: REJECT")
        return "REJECT", constraint_reason

    print(f"  │  constraints allow=1  →  ACCEPT")
    print(f"  └─ Decision: ACCEPT")
    return "ACCEPT", ""


# Apply an accepted event and return the resulting inverter state

def apply_event(event: m.DERControl) -> tuple[int, int, bool]:
    """
    Act on an ACCEPTED DERControl event.
    Returns (inverter_status, op_mode, connected) for DERStatus reporting.
    Replace the print stubs with real hardware / EMS API calls.
    """
    base = event.DERControlBase
    if base is None:
        return INVERTER_STATUS_MPPT, OP_MODE_OPERATIONAL, True

    connected = True
    inverter_status = INVERTER_STATUS_MPPT
    op_mode = OP_MODE_OPERATIONAL

    if base.opModMaxLimW is not None:
        if base.opModMaxLimW < DEVICE_MAX_W:
            inverter_status = INVERTER_STATUS_DERATED
            print(f"  → [ACTION] Curtail inverter output to {base.opModMaxLimW} W "
                  f"({base.opModMaxLimW / DEVICE_MAX_W * 100:.0f}% of rated)")
        else:
            print(f"  → [ACTION] Inverter at full output {base.opModMaxLimW} W")
        # TODO: write to inverter via Modbus / SunSpec / local EMS API

    if base.opModConnect is False:
        connected = False
        inverter_status = INVERTER_STATUS_OFF
        op_mode = OP_MODE_OFF
        print(f"  → [ACTION] Disconnect from grid")
        # TODO: open contactor / trip breaker

    if base.opModEnergize is False:
        inverter_status = INVERTER_STATUS_OFF
        op_mode = OP_MODE_OFF
        print(f"  → [ACTION] De-energise inverter")

    return inverter_status, op_mode, connected


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

    base = edev_list.EndDevice[0].href if edev_list.EndDevice else "/edev_0"

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
        ("DER CONTROL LIST",    base + "_fsa_0_derp_0_derc"),
        ("DER CURVES",          "/dc"),
        ("MIRROR USAGE POINTS", "/mup"),
        ("USAGE POINTS",        "/upt"),
    ]:
        print(f"\n=== {label} ===")
        print(client.request(path))

    derc_href = "/derp_1_derc"       # scheduled controls for Microinverter Program 1
    ders_href = base + "_der_0_ders" # DERStatus PUT endpoint for this device

    device_lfdi = None
    if edev_list.EndDevice:
        raw = edev_list.EndDevice[0].lFDI
        device_lfdi = raw.hex() if isinstance(raw, bytes) else str(raw)

    # PUT initial DERStatus — inverter is online at full output before any events
    print(f"\n=== INITIAL DER STATUS REPORT ===")
    put_der_status(
        client=client,
        ders_href=ders_href,
        inverter_status_value=INVERTER_STATUS_MPPT,
        op_mode_value=OP_MODE_OPERATIONAL,
        connected=True,
    )

    client.disconnect()

    # Part 2: DR event polling and decision tree
    print("\n" + "=" * 60)
    print("DR EVENT POLLING  –  every 5 s, 6 rounds")
    print(f"  Scheduled : {derc_href}")
    print(f"  DERStatus : {ders_href}")
    print(f"  EMS       : {'enabled' if EMS_ENABLED else 'DISABLED'}")
    print(f"  Device max: {DEVICE_MAX_W} W")
    print(f"  Override  : {'ACTIVE' if OVERRIDE_ACTIVE else 'off'}")
    print("=" * 60)

    # seen_events  : mRID -> event object, for all events seen this session
    # evaluated    : mRIDs that have already been acted on
    # prev_mrids   : mRIDs present in last poll, to detect when events end
    seen_events: dict[str, m.DERControl] = {}
    evaluated: set[str] = set()
    prev_mrids: set[str] = set()

    for poll_num in range(1, 7):
        print(f"\n--- Poll #{poll_num} at {time.strftime('%H:%M:%S')} ---")

        try:
            c = make_client()

            t = c.time()
            print(f"  Server time : {t.currentTime}")

            scheduled = fetch_der_controls(c, derc_href)
            print(f"  Scheduled DERControls : {len(scheduled)}")

            # Build current mRID set and cache event objects
            current_mrids: set[str] = set()
            for event in scheduled:
                mrid = event.mRID if isinstance(event.mRID, str) else event.mRID.hex()
                current_mrids.add(mrid)
                seen_events[mrid] = event

            # Detect events that just ended (present last poll, gone now)
            for mrid in prev_mrids - current_mrids:
                last_event = seen_events.get(mrid)
                if last_event and device_lfdi:
                    print(f"  *** Event ended: {mrid[:16]}... — reverting to full output ***")
                    # POST status=3 (completed) so utility knows device reverted
                    post_response(c, last_event, STATUS_EVENT_COMPLETED, device_lfdi)
                    # PUT DERStatus showing inverter back to normal
                    put_der_status(
                        client=c,
                        ders_href=ders_href,
                        inverter_status_value=INVERTER_STATUS_MPPT,
                        op_mode_value=OP_MODE_OPERATIONAL,
                        connected=True,
                    )
                # Allow re-evaluation if this event reappears later
                evaluated.discard(mrid)

            # Evaluate events not yet acted on
            for event in scheduled:
                mrid = event.mRID if isinstance(event.mRID, str) else event.mRID.hex()

                if mrid in evaluated:
                    print(f"  [skip] Already evaluated {mrid[:16]}...")
                    continue

                evaluated.add(mrid)

                # Run decision tree
                decision, reason = evaluate_event(event)

                # POST response to utility
                if device_lfdi:
                    if decision == "ACCEPT":
                        # status=2: event started — device is actively applying it
                        post_response(c, event, STATUS_EVENT_STARTED, device_lfdi)
                    elif decision == "OVERRIDE":
                        post_response(c, event, STATUS_EVENT_REJECTED_NOT_APPLICABLE, device_lfdi)
                    else:  # REJECT
                        post_response(c, event, STATUS_EVENT_REJECTED_UNMET_CONSTRAINT, device_lfdi)

                # Act and PUT DERStatus
                if decision == "ACCEPT":
                    inverter_status, op_mode, connected = apply_event(event)
                    put_der_status(
                        client=c,
                        ders_href=ders_href,
                        inverter_status_value=inverter_status,
                        op_mode_value=op_mode,
                        connected=connected,
                    )

                elif decision == "REJECT":
                    print(f"  → [REJECTED] {reason}")
                    put_der_status(
                        client=c,
                        ders_href=ders_href,
                        inverter_status_value=INVERTER_STATUS_MPPT,
                        op_mode_value=OP_MODE_OPERATIONAL,
                        connected=True,
                    )

                elif decision == "OVERRIDE":
                    print(f"  → [OVERRIDE] {reason}")
                    put_der_status(
                        client=c,
                        ders_href=ders_href,
                        inverter_status_value=INVERTER_STATUS_MPPT,
                        op_mode_value=OP_MODE_OPERATIONAL,
                        connected=True,
                    )

            # Save current mRIDs for next poll's ended-event detection
            prev_mrids = current_mrids

            c.disconnect()

        except Exception as e:
            print(f"  ERROR during poll: {e}")

        time.sleep(5)

    print("\nDone.")


if __name__ == "__main__":
    main()
