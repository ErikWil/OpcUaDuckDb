"""
Example: Using the opcua_python module to interact with an OPC UA server.

Prerequisites:
    - Build the Python module:  cd opcua-python && maturin develop
    - Have an OPC UA server running (e.g. on opc.tcp://localhost:4840)

This example demonstrates:
    1. Connecting to an OPC UA server
    2. Reading current values
    3. Writing a value
    4. Reading historical data
    5. Writing historical data
    6. Browsing the address space
"""

import time
from opcua_python import OpcUaClient, Vqt


def main():
    # ── 1. Connect ──────────────────────────────────────────────────────
    endpoint = "opc.tcp://localhost:4840"
    print(f"Connecting to {endpoint} ...")
    client = OpcUaClient(endpoint)
    print("Connected!\n")

    # ── 2. Read current values ──────────────────────────────────────────
    node_ids = [
        "ns=2;s=Pump01.Speed",
        "ns=2;s=FlowMeter01.FlowRate",
        "ns=2;s=TempSensor01.Temperature",
    ]
    print("Reading current values:")
    values = client.read_values(node_ids)
    for nid, vqt in zip(node_ids, values):
        print(f"  {nid}: value={vqt.value}, quality={vqt.quality}, ts={vqt.timestamp}")
    print()

    # ── 3. Write a value ────────────────────────────────────────────────
    target_node = "ns=2;s=Pump01.Speed"
    new_speed = 1500.0
    print(f"Writing {new_speed} to {target_node} ...")
    vqt = Vqt(new_speed)
    client.write_value(target_node, vqt)
    print("Write complete.\n")

    # ── 4. Read historical data ─────────────────────────────────────────
    now = time.time()
    one_hour_ago = now - 3600
    print("Reading raw history for the last hour:")
    history = client.read_history(
        ["ns=2;s=TempSensor01.Temperature"],
        from_ts=one_hour_ago,
        to_ts=now,
    )
    for node_id, vqts in history:
        print(f"  {node_id}: {len(vqts)} samples")
        for v in vqts[:5]:
            print(f"    value={v.value}, quality={v.quality}, ts={v.timestamp}")
    print()

    # Read with aggregation (10-second average)
    print("Reading 10-second average history:")
    history_avg = client.read_history(
        ["ns=2;s=TempSensor01.Temperature"],
        from_ts=one_hour_ago,
        to_ts=now,
        resample=10.0,
        aggregation="Average",
    )
    for node_id, vqts in history_avg:
        print(f"  {node_id}: {len(vqts)} aggregated samples")
    print()

    # ── 5. Write historical data ────────────────────────────────────────
    print("Writing historical values:")
    historical_values = [
        Vqt(21.5, quality=0, timestamp=now - 60),
        Vqt(22.0, quality=0, timestamp=now - 30),
        Vqt(22.3, quality=0, timestamp=now),
    ]
    client.write_history("ns=2;s=TempSensor01.Temperature", historical_values)
    print("Historical write complete.\n")

    # ── 6. Browse the address space ─────────────────────────────────────
    visited = set()

    def on_reference(ref_type: str, target_node: str) -> bool:
        """Return False if target_node has already been visited."""
        if target_node in visited:
            return False
        visited.add(target_node)
        print(f"  [{ref_type}] -> {target_node}")
        return True

    print("Browsing from i=85 (Objects folder):")
    client.browse("i=85", on_reference)
    print(f"  Discovered {len(visited)} nodes.\n")

    print("Done.")


if __name__ == "__main__":
    main()
