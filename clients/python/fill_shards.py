#!/usr/bin/env python3
"""
Fill active shard with unique payloads until seal (dedup-safe).

Example ZIPs deduplicate on re-upload; this script posts unique XML (~100 KB)
so segment total_bytes grows and 50 MB seal triggers.

  python fill_shards.py --wait --target-mb 55
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from lbf_client import LBFClient
from suppliers import SUPPLIERS


def unique_xml(size_kb: int) -> bytes:
    pad = max(0, size_kb * 1024 - 200)
    uid = uuid.uuid4().hex
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<seans ver="3.2.0" id="{uid}">\n'
        f'  <payload>{("A" * pad)}</payload>\n'
        f'</seans>\n'
    ).encode()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fill shards with unique XML for seal testing")
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--size-kb", type=int, default=100, help="Payload size per upload")
    p.add_argument("--target-mb", type=float, default=55.0, help="Stop when active shard reaches this MB")
    p.add_argument("--max-uploads", type=int, default=2000)
    p.add_argument("--delay", type=float, default=0.0)
    return p.parse_args()


def active_shard_bytes(client: LBFClient) -> tuple[int, int]:
    shards = client.list_shards()
    active = next(s for s in shards if s.get("state") == "active")
    return int(active["shard_id"]), int(active.get("total_bytes", 0))


def main() -> int:
    args = parse_args()
    client = LBFClient(args.base_url)
    if args.wait:
        client.wait_ready()

    target_bytes = int(args.target_mb * 1024 * 1024)
    uploads = 0
    errors = 0

    print(f"Target active shard size: {args.target_mb} MB ({target_bytes} bytes)")
    print(f"Payload ~{args.size_kb} KB, suppliers: {len(SUPPLIERS)}")

    while uploads < args.max_uploads:
        shard_id, total = active_shard_bytes(client)
        if total >= target_bytes:
            print(f"Active shard {shard_id} at {total / (1024*1024):.2f} MB — done")
            break

        supplier = SUPPLIERS[uploads % len(SUPPLIERS)]
        body = unique_xml(args.size_kb)
        name = f"fill-{uuid.uuid4().hex[:8]}.xml"
        r = client.upload(supplier.id, body, name)
        uploads += 1
        if r.error:
            errors += 1
            print(f"  FAIL #{uploads}: {r.error}")
            continue
        if uploads % 50 == 0 or uploads <= 3:
            print(
                f"  #{uploads} supplier={supplier.id} shard={r.shard_id} "
                f"active_bytes={total / (1024*1024):.2f} MB"
            )
        if args.delay:
            time.sleep(args.delay)

    print("\n=== Shard registry ===")
    for s in client.list_shards():
        mb = int(s.get("total_bytes", 0)) / (1024 * 1024)
        print(f"  shard {s['shard_id']}: {s['state']} {mb:.2f} MB")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
