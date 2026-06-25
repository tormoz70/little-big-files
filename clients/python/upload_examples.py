#!/usr/bin/env python3
"""
Upload example ZIPs from examples/ as 10 suppliers via Coordinator.

Usage:
  python upload_examples.py
  python upload_examples.py --repeat 300 --wait
  python upload_examples.py --verify-read
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

from lbf_client import LBFClient, global_shard_id
from suppliers import SUPPLIERS


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_example_zips(examples_dir: Path) -> list[tuple[str, bytes]]:
    if not examples_dir.is_dir():
        raise SystemExit(f"examples dir not found: {examples_dir}")
    files = sorted(examples_dir.glob("*.zip"))
    if not files:
        raise SystemExit(f"no .zip files in {examples_dir}")
    return [(f.name, f.read_bytes()) for f in files]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload EKB example ZIPs to LBF stand")
    p.add_argument("--base-url", default="http://localhost:8080", help="Coordinator URL")
    p.add_argument("--examples-dir", type=Path, default=None, help="Path to examples/")
    p.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="How many full rounds over all zips × suppliers (use 200+ to trigger 50MB seal)",
    )
    p.add_argument("--wait", action="store_true", help="Wait for Coordinator before upload")
    p.add_argument("--verify-read", action="store_true", help="GET /original for last upload per supplier")
    p.add_argument("--delay", type=float, default=0.0, help="Sleep seconds between uploads")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    examples_dir = args.examples_dir or (repo_root() / "examples")
    zips = load_example_zips(examples_dir)

    client = LBFClient(args.base_url)
    if args.wait:
        print(f"Waiting for {args.base_url} ...")
        client.wait_ready()

    print(f"Suppliers: {len(SUPPLIERS)}, example ZIPs: {len(zips)}, repeat: {args.repeat}")
    print(f"Coordinator: {args.base_url}")

    shard_counts: Counter[int] = Counter()
    supplier_counts: Counter[int] = Counter()
    errors = 0
    last_by_supplier: dict[int, tuple[bytes, int]] = {}

    idx = 0
    for round_num in range(args.repeat):
        for zip_name, zip_body in zips:
            supplier = SUPPLIERS[idx % len(SUPPLIERS)]
            idx += 1
            result = client.upload(supplier.id, zip_body, zip_name)
            if result.error:
                errors += 1
                print(f"  FAIL supplier={supplier.id} file={zip_name}: {result.error}")
                continue
            shard_counts[result.shard_id] += 1
            supplier_counts[supplier.id] += 1
            last_by_supplier[supplier.id] = (zip_body, result.package_id)
            if (sum(shard_counts.values()) % 20) == 0 or round_num == 0:
                print(
                    f"  ok supplier={supplier.id:4d} ({supplier.name}) "
                    f"shard={result.shard_id} local={result.local_id} "
                    f"global={result.package_id} mode={result.storage_mode}"
                )
            if args.delay:
                time.sleep(args.delay)

        if args.repeat > 1 and (round_num + 1) % 10 == 0:
            try:
                shards = client.list_shards()
                active = next((s for s in shards if s.get("state") == "active"), None)
                print(f"--- round {round_num + 1}/{args.repeat} active shard: {active}")
            except Exception as e:  # noqa: BLE001
                print(f"--- round {round_num + 1}: shard poll failed: {e}")

    print("\n=== Summary ===")
    print(f"Uploads OK: {sum(shard_counts.values())}, errors: {errors}")
    print(f"By shard: {dict(sorted(shard_counts.items()))}")
    print(f"By supplier: {dict(sorted(supplier_counts.items()))}")

    try:
        shards = client.list_shards()
        print("\nShard registry:")
        for s in shards:
            print(
                f"  shard {s['shard_id']}: state={s['state']} "
                f"bytes={s.get('total_bytes', 0)} primary={s.get('primary_url')}"
            )
    except Exception as e:  # noqa: BLE001
        print(f"Could not fetch shards: {e}")

    if args.verify_read and last_by_supplier:
        print("\nVerify read-back (/original):")
        ok = 0
        for supplier in SUPPLIERS:
            if supplier.id not in last_by_supplier:
                continue
            expected, pkg_id = last_by_supplier[supplier.id]
            body, status = client.get_original(pkg_id)
            if status == 200 and body == expected:
                ok += 1
                print(f"  supplier {supplier.id}: package {pkg_id} shard {global_shard_id(pkg_id)} OK")
            else:
                print(f"  supplier {supplier.id}: FAIL status={status} len={len(body)}")
        print(f"Read-back OK: {ok}/{len(last_by_supplier)}")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
