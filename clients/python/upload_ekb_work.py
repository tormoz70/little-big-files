#!/usr/bin/env python3
"""
Upload real EKB files from ekb_work2 via Coordinator.

Default orgs: 6866, 5879, 2793, 2791, 2451, 2450, 2447, 2107, 2101, 1577-1601
Layout: <work_root>/<org_folder>/posted000/*  (skips *.pattrs)

Usage:
  python upload_ekb_work.py --wait --count 100
  python upload_ekb_work.py --wait --work-dir c:/tmp/ekb_work2 --count 50 --seed 1
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from collections import Counter
from pathlib import Path

from ekb_work import pick_random_upload, resolve_orgs
from lbf_client import LBFClient, global_shard_id
from orgs import DEFAULT_ORGS, OrgSpec


DEFAULT_WORK_DIR = Path(r"c:\tmp\ekb_work2")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload random EKB files from ekb_work2")
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    p.add_argument("--count", type=int, default=100, help="Number of random uploads")
    p.add_argument(
        "--orgs",
        type=str,
        default="",
        help="Comma-separated org folder names (default: built-in 10 orgs)",
    )
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--wait", action="store_true")
    p.add_argument("--verify-read", action="store_true")
    p.add_argument("--delay", type=float, default=0.0)
    return p.parse_args()


def parse_org_list(raw: str) -> list[OrgSpec]:
    folders = [x.strip() for x in raw.split(",") if x.strip()]
    by_folder = {o.folder: o for o in DEFAULT_ORGS}
    specs: list[OrgSpec] = []
    for folder in folders:
        if folder in by_folder:
            specs.append(by_folder[folder])
        else:
            from ekb_work import supplier_id_for_folder

            specs.append(OrgSpec(folder, supplier_id_for_folder(folder)))
    return specs


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    org_specs = parse_org_list(args.orgs) if args.orgs else DEFAULT_ORGS
    org_specs = resolve_orgs(args.work_dir, org_specs)

    client = LBFClient(args.base_url)
    if args.wait:
        print(f"Waiting for {args.base_url} ...")
        client.wait_ready()

    print(f"Work dir: {args.work_dir}")
    print(f"Orgs ({len(org_specs)}):")
    for o in org_specs:
        print(f"  folder={o.folder} supplier_id={o.supplier_id}")
    print(f"Random uploads: {args.count}")
    print(f"Coordinator: {args.base_url}")

    shard_counts: Counter[int] = Counter()
    supplier_counts: Counter[int] = Counter()
    errors = 0
    last_by_supplier: dict[int, tuple[bytes, int]] = {}

    for n in range(1, args.count + 1):
        try:
            item = pick_random_upload(args.work_dir, org_specs, rng)
        except RuntimeError as e:
            print(f"  FAIL pick #{n}: {e}")
            errors += 1
            continue

        body = item.read_bytes()
        result = client.upload(item.supplier_id, body, item.filename)
        if result.error:
            errors += 1
            print(f"  FAIL org={item.org_folder} supplier={item.supplier_id} {item.filename}: {result.error}")
            continue

        shard_counts[result.shard_id] += 1
        supplier_counts[item.supplier_id] += 1
        last_by_supplier[item.supplier_id] = (body, result.package_id)

        if n <= 5 or n % 25 == 0 or n == args.count:
            print(
                f"  #{n} org={item.org_folder} supplier={item.supplier_id} "
                f"file={item.filename} shard={result.shard_id} "
                f"global={result.package_id} mode={result.storage_mode}"
            )
        if args.delay:
            time.sleep(args.delay)

    print("\n=== Summary ===")
    print(f"Uploads OK: {sum(shard_counts.values())}, errors: {errors}")
    print(f"By shard: {dict(sorted(shard_counts.items()))}")
    print(f"By supplier: {dict(sorted(supplier_counts.items()))}")

    try:
        shards = client.list_shards()
        print("\nShard registry:")
        for s in shards:
            mb = int(s.get("total_bytes", 0)) / (1024 * 1024)
            print(f"  shard {s['shard_id']}: {s['state']} {mb:.2f} MB")
    except Exception as e:  # noqa: BLE001
        print(f"Could not fetch shards: {e}")

    if args.verify_read and last_by_supplier:
        print("\nVerify read-back (/original), one per supplier:")
        ok = 0
        for sid, (expected, pkg_id) in sorted(last_by_supplier.items()):
            body, status = client.get_original(pkg_id)
            if status == 200 and body == expected:
                ok += 1
                print(f"  supplier {sid}: package {pkg_id} shard {global_shard_id(pkg_id)} OK")
            else:
                print(f"  supplier {sid}: FAIL status={status}")
        print(f"Read-back OK: {ok}/{len(last_by_supplier)}")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
