#!/usr/bin/env python3
"""
Load generator: random uploads from ekb_work2 (default) or examples/*.zip.

  python load_test.py --rate 5 --duration 3600
  python load_test.py --examples --rate 2 --duration 60
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

from ekb_work import pick_random_upload, resolve_orgs
from lbf_client import LBFClient
from orgs import DEFAULT_ORGS
from suppliers import SUPPLIERS

DEFAULT_WORK_DIR = Path(r"c:\tmp\ekb_work2")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR, help="ekb_work2 root")
    p.add_argument("--examples", action="store_true", help="Use examples/*.zip and synthetic suppliers 1001-1010")
    p.add_argument("--rate", type=float, default=2.0, help="Uploads per second")
    p.add_argument("--duration", type=float, default=30.0, help="Seconds to run")
    p.add_argument("--duplicate-ratio", type=float, default=0.3, help="Fraction of duplicate re-uploads")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    client = LBFClient(args.base_url)
    client.wait_ready()

    rng = random.Random(args.seed)
    interval = 1.0 / args.rate if args.rate > 0 else 0
    deadline = time.time() + args.duration
    ok = err = 0
    shard_hist: dict[int, int] = {}
    recent: list[tuple[int, str, bytes]] = []
    max_recent = 500

    if args.examples:
        zips = sorted((repo_root() / "examples").glob("*.zip"))
        if not zips:
            print("No examples/*.zip found", file=sys.stderr)
            return 1
        payloads = [(z.name, z.read_bytes()) for z in zips]
        print(f"Load test (examples): {args.rate}/s for {args.duration}s, {len(SUPPLIERS)} suppliers")
    else:
        if not args.work_dir.is_dir():
            print(f"Work dir not found: {args.work_dir}", file=sys.stderr)
            return 1
        org_specs = resolve_orgs(args.work_dir, DEFAULT_ORGS)
        print(
            f"Load test (ekb_work2): {args.rate}/s for {args.duration}s, "
            f"{len(org_specs)} orgs from {args.work_dir}"
        )

    while time.time() < deadline:
        if args.examples:
            supplier = random.choice(SUPPLIERS)
            name, body = random.choice(payloads)
            supplier_id = supplier.id
        elif random.random() < args.duplicate_ratio and recent:
            supplier_id, name, body = rng.choice(recent)
        else:
            try:
                item = pick_random_upload(args.work_dir, org_specs, rng)
            except RuntimeError as e:
                err += 1
                print(f"pick failed: {e}", file=sys.stderr)
                if interval:
                    time.sleep(interval)
                continue
            body = item.read_bytes()
            name = item.filename
            supplier_id = item.supplier_id
            recent.append((supplier_id, name, body))
            if len(recent) > max_recent:
                recent.pop(0)

        r = client.upload(supplier_id, body, name)
        if r.error:
            err += 1
        else:
            ok += 1
            shard_hist[r.shard_id] = shard_hist.get(r.shard_id, 0) + 1
        if interval:
            time.sleep(interval)

    print(f"Done: ok={ok} err={err} shard_hist={shard_hist}")
    for s in client.list_shards():
        print(f"  shard {s['shard_id']}: {s['state']} total_bytes={s.get('total_bytes', 0)}")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
