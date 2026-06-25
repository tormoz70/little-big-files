"""Load real EKB payloads from ekb_work2 layout: <work_root>/<org_id>/posted000/."""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from orgs import DEFAULT_ORGS, OrgSpec, folder_name_to_supplier_id, folder_to_supplier_id


@dataclass(frozen=True)
class EkbFile:
    org_folder: str
    supplier_id: int
    path: Path
    filename: str

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()


def is_ignored_file(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".pattrs") or path.suffix.lower() == ".pattrs"


def posted_dir(work_root: Path, org_folder: str) -> Path:
    return work_root / org_folder / "posted000"


def list_posted_files(posted: Path) -> list[Path]:
    if not posted.is_dir():
        return []
    return sorted(
        p for p in posted.iterdir()
        if p.is_file() and not is_ignored_file(p)
    )


def resolve_orgs(work_root: Path, orgs: list[OrgSpec] | None = None) -> list[OrgSpec]:
    specs = orgs if orgs is not None else DEFAULT_ORGS
    available: list[OrgSpec] = []
    for spec in specs:
        posted = posted_dir(work_root, spec.folder)
        if list_posted_files(posted):
            available.append(spec)
        else:
            raise FileNotFoundError(f"no files in {posted}")
    return available


def pick_random_file(work_root: Path, spec: OrgSpec, rng: random.Random) -> EkbFile | None:
    files = list_posted_files(posted_dir(work_root, spec.folder))
    if not files:
        return None
    path = rng.choice(files)
    return EkbFile(
        org_folder=spec.folder,
        supplier_id=spec.supplier_id,
        path=path,
        filename=path.name,
    )


def pick_random_upload(work_root: Path, orgs: list[OrgSpec], rng: random.Random) -> EkbFile:
    if not orgs:
        raise ValueError("no orgs configured")
    for _ in range(len(orgs) * 5):
        spec = rng.choice(orgs)
        picked = pick_random_file(work_root, spec, rng)
        if picked is not None:
            return picked
    raise RuntimeError("no uploadable files found under posted000")


def supplier_id_for_folder(folder: str) -> int:
    sid = folder_to_supplier_id(folder)
    if sid is not None:
        return sid
    raise ValueError(f"cannot map folder to supplier_id: {folder}")
