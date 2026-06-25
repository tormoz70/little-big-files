"""Ten test orgs from ekb_work2 (folder name -> supplier_id for API)."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class OrgSpec:
    """EKB work directory org folder and Coordinator supplier_id."""

    folder: str
    supplier_id: int

    @property
    def name(self) -> str:
        return self.folder


DEFAULT_ORGS: list[OrgSpec] = [
    OrgSpec("6866", 6866),
    OrgSpec("5879", 5879),
    OrgSpec("2793", 2793),
    OrgSpec("2791", 2791),
    OrgSpec("2451", 2451),
    OrgSpec("2450", 2450),
    OrgSpec("2447", 2447),
    OrgSpec("2107", 2107),
    OrgSpec("2101", 2101),
    OrgSpec("1577-1601", 1577),
]


def org_folders() -> list[str]:
    return [o.folder for o in DEFAULT_ORGS]


def supplier_ids() -> list[int]:
    return [o.supplier_id for o in DEFAULT_ORGS]


def folder_to_supplier_id(folder: str) -> int | None:
    for o in DEFAULT_ORGS:
        if o.folder == folder:
            return o.supplier_id
    return folder_name_to_supplier_id(folder)


def folder_name_to_supplier_id(folder: str) -> int | None:
    if re.fullmatch(r"\d+", folder):
        return int(folder)
    m = re.fullmatch(r"(\d+)-(\d+)", folder)
    if m:
        return int(m.group(1))
    return None
