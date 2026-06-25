"""Ten test suppliers for the local stand."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Supplier:
    id: int
    name: str
    region: str


SUPPLIERS: list[Supplier] = [
    Supplier(1001, "ekb-north-01", "north"),
    Supplier(1002, "ekb-north-02", "north"),
    Supplier(1003, "ekb-south-01", "south"),
    Supplier(1004, "ekb-south-02", "south"),
    Supplier(1005, "ekb-west-01", "west"),
    Supplier(1006, "ekb-west-02", "west"),
    Supplier(1007, "ekb-east-01", "east"),
    Supplier(1008, "ekb-east-02", "east"),
    Supplier(1009, "ekb-central-01", "central"),
    Supplier(1010, "ekb-central-02", "central"),
]


def supplier_ids() -> list[int]:
    return [s.id for s in SUPPLIERS]
