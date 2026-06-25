"""Tests for ekb_work2 org folder -> supplier_id mapping."""

from __future__ import annotations

import unittest

from orgs import DEFAULT_ORGS, folder_name_to_supplier_id, folder_to_supplier_id


class TestOrgMapping(unittest.TestCase):
    def test_default_org_1577_1601(self) -> None:
        spec = next(o for o in DEFAULT_ORGS if o.folder == "1577-1601")
        self.assertEqual(1577, spec.supplier_id)

    def test_folder_name_single_org(self) -> None:
        self.assertEqual(2447, folder_name_to_supplier_id("2447"))

    def test_folder_name_network_cinema(self) -> None:
        self.assertEqual(1577, folder_name_to_supplier_id("1577-1601"))

    def test_folder_to_supplier_id_unknown(self) -> None:
        self.assertIsNone(folder_name_to_supplier_id("bad-name"))

    def test_folder_to_supplier_id_builtin(self) -> None:
        self.assertEqual(2101, folder_to_supplier_id("2101"))


if __name__ == "__main__":
    unittest.main()
