"""HTTP client for little-big-files Coordinator API."""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests


SHARD_SHIFT = 48


def global_shard_id(package_id: int) -> int:
    return package_id >> SHARD_SHIFT


def global_local_id(package_id: int) -> int:
    return package_id & ((1 << SHARD_SHIFT) - 1)


def normalize_shard(raw: dict[str, Any]) -> dict[str, Any]:
    """Coordinator may return Go field names (ShardID) or json tags (shard_id)."""
    return {
        "shard_id": raw.get("shard_id", raw.get("ShardID")),
        "state": raw.get("state", raw.get("State")),
        "primary_url": raw.get("primary_url", raw.get("PrimaryURL")),
        "replica_url": raw.get("replica_url", raw.get("ReplicaURL")),
        "total_bytes": raw.get("total_bytes", raw.get("TotalBytes", 0)),
    }


@dataclass
class UploadResult:
    supplier_id: int
    filename: str
    package_id: int
    shard_id: int
    local_id: int
    file_count: int
    storage_mode: str
    status_code: int
    error: str | None = None


class LBFClient:
    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def wait_ready(self, retries: int = 30, delay: float = 2.0) -> None:
        import time

        last_err: Exception | None = None
        for _ in range(retries):
            try:
                self.list_shards()
                return
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(delay)
        raise RuntimeError(f"Coordinator not ready at {self.base_url}") from last_err

    def post_package(
        self,
        supplier_id: int,
        body: bytes,
        filename: str | None = None,
    ) -> tuple[dict[str, Any], int]:
        params: dict[str, str | int] = {"supplier_id": supplier_id}
        if filename:
            params["filename"] = filename
        url = f"{self.base_url}/v1/packages?{urllib.parse.urlencode(params)}"
        resp = self.session.post(url, data=body, timeout=self.timeout)
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json(), resp.status_code
        return {"error": resp.text}, resp.status_code

    def get_package(self, package_id: int) -> tuple[dict[str, Any], int]:
        url = f"{self.base_url}/v1/packages/{package_id}"
        resp = self.session.get(url, timeout=self.timeout)
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json(), resp.status_code
        return {"error": resp.text}, resp.status_code

    def get_original(self, package_id: int) -> tuple[bytes, int]:
        url = f"{self.base_url}/v1/packages/{package_id}/original"
        resp = self.session.get(url, timeout=self.timeout)
        return resp.content, resp.status_code

    def list_shards(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/v1/admin/shards"
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return [normalize_shard(s) for s in resp.json()]

    def seal_rotate(self) -> dict[str, Any]:
        url = f"{self.base_url}/v1/admin/seal-rotate"
        resp = self.session.post(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def upload(
        self,
        supplier_id: int,
        body: bytes,
        filename: str,
    ) -> UploadResult:
        data, status = self.post_package(supplier_id, body, filename)
        if status != 201:
            return UploadResult(
                supplier_id=supplier_id,
                filename=filename,
                package_id=0,
                shard_id=-1,
                local_id=0,
                file_count=0,
                storage_mode="",
                status_code=status,
                error=str(data.get("error", data)),
            )
        pkg_id = int(data["package_id"])
        return UploadResult(
            supplier_id=supplier_id,
            filename=filename,
            package_id=pkg_id,
            shard_id=global_shard_id(pkg_id),
            local_id=global_local_id(pkg_id),
            file_count=int(data.get("file_count", 0)),
            storage_mode=str(data.get("storage_mode", "")),
            status_code=status,
        )
