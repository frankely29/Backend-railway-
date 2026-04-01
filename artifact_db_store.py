from __future__ import annotations

import gzip
import hashlib
import json
import time
from typing import Any, Dict, List, Optional

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one

# This table is intentionally scoped to small generated JSON artifacts only.
# Do NOT store parquet/frame blobs here.
# Do NOT mix pickup_logs, leaderboard, miles/hours, or other runtime tables here.
ALLOWED_ARTIFACT_KEYS = {
    "assistant_outlook",
    "day_tendency_model",
    "scoring_shadow_manifest",
    "timeline",
}


def _normalize_payload(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _decode_record(record: Any) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    content_encoding = str(record["content_encoding"] or "identity")
    payload_json = record["payload_json"]
    payload_gzip = record["payload_gzip"]

    raw_json: str
    if content_encoding == "gzip":
        if payload_gzip is None:
            return None
        raw_json = gzip.decompress(bytes(payload_gzip)).decode("utf-8")
    else:
        raw_json = str(payload_json or "")

    return {
        "artifact_key": str(record["artifact_key"]),
        "artifact_scope": str(record.get("artifact_scope") or "global") if hasattr(record, "get") else str(record["artifact_scope"] or "global"),
        "payload": json.loads(raw_json) if raw_json else {},
        "payload_json": raw_json,
        "content_encoding": content_encoding,
        "updated_at_unix": int(record["updated_at_unix"]),
        "content_sha256": str(record["content_sha256"]),
        "payload_bytes": int(record["payload_bytes"]),
    }


def ensure_generated_artifact_store_schema() -> None:
    payload_gzip_type = "BYTEA" if DB_BACKEND == "postgres" else "BLOB"
    _db_exec(
        f"""
        CREATE TABLE IF NOT EXISTS generated_artifact_store (
            artifact_key TEXT PRIMARY KEY,
            artifact_scope TEXT NOT NULL DEFAULT 'global',
            payload_json TEXT NOT NULL,
            payload_gzip {payload_gzip_type} NULL,
            content_encoding TEXT NOT NULL DEFAULT 'identity',
            updated_at_unix BIGINT NOT NULL,
            content_sha256 TEXT NOT NULL,
            payload_bytes INTEGER NOT NULL
        )
        """
    )


def save_generated_artifact(artifact_key: str, payload: Any, compress: bool = False) -> Dict[str, Any]:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        raise ValueError(f"Unsupported generated artifact key: {artifact_key}")

    payload_json = _normalize_payload(payload)
    payload_bytes = len(payload_json.encode("utf-8"))
    content_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    updated_at_unix = int(time.time())
    content_encoding = "gzip" if compress else "identity"
    payload_gzip = gzip.compress(payload_json.encode("utf-8")) if compress else None

    _db_exec(
        """
        INSERT INTO generated_artifact_store (
            artifact_key, artifact_scope, payload_json, payload_gzip, content_encoding,
            updated_at_unix, content_sha256, payload_bytes
        ) VALUES (?, 'global', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(artifact_key) DO UPDATE SET
            artifact_scope=excluded.artifact_scope,
            payload_json=excluded.payload_json,
            payload_gzip=excluded.payload_gzip,
            content_encoding=excluded.content_encoding,
            updated_at_unix=excluded.updated_at_unix,
            content_sha256=excluded.content_sha256,
            payload_bytes=excluded.payload_bytes
        """,
        (
            artifact_key,
            payload_json,
            payload_gzip,
            content_encoding,
            updated_at_unix,
            content_sha256,
            payload_bytes,
        ),
    )

    return {
        "artifact_key": artifact_key,
        "content_encoding": content_encoding,
        "updated_at_unix": updated_at_unix,
        "content_sha256": content_sha256,
        "payload_bytes": payload_bytes,
    }


def load_generated_artifact(artifact_key: str) -> Optional[Dict[str, Any]]:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return None
    row = _db_query_one(
        """
        SELECT artifact_key, artifact_scope, payload_json, payload_gzip, content_encoding,
               updated_at_unix, content_sha256, payload_bytes
        FROM generated_artifact_store
        WHERE artifact_key = ?
        """,
        (artifact_key,),
    )
    return _decode_record(row)


def delete_generated_artifact(artifact_key: str) -> None:
    _db_exec("DELETE FROM generated_artifact_store WHERE artifact_key = ?", (artifact_key,))


def generated_artifact_present(artifact_key: str) -> bool:
    row = _db_query_one(
        "SELECT artifact_key FROM generated_artifact_store WHERE artifact_key = ?",
        (artifact_key,),
    )
    return bool(row)


def generated_artifact_report() -> Dict[str, Any]:
    rows = _db_query_all(
        """
        SELECT artifact_key, updated_at_unix, content_sha256, payload_bytes, content_encoding
        FROM generated_artifact_store
        ORDER BY artifact_key ASC
        """
    )
    artifacts: List[Dict[str, Any]] = []
    for row in rows:
        artifacts.append(
            {
                "artifact_key": str(row["artifact_key"]),
                "updated_at_unix": int(row["updated_at_unix"]),
                "content_sha256": str(row["content_sha256"]),
                "payload_bytes": int(row["payload_bytes"]),
                "content_encoding": str(row["content_encoding"]),
            }
        )
    return {
        "present_keys": [item["artifact_key"] for item in artifacts],
        "artifacts": artifacts,
    }
