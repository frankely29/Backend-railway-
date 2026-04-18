from __future__ import annotations

import gzip
import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one

# This table is intentionally scoped to small generated JSON artifacts only.
# Do NOT store parquet files or frame_*.json blobs here (runtime serves those from volume).
# Do NOT mix pickup_logs, leaderboard, miles/hours, or other runtime app tables here.
ALLOWED_ARTIFACT_KEYS = {
    "day_tendency_model",
    "scoring_shadow_manifest",
    "timeline",
    "trap_candidate_review",
    "month_tendency_benchmark",
}


def _row_value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except Exception:
        if hasattr(row, "get"):
            try:
                return row.get(key)
            except Exception:
                return None
    return None


def _serialize_payload(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _encode_payload(payload: Any, compress: bool) -> Tuple[bytes, str, int, str]:
    raw_bytes = _serialize_payload(payload)
    payload_uncompressed_bytes = len(raw_bytes)
    content_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    if compress:
        return gzip.compress(raw_bytes), "gzip+json", payload_uncompressed_bytes, content_sha256
    return raw_bytes, "json", payload_uncompressed_bytes, content_sha256


def _decode_record(record: Any) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    artifact_key = str(_row_value(record, "artifact_key") or "")
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return None

    content_encoding = str(_row_value(record, "content_encoding") or "json")
    payload_blob = _row_value(record, "payload_bytes")
    if payload_blob is None:
        return None

    blob_bytes = bytes(payload_blob)
    if content_encoding == "gzip+json":
        raw_json_bytes = gzip.decompress(blob_bytes)
    else:
        raw_json_bytes = blob_bytes

    payload = json.loads(raw_json_bytes.decode("utf-8")) if raw_json_bytes else {}
    metadata = {
        "artifact_key": artifact_key,
        "content_encoding": content_encoding,
        "content_sha256": str(_row_value(record, "content_sha256") or ""),
        "updated_at_unix": int(_row_value(record, "updated_at_unix") or 0),
        "payload_uncompressed_bytes": int(_row_value(record, "payload_uncompressed_bytes") or 0),
    }

    return {
        "payload": payload,
        "metadata": metadata,
        "artifact_key": metadata["artifact_key"],
        "content_encoding": metadata["content_encoding"],
        "content_sha256": metadata["content_sha256"],
        "updated_at_unix": metadata["updated_at_unix"],
        "payload_uncompressed_bytes": metadata["payload_uncompressed_bytes"],
    }


def _metadata_from_record(record: Any) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    artifact_key = str(_row_value(record, "artifact_key") or "")
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return None
    return {
        "artifact_key": artifact_key,
        "content_encoding": str(_row_value(record, "content_encoding") or "json"),
        "content_sha256": str(_row_value(record, "content_sha256") or ""),
        "updated_at_unix": int(_row_value(record, "updated_at_unix") or 0),
        "payload_uncompressed_bytes": int(_row_value(record, "payload_uncompressed_bytes") or 0),
    }


def ensure_generated_artifact_store_schema() -> None:
    payload_blob_type = "BYTEA" if DB_BACKEND == "postgres" else "BLOB"
    needs_recreate = False
    try:
        _db_query_one("SELECT payload_json FROM generated_artifact_store LIMIT 1")
        needs_recreate = True
    except Exception:
        needs_recreate = False
    if needs_recreate:
        _db_exec("DROP TABLE IF EXISTS generated_artifact_store")
    _db_exec(
        f"""
        CREATE TABLE IF NOT EXISTS generated_artifact_store (
            artifact_key TEXT PRIMARY KEY,
            payload_bytes {payload_blob_type} NOT NULL,
            content_encoding TEXT NOT NULL DEFAULT 'json',
            updated_at_unix BIGINT NOT NULL,
            content_sha256 TEXT NOT NULL,
            payload_uncompressed_bytes BIGINT NOT NULL
        )
        """
    )
    _db_exec(
        """
        DELETE FROM generated_artifact_store
        WHERE artifact_key NOT IN ('day_tendency_model','scoring_shadow_manifest','timeline','trap_candidate_review','month_tendency_benchmark')
        """
    )


def save_generated_artifact(artifact_key: str, payload: Any, compress: bool = False) -> Dict[str, Any]:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        raise ValueError(f"Unsupported generated artifact key: {artifact_key}")

    payload_bytes, content_encoding, payload_uncompressed_bytes, content_sha256 = _encode_payload(payload, compress)
    updated_at_unix = int(time.time())
    if DB_BACKEND == "postgres":
        _db_exec(
            """
            INSERT INTO generated_artifact_store (
                artifact_key, payload_bytes, content_encoding,
                updated_at_unix, content_sha256, payload_uncompressed_bytes
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (artifact_key) DO UPDATE SET
                payload_bytes=EXCLUDED.payload_bytes,
                content_encoding=EXCLUDED.content_encoding,
                updated_at_unix=EXCLUDED.updated_at_unix,
                content_sha256=EXCLUDED.content_sha256,
                payload_uncompressed_bytes=EXCLUDED.payload_uncompressed_bytes
            """,
            (
                artifact_key,
                payload_bytes,
                content_encoding,
                updated_at_unix,
                content_sha256,
                payload_uncompressed_bytes,
            ),
        )
    else:
        _db_exec(
            """
            INSERT INTO generated_artifact_store (
                artifact_key, payload_bytes, content_encoding,
                updated_at_unix, content_sha256, payload_uncompressed_bytes
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(artifact_key) DO UPDATE SET
                payload_bytes=excluded.payload_bytes,
                content_encoding=excluded.content_encoding,
                updated_at_unix=excluded.updated_at_unix,
                content_sha256=excluded.content_sha256,
                payload_uncompressed_bytes=excluded.payload_uncompressed_bytes
            """,
            (
                artifact_key,
                payload_bytes,
                content_encoding,
                updated_at_unix,
                content_sha256,
                payload_uncompressed_bytes,
            ),
        )

    return {
        "artifact_key": artifact_key,
        "content_encoding": content_encoding,
        "updated_at_unix": updated_at_unix,
        "content_sha256": content_sha256,
        "payload_uncompressed_bytes": payload_uncompressed_bytes,
    }


def load_generated_artifact(artifact_key: str) -> Optional[Dict[str, Any]]:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return None
    row = _db_query_one(
        """
        SELECT artifact_key, payload_bytes, content_encoding,
               updated_at_unix, content_sha256, payload_uncompressed_bytes
        FROM generated_artifact_store
        WHERE artifact_key = ?
        """,
        (artifact_key,),
    )
    return _decode_record(row)


def load_generated_artifact_metadata(artifact_key: str) -> Optional[Dict[str, Any]]:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return None
    row = _db_query_one(
        """
        SELECT artifact_key, content_encoding, updated_at_unix, content_sha256, payload_uncompressed_bytes
        FROM generated_artifact_store
        WHERE artifact_key = ?
        """,
        (artifact_key,),
    )
    return _metadata_from_record(row)


def delete_generated_artifact(artifact_key: str) -> None:
    _db_exec("DELETE FROM generated_artifact_store WHERE artifact_key = ?", (artifact_key,))


def generated_artifact_present(artifact_key: str) -> bool:
    if artifact_key not in ALLOWED_ARTIFACT_KEYS:
        return False
    row = _db_query_one(
        "SELECT artifact_key FROM generated_artifact_store WHERE artifact_key = ?",
        (artifact_key,),
    )
    return bool(row)


def generated_artifact_report() -> Dict[str, Any]:
    rows = _db_query_all(
        """
        SELECT artifact_key, updated_at_unix, content_sha256, payload_uncompressed_bytes, content_encoding
        FROM generated_artifact_store
        ORDER BY artifact_key ASC
        """
    )
    artifacts: List[Dict[str, Any]] = []
    for row in rows:
        artifacts.append(
            {
                "artifact_key": str(_row_value(row, "artifact_key") or ""),
                "updated_at_unix": int(_row_value(row, "updated_at_unix") or 0),
                "content_sha256": str(_row_value(row, "content_sha256") or ""),
                "payload_uncompressed_bytes": int(_row_value(row, "payload_uncompressed_bytes") or 0),
                "content_encoding": str(_row_value(row, "content_encoding") or "json"),
            }
        )
    return {
        "present_keys": [item["artifact_key"] for item in artifacts],
        "artifacts": artifacts,
    }
