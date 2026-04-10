import io
import json
import logging
import math
import traceback
import uuid
from collections import defaultdict, deque
from typing import Any

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("etl")


def _pg_text_unique_field(val: Any) -> str | None:
    """
    Normalize text fields that participate in PostgreSQL UNIQUE ... NULLS NOT DISTINCT.
    None, '', and whitespace-only all map to None so Python grouping matches the DB.
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    s = str(val).strip()
    return None if s == "" else s


def _safe_int(val: Any) -> int | None:
    """Coerce any scalar to int; None/NaN/empty → None. Safe for dirty CSV / Supabase JSON."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and val.strip() == "":
        return None
    try:
        f = float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
    try:
        if math.isnan(f) or math.isinf(f):
            return None
    except TypeError:
        return None
    try:
        return int(f)
    except (ValueError, OverflowError):
        return None


def _safe_float(val: Any) -> float | None:
    """Coerce to float for fees; None/NaN/empty → None."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and val.strip() == "":
        return None
    try:
        x = float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from supabase import Client, create_client

import os

# Load environment variables securely from .env file
load_dotenv()

app = FastAPI(title="Logistics AI Agent API", version="1.0")

# --- Background Task Progress Store ---
# Maps task_id -> {task_id, status, progress, message}
task_store: dict[str, dict[str, Any]] = {}


def _update_task(task_id: str, progress: int, message: str, status: str = "running") -> None:
    task_store[task_id] = {
        "task_id": task_id,
        "status": status,
        "progress": progress,
        "message": message,
    }


# Fetch keys from environment
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Initialize Database Connection
supabase: Client | None = None
try:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Missing Supabase credentials in .env file!")
    else:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Connected to Supabase Successfully!")
except Exception as e:
    print(f"Database Connection Error: {e}")


def _require_supabase() -> Client:
    if supabase is None:
        raise HTTPException(
            status_code=503,
            detail="Database not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.",
        )
    return supabase


def _normalize_header(name: str) -> str:
    return (
        name.replace("\ufeff", "")
        .strip()
        .replace("-", "_")
        .replace(" ", "_")
        .lower()
    )


def _cell_to_json_safe(val: Any) -> Any:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if hasattr(val, "item"):
        try:
            return val.item()
        except Exception:
            pass
    return val


def _read_tabular_file(content: bytes) -> pd.DataFrame:
    """Read CSV or tab-separated text; infer delimiter (comma vs tab)."""
    buf = io.BytesIO(content)
    _csv_kw: dict[str, Any] = {
        "sep": None,
        "engine": "python",
        "encoding": "utf-8-sig",
        "dtype": object,
        "keep_default_na": False,
    }
    try:
        return pd.read_csv(buf, **_csv_kw)
    except Exception:
        buf.seek(0)
        return pd.read_csv(
            buf, sep="\t", encoding="utf-8-sig", dtype=object, keep_default_na=False,
        )


def _get_from_row(row: dict[str, Any], *candidates: str) -> str | None:
    """Match staging raw_row keys flexibly (hyphen / underscore / case)."""
    norm_map = {_normalize_header(k): k for k in row.keys()}
    for cand in candidates:
        key = _normalize_header(cand)
        if key in norm_map:
            v = row.get(norm_map[key])
            s = _cell_to_json_safe(v)
            if s is not None and str(s).strip() != "":
                return str(s).strip()
    return None


def _parse_quantity(row: dict[str, Any]) -> float:
    raw = _get_from_row(
        row,
        "quantity",
        "shipped_quantity",
        "requested_quantity",
        "disposed_quantity",
        "cancelled_quantity",
        "Quantity",
    )
    if raw is None:
        return 0.0
    try:
        return float(str(raw).replace(",", ""))
    except ValueError:
        return 0.0


def _group_key_for_row(row: dict[str, Any]) -> str | None:
    track = _get_from_row(
        row,
        "tracking_number",
        "tracking-number",
        "tracking_id",
        "tracking-id",
    )
    if track:
        return f"tn:{track}"
    oid = _get_from_row(
        row,
        "order_id",
        "order-id",
        "removal_order_id",
        "removal-order-id",
        "amazon_order_id",
    )
    if oid:
        return f"oid:{oid}"
    return None


# --- Removal ETL: field mapping and extraction ---

REMOVAL_FIELD_MAP: dict[str, list[str]] = {
    "order_id":           [
        "order-id",
        "order_id",
        "removal_order_id",
        "removal-order-id",
        "amazon_order_id",
        "amazon-order-id",
    ],
    "order_source":       ["order-source", "order_source", "order source"],
    "order_type":         ["order-type", "order_type", "order type"],
    "order_status":       ["order-status", "order_status"],
    "sku":                ["sku", "merchant_sku", "merchant-sku"],
    "fnsku":              ["fnsku"],
    "disposition":        ["disposition", "fc-disposition", "detailed-disposition"],
    "shipped_quantity":   ["shipped-quantity", "shipped_quantity"],
    "requested_quantity": ["requested-quantity", "requested_quantity", "quantity"],
    "cancelled_quantity": ["cancelled-quantity", "cancelled_quantity"],
    "disposed_quantity":  ["disposed-quantity", "disposed_quantity"],
    "in_process_quantity": ["in-process-quantity", "in_process_quantity", "in process quantity"],
    "tracking_number":    ["tracking-number", "tracking_number"],
    "carrier":            ["carrier", "carrier-name", "carrier_name"],
    "shipment_date":      ["carrier-shipment-date", "shipment-date", "ship-date", "shipped-date"],
    "order_date":         ["request-date", "order-date", "order_date", "request_date"],
    "last_updated_date":  ["last-updated-date", "last_updated_date", "last updated date"],
    "removal_fee":        ["removal-fee", "removal_fee", "removal fee"],
    "currency":           ["currency"],
}

_INT_REMOVAL_COLS = {
    "shipped_quantity",
    "requested_quantity",
    "cancelled_quantity",
    "disposed_quantity",
    "in_process_quantity",
}


def _resolve_import_store_id_from_upload(db: Any, organization_id: str, upload_id: str | None) -> str | None:
    """Reads import_store_id / ledger_store_id from raw_report_uploads.metadata."""
    if not upload_id:
        return None
    try:
        res = (
            db.table("raw_report_uploads")
            .select("metadata")
            .eq("id", upload_id)
            .eq("organization_id", organization_id)
            .limit(1)
            .execute()
        )
        row = (res.data or [None])[0] if isinstance(res.data, list) else res.data
        meta = row.get("metadata") if isinstance(row, dict) else None
        if not isinstance(meta, dict):
            return None
        raw = meta.get("import_store_id") or meta.get("ledger_store_id")
        if not raw:
            return None
        sid = str(raw).strip()
        uuid.UUID(sid)
        return sid
    except Exception:
        return None


def _extract_removal_row(
    raw: dict[str, Any],
    organization_id: str,
    upload_id: str | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"organization_id": organization_id}
    if upload_id:
        result["upload_id"] = upload_id

    for db_col, candidates in REMOVAL_FIELD_MAP.items():
        val = _get_from_row(raw, *candidates)
        if db_col in _INT_REMOVAL_COLS:
            result[db_col] = _safe_int(val) if val is not None else None
        elif db_col in ("order_date", "last_updated_date"):
            if val is not None:
                try:
                    d = pd.to_datetime(val, errors="coerce")
                    result[db_col] = d.date().isoformat() if pd.notna(d) else None
                except Exception:
                    result[db_col] = None
            else:
                result[db_col] = None
        elif db_col == "removal_fee":
            result[db_col] = _safe_float(val) if val is not None else None
        else:
            result[db_col] = val

    # Coalesce SKU from FNSKU when the report only lists FNSKU (distinct 5-column keys).
    if not _pg_text_unique_field(result.get("sku")):
        fn = _get_from_row(raw, "fnsku", "asin")
        if fn:
            result["sku"] = str(fn).strip()

    # Align nullable text columns with DB NULLS NOT DISTINCT semantics before merge/upsert.
    for nullable in ("sku", "fnsku", "disposition", "tracking_number"):
        t = _pg_text_unique_field(result.get(nullable))
        result[nullable] = t

    return result


# --- Removals UPSERT: one row per amazon_staging line (source_staging_id) — migration 60519 ---

_REMOVALS_CONFLICT = "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type"
_REMOVAL_SHIPMENT_CONFLICT = (
    "organization_id,store_id,order_id,tracking_number,sku,fnsku,disposition,"
    "requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type"
)
_REMOVAL_WRITE_COLS = frozenset(REMOVAL_FIELD_MAP.keys()) | {
    "organization_id",
    "upload_id",
    "raw_data",
    "source_staging_id",
    "store_id",
}

# Columns managed exclusively by Postgres — NEVER send these in upsert/update payloads.
# Sending `id=None` triggers "null value in column 'id' violates not-null constraint".
_DB_SYSTEM_COLS = frozenset({"id", "created_at", "updated_at"})


def _removal_order_date_key(val: Any) -> str | None:
    """Normalize date column for identity tuples (YYYY-MM-DD or None)."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if hasattr(val, "isoformat"):
        try:
            return str(val.isoformat())[:10]
        except Exception:
            pass
    s = str(val).strip()
    if not s:
        return None
    return s[:10]


def _removal_null_slot_match_key(r: dict[str, Any]) -> tuple[Any, ...]:
    """Match shipment lines to NULL-tracking removal rows — canonical cross-file identity (no qty/date)."""
    sid = r.get("store_id")
    store_part = str(sid).strip() if sid is not None and str(sid).strip() != "" else ""
    return (
        str(r.get("organization_id") or "").strip(),
        store_part,
        _pg_text_unique_field(r.get("order_id")),
        _pg_text_unique_field(r.get("order_type")),
        _pg_text_unique_field(r.get("sku")),
        _pg_text_unique_field(r.get("fnsku")),
        _pg_text_unique_field(r.get("disposition")),
    )


def _removal_row_for_write(row: dict[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in _REMOVAL_WRITE_COLS if k in row}


def _merge_shipment_into_null_slot(existing: dict[str, Any], shipment: dict[str, Any]) -> dict[str, Any]:
    """Backfill tracking; fill carrier/shipment_date only when existing is empty (Wave 1)."""
    out = dict(existing)
    tn = _pg_text_unique_field(shipment.get("tracking_number"))
    if tn:
        out["tracking_number"] = tn
    inc_c = shipment.get("carrier")
    if inc_c is not None and str(inc_c).strip() != "":
        if not _pg_text_unique_field(existing.get("carrier")):
            out["carrier"] = inc_c
    inc_sd = shipment.get("shipment_date")
    if inc_sd is not None and str(inc_sd).strip() != "":
        if existing.get("shipment_date") is None or str(existing.get("shipment_date")).strip() == "":
            out["shipment_date"] = inc_sd
    return out


# expected_packages: columns we sync from Amazon (DO UPDATE); all other columns are warehouse-owned.
EXPECTED_PKG_AMAZON_COLS = frozenset({
    "organization_id",
    "store_id",
    "upload_id",
    "source_staging_id",
    "order_type",
    "order_id",
    "sku",
    "fnsku",
    "tracking_number",
    "shipped_quantity",
    "requested_quantity",
    "disposed_quantity",
    "cancelled_quantity",
    "order_status",
    "disposition",
    "order_date",
    "carrier",
    "shipment_date",
})
# Shipment-derived columns (from amazon_removals after Phase 3 enrichment): merge fill-null only.
_EP_WORKLIST_AMAZON_FILL_NULL = frozenset({
    "tracking_number",
    "carrier",
    "shipment_date",
    "order_date",
    "fnsku",
    "store_id",
    "order_type",
})
# Matches uq_expected_packages_canonical_cross_file (quantities/dates are attributes, not upsert keys).
_EXPECTED_PKG_CONFLICT = (
    "organization_id,store_id,order_id,order_type,sku,fnsku,disposition"
)


def _ep_value_absent(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and str(v).strip() == "":
        return True
    return False


def _expected_pkg_identity_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Canonical cross-file key: matches uq_expected_packages_canonical_cross_file (DB)."""
    st = row.get("store_id")
    sid = str(st).strip() if st is not None and str(st).strip() != "" else ""
    return (
        str(row.get("organization_id", "")),
        sid,
        _pg_text_unique_field(row.get("order_id")),
        _pg_text_unique_field(row.get("order_type")),
        _pg_text_unique_field(row.get("sku")),
        _pg_text_unique_field(row.get("fnsku")),
        _pg_text_unique_field(row.get("disposition")),
    )


# When merging duplicate expected_packages payloads for one ON CONFLICT key, prefer filled shipment/line fields.
_EP_UPSERT_COLLAPSE_PREFER_FIELDS = frozenset({
    "tracking_number",
    "carrier",
    "shipment_date",
    "order_type",
    "fnsku",
})


def _dedupe_merged_rows_for_expected_packages_upsert(
    merged_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Collapse rows sharing _expected_pkg_identity_key (same as upsert ON CONFLICT) to avoid duplicate-row errors."""
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    key_order: list[tuple[Any, ...]] = []
    for r in merged_rows:
        k = _expected_pkg_identity_key(r)
        if k not in groups:
            key_order.append(k)
            groups[k] = []
        groups[k].append(r)

    out: list[dict[str, Any]] = []
    collapsed = 0
    for k in key_order:
        group = groups[k]
        if len(group) == 1:
            out.append(group[0])
            continue
        collapsed += len(group) - 1
        merged = dict(group[0])
        for other in group[1:]:
            for fk in _EP_UPSERT_COLLAPSE_PREFER_FIELDS:
                if fk not in merged or _ep_value_absent(merged.get(fk)):
                    v = other.get(fk)
                    if not _ep_value_absent(v):
                        merged[fk] = v
        out.append(merged)

    return out, collapsed


def _merge_expected_pkg_amazon_only(existing: dict[str, Any], incoming_amazon: dict[str, Any]) -> dict[str, Any]:
    """Preserve scanner / warehouse fields; overwrite only Amazon-sourced columns.

    For shipment-derived fields already on amazon_removals, propagate into expected_packages but never
    replace a populated worklist value with null/empty from incoming.
    """
    out = dict(existing)
    for k, v in incoming_amazon.items():
        if k not in EXPECTED_PKG_AMAZON_COLS:
            continue
        if k in _EP_WORKLIST_AMAZON_FILL_NULL:
            if _ep_value_absent(v):
                continue
            out[k] = v
        else:
            out[k] = v
    return out


def _is_return_order_type_for_worklist(row: dict[str, Any]) -> bool:
    """expected_packages worklist: Return lines only; missing order_type counts as Return (shipment CSV often omits it)."""
    t = _pg_text_unique_field(row.get("order_type"))
    if not t:
        return True
    return t.strip().lower() == "return"


# Data Model for Amazon SP-API Sync
class AmazonOrderSync(BaseModel):
    amazon_order_id: str
    org_id: str
    store_id: str
    raw_data: dict


class SyncRemovalsRequest(BaseModel):
    organization_id: str
    upload_id: str | None = None


class GenerateWorklistRequest(BaseModel):
    organization_id: str
    upload_id: str | None = None


# 1. Root Endpoint (Health Check)
@app.get("/")
def read_root():
    return {"status": "Agent Backend is Live!", "service": "AI Logistics"}


# 2. Agent Queue Endpoint
@app.get("/agent/pending-claims")
async def get_pending_claims():
    db = _require_supabase()
    try:
        response = (
            db.table("claim_submissions").select("*").eq("status", "ready_to_send").execute()
        )
        return {"count": len(response.data), "claims": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 3. Landing Zone Endpoint for Live Amazon Data
@app.post("/sync/order")
async def save_raw_amazon_order(order: AmazonOrderSync):
    db = _require_supabase()
    try:
        data = {
            "organization_id": order.org_id,
            "store_id": order.store_id,
            "amazon_order_id": order.amazon_order_id,
            "raw_data": order.raw_data,
            "status": "synced",
        }
        db.table("marketplace_orders").upsert(data).execute()
        return {"status": "success", "message": "Order synced to Landing Zone"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- ETL: Amazon warehouse / removal file staging ---

STAGING_INSERT_BATCH = 500
# PostgREST caps responses at ~1000 rows unless paged — full-table reads must loop.
WORKLIST_FETCH_PAGE = 1000


def _fetch_org_table_all(
    db: Any,
    table: str,
    organization_id: str,
) -> list[dict[str, Any]]:
    """Fetch every row for an org (paginated). Static snapshot; no deletes between pages."""
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        res = (
            db.table(table)
            .select("*")
            .eq("organization_id", organization_id)
            .order("id")
            .range(offset, offset + WORKLIST_FETCH_PAGE - 1)
            .execute()
        )
        chunk = res.data or []
        out.extend(chunk)
        if len(chunk) < WORKLIST_FETCH_PAGE:
            break
        offset += WORKLIST_FETCH_PAGE
    return out


def _fetch_amazon_removal_shipments_by_upload(
    db: Any,
    organization_id: str,
    upload_id: str,
) -> list[dict[str, Any]]:
    """Rows archived for one import session (REMOVAL_SHIPMENT sync target)."""
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        res = (
            db.table("amazon_removal_shipments")
            .select("*")
            .eq("organization_id", organization_id)
            .eq("upload_id", upload_id)
            .order("id")
            .range(offset, offset + WORKLIST_FETCH_PAGE - 1)
            .execute()
        )
        chunk = res.data or []
        out.extend(chunk)
        if len(chunk) < WORKLIST_FETCH_PAGE:
            break
        offset += WORKLIST_FETCH_PAGE
    return out


def _removal_like_from_shipment_archive(sh: dict[str, Any]) -> dict[str, Any]:
    """Map amazon_removal_shipments → amazon_removals-shaped row for the worklist loop."""
    row = dict(sh)
    sid = sh.get("amazon_staging_id")
    if sid is not None and str(sid).strip() != "":
        row["source_staging_id"] = sid
    return row


@app.post("/etl/upload-removal")
async def etl_upload_removal(
    file: UploadFile = File(...),
    organization_id: str = Form(...),
    batch_id: str | None = Form(None),
):
    """
    Accept a CSV or TXT report, parse with pandas, and insert raw rows into
    `amazon_staging` with a shared batch_id (stored in raw_row and as top-level
    batch_id when the column exists).
    """
    db = _require_supabase()
    try:
        uuid.UUID(organization_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="organization_id must be a valid UUID.")

    try:
        raw_bytes = await file.read()
        if not raw_bytes:
            raise HTTPException(status_code=400, detail="Empty file.")

        batch = batch_id.strip() if batch_id and batch_id.strip() else str(uuid.uuid4())
        try:
            uuid.UUID(batch)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="batch_id must be a valid UUID when provided.",
            )

        df = _read_tabular_file(raw_bytes)
        df.columns = [_normalize_header(str(c)) for c in df.columns]
        df = df.dropna(axis=1, how="all")

        if df.empty:
            raise HTTPException(status_code=400, detail="No data rows in file.")

        rows_to_insert: list[dict[str, Any]] = []
        for _, series in df.iterrows():
            row_dict: dict[str, Any] = {}
            for col in df.columns:
                row_dict[col] = _cell_to_json_safe(series[col])

            payload = {"batch_id": batch, **row_dict}

            snapshot_date = None
            for date_key in (
                "date",
                "date_time",
                "posted_date",
                "order_date",
                "request_date",
                "snapshot_date",
            ):
                if date_key in payload and payload[date_key]:
                    try:
                        d = pd.to_datetime(payload[date_key], errors="coerce")
                        if pd.notna(d):
                            snapshot_date = d.date().isoformat()
                            break
                    except Exception:
                        continue

            row_insert: dict[str, Any] = {
                "organization_id": organization_id,
                "raw_row": payload,
                "batch_id": batch,
            }
            if snapshot_date:
                row_insert["snapshot_date"] = snapshot_date

            rows_to_insert.append(row_insert)

        inserted = 0
        for i in range(0, len(rows_to_insert), STAGING_INSERT_BATCH):
            chunk = rows_to_insert[i : i + STAGING_INSERT_BATCH]
            db.table("amazon_staging").insert(chunk).execute()
            inserted += len(chunk)

        return {
            "status": "success",
            "batch_id": batch,
            "rows_inserted": inserted,
            "message": "Staging load completed.",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"ETL upload failed: {e!s}",
        )


@app.post("/etl/process-staging")
async def etl_process_staging(organization_id: str = Form(...)):
    """
    Read `amazon_staging`, group rows by tracking_number or (if empty) order_id,
    create `expected_pallets` + `expected_items`, then clear staging for the org.
    """
    db = _require_supabase()
    try:
        uuid.UUID(organization_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="organization_id must be a valid UUID.")

    try:
        res = (
            db.table("amazon_staging")
            .select("id,organization_id,raw_row")
            .eq("organization_id", organization_id)
            .execute()
        )
        staging_rows = res.data or []
        if not staging_rows:
            return {
                "status": "success",
                "message": "No staging rows to process.",
                "pallets_created": 0,
                "items_created": 0,
                "staging_cleared": False,
            }

        # Build groups: group_key -> list of raw_row dicts
        groups: dict[str, list[dict[str, Any]]] = {}
        skipped = 0
        for row in staging_rows:
            raw = row.get("raw_row")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    skipped += 1
                    continue
            if not isinstance(raw, dict):
                skipped += 1
                continue

            gkey = _group_key_for_row(raw)
            if not gkey:
                skipped += 1
                continue
            groups.setdefault(gkey, []).append(raw)

        if not groups:
            raise HTTPException(
                status_code=400,
                detail="Could not derive tracking_number or order_id for any row; nothing to process.",
            )

        pallet_payloads: list[dict[str, Any]] = []
        group_order: list[str] = []

        for gkey, raws in groups.items():
            group_order.append(gkey)
            first = raws[0]
            track = _get_from_row(
                first,
                "tracking_number",
                "tracking-number",
                "tracking_id",
                "tracking-id",
            )
            oid = _get_from_row(
                first,
                "order_id",
                "order-id",
                "removal_order_id",
                "removal-order-id",
                "amazon_order_id",
            )
            batch = _get_from_row(first, "batch_id")

            pallet_payloads.append(
                {
                    "organization_id": organization_id,
                    "status": "Pending",
                    "tracking_number": track,
                    "order_id": oid,
                    "batch_id": batch if batch else None,
                }
            )

        pal_res = db.table("expected_pallets").insert(pallet_payloads).select("id").execute()
        pallet_ids = [r["id"] for r in (pal_res.data or [])]

        if len(pallet_ids) != len(group_order):
            raise HTTPException(
                status_code=500,
                detail="Insert into expected_pallets did not return all IDs.",
            )

        gkey_to_pallet = dict(zip(group_order, pallet_ids, strict=True))

        item_rows: list[dict[str, Any]] = []
        for gkey, raws in groups.items():
            pid = gkey_to_pallet[gkey]
            sku_qty: dict[str, float] = {}
            for raw in raws:
                sku = _get_from_row(raw, "sku", "fnsku", "asin")
                if not sku:
                    continue
                qty = _parse_quantity(raw)
                sku_qty[sku] = sku_qty.get(sku, 0.0) + qty

            for sku, qty in sku_qty.items():
                item_rows.append(
                    {
                        "expected_pallet_id": pid,
                        "sku": sku,
                        "quantity": qty,
                    }
                )

        items_created = 0
        if item_rows:
            for i in range(0, len(item_rows), STAGING_INSERT_BATCH):
                chunk = item_rows[i : i + STAGING_INSERT_BATCH]
                db.table("expected_items").insert(chunk).execute()
                items_created += len(chunk)

        db.table("amazon_staging").delete().eq("organization_id", organization_id).execute()

        return {
            "status": "success",
            "pallets_created": len(pallet_ids),
            "items_created": items_created,
            "rows_skipped_no_key": skipped,
            "staging_cleared": True,
            "message": "Staging processed and cleared.",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"ETL process failed: {e!s}",
        )


# --- Background task: staging -> amazon_removals ---
# Must be a plain def (not async) so FastAPI runs it in a thread pool,
# keeping the synchronous supabase-py client off the event loop.
# IMPORTANT: never call _require_supabase() here — it raises HTTPException,
# which is a Starlette request-level exception and will cause
# "RuntimeError: Caught handled exception, but response already started"
# when raised outside a request context (i.e., in a background thread).

def _run_sync_removals(
    task_id: str,
    organization_id: str,
    upload_id: str | None,
) -> None:
    """
    ETL Phase 3: amazon_staging → amazon_removals (+ append-only shipment history).

    Identity: UPSERT uses DB conflict key on amazon_removals (staging + business line).

    Same-key rows in one import are merged: quantity columns are summed; other
    fields use the last non-null value in file order (Amazon updates win).

    Shipment rows with tracking append to amazon_removal_shipments (full raw history).
    NULL-tracking DB rows matched on canonical cross-file key (org, store, order_id,
    order_type, sku, fnsku, disposition) are UPDATED in place
    (no DELETE) when a shipment line fills in tracking.

    Re-syncs UPSERT so changed order_status / quantities update existing rows.
    """
    print(f"[sync-removals] START task_id={task_id} org={organization_id} upload_id={upload_id}")
    log.info(
        "[sync-removals] START task_id=%s org=%s upload_id=%s",
        task_id, organization_id, upload_id,
    )
    _update_task(task_id, 5, "Task is alive. Connecting to database...")

    _url = os.getenv("SUPABASE_URL")
    _key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not _url or not _key:
        log.error("[sync-removals] ERROR: Missing Supabase credentials")
        _update_task(task_id, 0, "Missing Supabase credentials in environment.", status="failed")
        return

    try:
        db = create_client(_url, _key)
        print("[sync-removals] Supabase client created successfully")
    except Exception as conn_err:
        log.error("[sync-removals] CONNECTION ERROR: %s", conn_err)
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    store_id = _resolve_import_store_id_from_upload(db, organization_id, upload_id)
    if not store_id:
        log.error("[sync-removals] Missing Imports Target Store on raw_report_uploads metadata")
        _update_task(
            task_id,
            0,
            "Imports Target Store missing — set import_store_id or ledger_store_id on the upload metadata.",
            status="failed",
        )
        return
    log.info("[sync-removals] Using store_id=%s", store_id)

    enriched_count = 0
    unmatched_shipment_rows: list[dict[str, Any]] = []
    no_tracking_rows: list[dict[str, Any]] = []

    try:
        # ── Phase 1: Fetch staging rows ───────────────────────────────────────────
        _update_task(task_id, 10, "Fetching rows from amazon_staging...")
        query = db.table("amazon_staging").select("*").eq("organization_id", organization_id)
        if upload_id:
            query = query.eq("upload_id", upload_id)
        staging_rows: list[dict[str, Any]] = (query.execute().data or [])
        log.info("[sync-removals] Fetched %d rows from amazon_staging", len(staging_rows))

        if not staging_rows:
            _update_task(task_id, 100, "No staging rows found. Nothing to sync.", status="completed")
            return

        # ── Phase 2: Extract, keep staging id + raw for shipment archive ──────────
        _update_task(task_id, 18, f"Extracting {len(staging_rows)} staging rows...")

        packed: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        staging_ids: list[str] = []
        skipped = 0
        skipped_no_order_id = 0

        for row in staging_rows:
            raw = row.get("raw_row", {})
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    skipped += 1
                    continue
            if not isinstance(raw, dict):
                skipped += 1
                continue

            row_upload_id: str | None = row.get("upload_id") or upload_id
            extracted = _extract_removal_row(raw, organization_id, row_upload_id)

            if not _pg_text_unique_field(extracted.get("order_id")):
                skipped += 1
                skipped_no_order_id += 1
                continue

            staging_uuid = str(row["id"])
            extracted["source_staging_id"] = staging_uuid
            extracted["store_id"] = store_id

            packed.append((staging_uuid, raw, extracted))
            staging_ids.append(str(row["id"]))

        log.info("[sync-removals] Extracted %d valid rows; skipped %d", len(packed), skipped)
        if skipped_no_order_id > 0:
            log.warning(
                "[sync-removals] SKIPPED %d staging rows with no valid order_id — "
                "these rows remain in amazon_staging and are NOT written to amazon_removals.",
                skipped_no_order_id,
            )

        if not packed:
            _update_task(task_id, 100, "No valid removal rows extracted (missing order_id).", status="completed")
            return

        # ── Phase 2b: Append-only raw history — one row per staging line (aligned with Next.js REMOVAL_SHIPMENT sync).
        shipment_history_rows: list[dict[str, Any]] = []
        for staging_id, raw, ext in packed:
            row_ship: dict[str, Any] = {
                "organization_id": organization_id,
                "upload_id": ext.get("upload_id"),
                "amazon_staging_id": staging_id,
                "store_id": store_id,
                "raw_row": raw,
                "order_id": ext.get("order_id"),
                "sku": ext.get("sku"),
                "fnsku": ext.get("fnsku"),
                "disposition": ext.get("disposition"),
                "tracking_number": ext.get("tracking_number"),
                "carrier": ext.get("carrier"),
                "shipment_date": ext.get("shipment_date"),
                "order_date": ext.get("order_date"),
                "order_type": ext.get("order_type"),
                "requested_quantity": ext.get("requested_quantity"),
                "shipped_quantity": ext.get("shipped_quantity"),
                "disposed_quantity": ext.get("disposed_quantity"),
                "cancelled_quantity": ext.get("cancelled_quantity"),
            }
            shipment_history_rows.append(row_ship)
        if shipment_history_rows:
            _update_task(task_id, 20, f"Archiving {len(shipment_history_rows)} raw shipment rows...")
            for i in range(0, len(shipment_history_rows), STAGING_INSERT_BATCH):
                chunk = shipment_history_rows[i : i + STAGING_INSERT_BATCH]
                db.table("amazon_removal_shipments").upsert(
                    chunk,
                    on_conflict=_REMOVAL_SHIPMENT_CONFLICT,
                ).execute()
            log.info(
                "[sync-removals] Archived %d rows to amazon_removal_shipments",
                len(shipment_history_rows),
            )

        # ── Phase 3: One removal row per staging line (no pre-merge by logical key) ─
        extracted_rows = [p[2] for p in packed]
        _update_task(task_id, 24, f"Prepared {len(extracted_rows)} removal line(s) (one per staging row)...")

        has_tracking_rows: list[dict[str, Any]] = []
        for r in extracted_rows:
            if _pg_text_unique_field(r.get("tracking_number")):
                has_tracking_rows.append(r)
            else:
                no_tracking_rows.append(r)

        log.info(
            "[sync-removals] Split: %d order rows (no tracking), %d rows with tracking",
            len(no_tracking_rows), len(has_tracking_rows),
        )

        upserted_total = 0

        # ── Phase 4: Upsert order rows (tracking IS NULL) ─────────────────────────
        if no_tracking_rows:
            n_order = len(no_tracking_rows)
            _update_task(task_id, 28, f"Upserting {n_order} order rows (no tracking)...")
            for i in range(0, n_order, STAGING_INSERT_BATCH):
                chunk = [_removal_row_for_write(r) for r in no_tracking_rows[i : i + STAGING_INSERT_BATCH]]
                db.table("amazon_removals").upsert(
                    chunk, on_conflict=_REMOVALS_CONFLICT,
                ).execute()
                upserted_total += len(chunk)
                progress = 28 + int((min(i + STAGING_INSERT_BATCH, n_order) / max(n_order, 1)) * 18)
                _update_task(task_id, progress, f"Upserted order rows… {min(i + STAGING_INSERT_BATCH, n_order)}/{n_order}")
            log.info("[sync-removals] Upserted %d order rows into amazon_removals", n_order)

        # ── Phase 5: Shipment rows — UPDATE NULL slots in place; UPSERT the rest ───
        if has_tracking_rows:
            _update_task(
                task_id, 48,
                f"Matching {len(has_tracking_rows)} shipment rows to NULL-tracking removals...",
            )

            try:
                existing_null_rows: list[dict[str, Any]] = (
                    db.table("amazon_removals")
                    .select("*")
                    .eq("organization_id", organization_id)
                    .eq("store_id", store_id)
                    .is_("tracking_number", "null")
                    .execute()
                    .data or []
                )
            except Exception as fetch_err:
                log.warning(
                    "[sync-removals] Could not fetch NULL-tracking rows (%s) — "
                    "all shipment rows will be upserted as new entries.", fetch_err,
                )
                existing_null_rows = []

            log.info(
                "[sync-removals] Found %d NULL-tracking rows for enrichment",
                len(existing_null_rows),
            )

            null_match: dict[tuple[Any, ...], deque] = defaultdict(deque)
            null_rows_by_id: dict[str, dict[str, Any]] = {}

            for db_row in existing_null_rows:
                mk = _removal_null_slot_match_key(db_row)
                rid = str(db_row["id"])
                null_match[mk].append(rid)
                null_rows_by_id[rid] = db_row

            for s_row in has_tracking_rows:
                match_key = _removal_null_slot_match_key(s_row)

                if null_match[match_key]:
                    match_id = null_match[match_key].popleft()
                    existing = null_rows_by_id[match_id]
                    merged = _merge_shipment_into_null_slot(existing, s_row)
                    payload = _removal_row_for_write(merged)
                    db.table("amazon_removals").update(payload).eq("id", match_id).execute()
                    enriched_count += 1
                else:
                    unmatched_shipment_rows.append(s_row)

            log.info(
                "[sync-removals] Shipment handling: %d rows updated in place, %d upserted as new",
                enriched_count, len(unmatched_shipment_rows),
            )

            if unmatched_shipment_rows:
                _update_task(task_id, 68, f"Upserting {len(unmatched_shipment_rows)} new shipment rows...")
                for i in range(0, len(unmatched_shipment_rows), STAGING_INSERT_BATCH):
                    chunk = [
                        _removal_row_for_write(r)
                        for r in unmatched_shipment_rows[i : i + STAGING_INSERT_BATCH]
                    ]
                    db.table("amazon_removals").upsert(
                        chunk, on_conflict=_REMOVALS_CONFLICT,
                    ).execute()
                    upserted_total += len(chunk)

        # ── Phase 6: Row-count verification ───────────────────────────────────────
        # Every extracted staging line must either be upserted as new OR updated in-place.
        staged_line_count = len(extracted_rows)
        accounted_for = upserted_total + enriched_count
        if accounted_for < staged_line_count:
            log.warning(
                "[sync-removals] ROW COUNT MISMATCH: %d staging lines but only %d accounted for "
                "(upserted=%d + enriched_in_place=%d). %d rows may have been silently dropped. "
                "Check for upstream errors in the log above.",
                staged_line_count,
                accounted_for,
                upserted_total,
                enriched_count,
                staged_line_count - accounted_for,
            )
        else:
            log.info(
                "[sync-removals] Row count OK: all %d staging lines accounted for "
                "(upserted=%d, enriched_in_place=%d).",
                staged_line_count,
                upserted_total,
                enriched_count,
            )
        if skipped > 0:
            log.info(
                "[sync-removals] Staging rows skipped (total=%d, no_order_id=%d): "
                "these remain in amazon_staging and are excluded from the counts above.",
                skipped,
                skipped_no_order_id,
            )

        # ── Phase 7: Clean up staging ─────────────────────────────────────────────
        _update_task(task_id, 82, f"Cleaning up {len(staging_ids)} rows from amazon_staging...")
        log.info("[sync-removals] Deleting %d staging rows by id", len(staging_ids))
        for i in range(0, len(staging_ids), STAGING_INSERT_BATCH):
            db.table("amazon_staging").delete().in_(
                "id", staging_ids[i : i + STAGING_INSERT_BATCH]
            ).execute()

        log.info(
            "[sync-removals] wave1_reconciliation %s",
            json.dumps(
                {
                    "store_id": store_id,
                    "staging_lines": staged_line_count,
                    "upsert_chunks_rows": upserted_total,
                    "enriched_in_place": enriched_count,
                    "skipped_staging": skipped,
                },
                default=str,
            ),
        )
        log.info(
            "[sync-removals] DONE — upserted=%d updated_in_place=%d staging_skipped=%d",
            upserted_total,
            enriched_count,
            skipped,
        )
        _update_task(
            task_id, 100,
            (
                f"Sync complete: {upserted_total} upserts + {enriched_count} in-place enrichments "
                f"into amazon_removals ({len(shipment_history_rows)} raw shipment rows archived). "
                "Run Generate Worklist to update expected_packages."
            ),
            status="completed",
        )

    except Exception as e:
        log.error("[sync-removals] FATAL ERROR:\n%s", traceback.format_exc())
        _update_task(task_id, 0, f"Sync failed: {e!s}", status="failed")


# --- Helpers for worklist generation ---


def _generate_worklist_core(
    db: Any,
    task_id: str,
    organization_id: str,
    *,
    progress_start: int = 0,
    upload_id: str | None = None,
) -> None:
    """
    Phase 4: amazon_removals → expected_packages (warehouse worklist).

    Only removal rows whose order_type is Return are written to expected_packages
    (Disposal / Liquidations stay in amazon_removals only).

    Rows are written with INSERT ... ON CONFLICT ... DO UPDATE (via Supabase upsert).

    No wholesale DELETE: warehouse fields (e.g. actual_scanned_count) are merged
    from existing expected_packages and never overwritten by Amazon-only columns.

    When upload_id is set (Phase 4 from the importer), input rows are scoped to that
    session: prefer amazon_removals with matching upload_id; if none (REMOVAL_SHIPMENT
    only wrote amazon_removal_shipments), build from that archive. expected_packages
    rows are stamped with this upload_id.

    progress_start: baseline when called from POST /etl/generate-worklist.
    """

    def _prog(fraction: float, msg: str) -> None:
        actual = min(int(progress_start + fraction * (100 - progress_start)), 99)
        _update_task(task_id, actual, msg)

    try:
        session_upload = str(upload_id).strip() if upload_id else None

        # ── 1. Fetch amazon_removals ──────────────────────────────────────────────
        _prog(0.02, "Fetching amazon_removals rows for worklist generation...")
        log.info("[worklist-core] Fetching amazon_removals for org=%s", organization_id)

        try:
            removal_rows = _fetch_org_table_all(db, "amazon_removals", organization_id)
        except Exception as fetch_err:
            log.exception("[worklist-core] FETCH amazon_removals FAILED")
            _update_task(task_id, 0, f"Worklist fetch failed: {fetch_err!s}", status="failed")
            return

        n_removals_loaded = len(removal_rows)
        log.info("[worklist-core] amazon_removals rows loaded for generation: %d", n_removals_loaded)

        if session_upload:
            scoped = [r for r in removal_rows if str(r.get("upload_id") or "").strip() == session_upload]
            if scoped:
                removal_rows = scoped
                log.info(
                    "[worklist-core] scoped to upload_id=%s: %d amazon_removals row(s)",
                    session_upload,
                    len(removal_rows),
                )
            else:
                ship_rows = _fetch_amazon_removal_shipments_by_upload(db, organization_id, session_upload)
                removal_rows = [_removal_like_from_shipment_archive(x) for x in ship_rows]
                log.info(
                    "[worklist-core] no amazon_removals for upload_id=%s: using %d amazon_removal_shipments row(s)",
                    session_upload,
                    len(removal_rows),
                )

        if not removal_rows:
            log.warning("[worklist-core] no input rows for this worklist run — nothing to write")
            _update_task(task_id, 100, "No removal or shipment rows for this upload; worklist unchanged.", status="completed")
            return

        # ── 2. Load existing worklist (preserve warehouse columns) ───────────────
        _prog(0.08, "Loading existing expected_packages for merge...")
        try:
            existing_ep = _fetch_org_table_all(db, "expected_packages", organization_id)
        except Exception as ep_err:
            log.exception("[worklist-core] FETCH expected_packages FAILED")
            _update_task(task_id, 0, f"Worklist existing fetch failed: {ep_err!s}", status="failed")
            return

        existing_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
        for er in existing_ep:
            existing_by_key[_expected_pkg_identity_key(er)] = er

        # ── 3. Map removals → Amazon column set; merge with existing worklist rows ─
        _prog(0.12, f"Mapping {len(removal_rows)} rows to expected_packages format...")

        merged_rows: list[dict[str, Any]] = []
        skipped_no_order = 0
        skipped_non_return = 0

        for row in removal_rows:
            order_id = _pg_text_unique_field(row.get("order_id"))
            if not order_id:
                skipped_no_order += 1
                continue
            if not _is_return_order_type_for_worklist(row):
                skipped_non_return += 1
                continue

            incoming: dict[str, Any] = {"organization_id": organization_id, "order_id": order_id}
            for k in EXPECTED_PKG_AMAZON_COLS:
                if k in ("organization_id", "order_id"):
                    continue
                if k not in row:
                    continue
                val = row[k]
                if k in _EP_WORKLIST_AMAZON_FILL_NULL and _ep_value_absent(val):
                    continue
                incoming[k] = val

            if not incoming.get("sku"):
                incoming["sku"] = ""
            if not incoming.get("order_status"):
                incoming["order_status"] = "Pending"

            if session_upload:
                incoming["upload_id"] = session_upload

            k = _expected_pkg_identity_key(incoming)
            if k in existing_by_key:
                merged_rows.append(_merge_expected_pkg_amazon_only(existing_by_key[k], incoming))
            else:
                merged_rows.append(incoming)

        log.info(
            "[worklist-core] Prepared %d upsert rows (skipped %d no order_id, %d non-Return order_type)",
            len(merged_rows), skipped_no_order, skipped_non_return,
        )

        if skipped_no_order > 0:
            log.warning(
                "[worklist-core] SKIPPED %d removal rows with no order_id — "
                "these will NOT appear in expected_packages.",
                skipped_no_order,
            )
        if skipped_non_return > 0:
            log.info(
                "[worklist-core] SKIPPED %d removal rows (order_type is not Return) — "
                "expected_packages only includes Return lines.",
                skipped_non_return,
            )

        if not merged_rows:
            log.info(
                "[worklist-core] rows skipped before insert: no_order=%d non_return=%d — no merged rows",
                skipped_no_order,
                skipped_non_return,
            )
            _update_task(task_id, 100, "No worklist rows produced from amazon_removals.", status="completed")
            return

        log.info(
            "[worklist-core] rows skipped before insert: no_order=%d non_return=%d",
            skipped_no_order,
            skipped_non_return,
        )

        merged_before_dedupe = len(merged_rows)
        merged_rows, merged_collapsed = _dedupe_merged_rows_for_expected_packages_upsert(merged_rows)
        merged_after_dedupe = len(merged_rows)
        log.info("[worklist-core] merged_rows_before_dedupe: %d", merged_before_dedupe)
        log.info("[worklist-core] merged_rows_after_dedupe: %d", merged_after_dedupe)
        log.info("[worklist-core] merged_rows_collapsed_by_conflict_key: %d", merged_collapsed)

        # ── 4. UPSERT (no DELETE) ─────────────────────────────────────────────────
        # Strip Postgres-managed system columns before upserting.  Sending id=None
        # (when an existing row was merged then cleared of its PK) triggers the
        # "null value in column 'id' violates not-null constraint" error.
        _prog(0.28, f"Upserting {len(merged_rows)} rows into expected_packages...")

        upserted = 0
        n_pkg = len(merged_rows)
        for i in range(0, n_pkg, STAGING_INSERT_BATCH):
            # Strip system columns so Postgres manages id/created_at/updated_at itself.
            chunk = [
                {k: v for k, v in r.items() if k not in _DB_SYSTEM_COLS}
                for r in merged_rows[i : i + STAGING_INSERT_BATCH]
            ]
            try:
                db.table("expected_packages").upsert(
                    chunk, on_conflict=_EXPECTED_PKG_CONFLICT,
                ).execute()
                upserted += len(chunk)
                frac = 0.28 + (upserted / max(n_pkg, 1)) * 0.70
                _prog(frac, f"Upserted {upserted}/{n_pkg} expected_packages rows...")
            except Exception as ins_err:
                log.exception("[worklist-core] UPSERT chunk at offset %d FAILED", i)
                _update_task(
                    task_id, 0,
                    f"expected_packages upsert failed at offset {i}: {ins_err!s}",
                    status="failed",
                )
                return

        ep_rows_generated = len(merged_rows)
        ep_rows_populated_shipment_derived = sum(
            1
            for r in merged_rows
            if any(not _ep_value_absent(r.get(fk)) for fk in _EP_WORKLIST_AMAZON_FILL_NULL)
        )
        ep_rows_missing_tracking_carrier_or_shipment_date = sum(
            1
            for r in merged_rows
            if _ep_value_absent(r.get("tracking_number"))
            or _ep_value_absent(r.get("carrier"))
            or _ep_value_absent(r.get("shipment_date"))
        )

        log.info(
            "[worklist-core] REMOVAL_PIPELINE %s",
            json.dumps(
                {
                    "checkpoint": "REMOVAL_PIPELINE",
                    "stage": "C_expected_packages",
                    "organization_id": organization_id,
                    "expected_rows_generated": ep_rows_generated,
                    "expected_rows_populated_shipment_derived": ep_rows_populated_shipment_derived,
                    "expected_rows_missing_tracking_carrier_or_shipment_date": (
                        ep_rows_missing_tracking_carrier_or_shipment_date
                    ),
                    "expected_packages_upserted": upserted,
                    "skipped_no_order": skipped_no_order,
                    "skipped_non_return": skipped_non_return,
                },
                default=str,
            ),
        )

        log.info("[worklist-core] expected_packages rows inserted: %d", upserted)

        # ── Row-count verification ─────────────────────────────────────────────
        if upserted < len(merged_rows):
            log.warning(
                "[worklist-core] ROW COUNT MISMATCH: prepared %d rows but only %d were upserted.",
                len(merged_rows), upserted,
            )
        explained = skipped_no_order + skipped_non_return + len(merged_rows)
        if explained != len(removal_rows):
            log.warning(
                "[worklist-core] ROW COUNT WARNING: input_rows=%d but "
                "skipped_no_order=%d + skipped_non_return=%d + worklist=%d = %d.",
                len(removal_rows),
                skipped_no_order,
                skipped_non_return,
                len(merged_rows),
                explained,
            )
        else:
            log.info(
                "[worklist-core] Row count OK: %d input = %d skipped (no order) + "
                "%d skipped (non-Return) + %d worklist row(s).",
                len(removal_rows), skipped_no_order, skipped_non_return, len(merged_rows),
            )

        log.info(
            "[worklist-core] DONE — upserted %d rows (input_rows=%d, skipped_no_order=%d, skipped_non_return=%d)",
            upserted, len(removal_rows), skipped_no_order, skipped_non_return,
        )
        log.info(
            "[worklist-core] wave1_reconciliation %s",
            json.dumps(
                {
                    "phase": "generate_worklist",
                    "upload_id": session_upload,
                    "input_rows": len(removal_rows),
                    "expected_packages_upserted": upserted,
                    "expected_packages_rows_generated": ep_rows_generated,
                    "expected_packages_rows_populated_shipment_derived": ep_rows_populated_shipment_derived,
                    "expected_packages_rows_missing_tracking_carrier_or_shipment_date": (
                        ep_rows_missing_tracking_carrier_or_shipment_date
                    ),
                    "skipped_no_order": skipped_no_order,
                    "skipped_non_return": skipped_non_return,
                },
                default=str,
            ),
        )
        _update_task(
            task_id, 100,
            f"Worklist complete: {upserted} rows upserted into expected_packages "
            f"(Amazon fields updated; warehouse scan progress preserved).",
            status="completed",
        )

    except Exception as e:
        log.exception("[worklist-core] Phase 4 (generate worklist) failed")
        _update_task(task_id, 0, f"Worklist failed: {e!s}", status="failed")


# --- Background task: amazon_removals -> expected_packages (standalone trigger) ---
# Must be a plain def (not async) so FastAPI runs it in a thread pool.
# Triggered only via POST /etl/generate-worklist (Phase 4 in the Next.js importer).

def _run_generate_worklist(
    task_id: str,
    organization_id: str,
    upload_id: str | None,  # kept for API compatibility; ignored inside core
) -> None:
    print(f"[generate-worklist] START task_id={task_id} org={organization_id}")
    _update_task(task_id, 5, "Task is alive. Connecting to database...")

    _url = os.getenv("SUPABASE_URL")
    _key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not _url or not _key:
        _update_task(task_id, 0, "Missing Supabase credentials in environment.", status="failed")
        return

    try:
        db = create_client(_url, _key)
        print("[generate-worklist] Supabase client created successfully")
    except Exception as conn_err:
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    try:
        _generate_worklist_core(db, task_id, organization_id, progress_start=0, upload_id=upload_id)
    except Exception:
        log.exception("[generate-worklist] background task crashed")
        _update_task(task_id, 0, "Worklist task crashed — see server logs.", status="failed")


# --- ETL Endpoints: Removals Pipeline ---

@app.post("/etl/sync-removals")
async def etl_sync_removals(
    request: SyncRemovalsRequest,
    background_tasks: BackgroundTasks,
):
    """
    Move rows from amazon_staging into amazon_removals (archive).
    Returns a task_id immediately; poll GET /etl/task/{task_id} for progress.
    """
    try:
        uuid.UUID(request.organization_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="organization_id must be a valid UUID.")

    if request.upload_id:
        try:
            uuid.UUID(request.upload_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="upload_id must be a valid UUID.")

    task_id = str(uuid.uuid4())
    _update_task(task_id, 0, "Task queued.", status="queued")
    background_tasks.add_task(_run_sync_removals, task_id, request.organization_id, request.upload_id)
    return {"task_id": task_id, "status": "queued", "poll_url": f"/etl/task/{task_id}"}


@app.post("/etl/generate-worklist")
async def etl_generate_worklist(
    request: GenerateWorklistRequest,
    background_tasks: BackgroundTasks,
):
    """
    Transform amazon_removals rows into expected_packages (operational worklist).
    Uses UPSERT on canonical cross-file key (organization_id, store_id, order_id, order_type, sku, fnsku, disposition);
    quantities/dates are row attributes; warehouse scan fields are preserved on conflict.
    Returns a task_id immediately; poll GET /etl/task/{task_id} for progress.
    """
    try:
        uuid.UUID(request.organization_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="organization_id must be a valid UUID.")

    if request.upload_id:
        try:
            uuid.UUID(request.upload_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="upload_id must be a valid UUID.")

    task_id = str(uuid.uuid4())
    _update_task(task_id, 0, "Task queued.", status="queued")
    background_tasks.add_task(_run_generate_worklist, task_id, request.organization_id, request.upload_id)
    return {"task_id": task_id, "status": "queued", "poll_url": f"/etl/task/{task_id}"}


@app.get("/etl/task/{task_id}")
async def get_task_status(task_id: str):
    """
    Poll the progress of a background ETL task.
    Returns: task_id, status (queued|running|completed|failed), progress (0-100), message.
    """
    if task_id not in task_store:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
    return task_store[task_id]
