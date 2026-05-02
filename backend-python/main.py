import csv
import io
import json
import logging
import math
import traceback
import uuid
import hashlib
import os
from collections import defaultdict, deque
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import Client, create_client

# Load environment variables securely from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("etl")

# Initialize FastAPI App
app = FastAPI(title="Logistics AI Agent API", version="1.0")

# --- CORS Middleware ---
# NOTE: allow_credentials=True + allow_origins=["*"] is invalid per the spec and
# makes browsers reject preflight. JWT cookies are not used on these ETL routes.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Background Task Progress Store ---
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

def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None

def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None

def _safe_bool(val: Any) -> bool | None:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"true", "yes", "y", "1"}:
        return True
    if s in {"false", "no", "n", "0"}:
        return False
    return None

def _safe_timestamp(val: Any) -> str | None:
    if val is None:
        return None
    try:
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.isoformat()
    except Exception:
        return None

def _safe_date(val: Any) -> str | None:
    ts = _safe_timestamp(val)
    if not ts:
        return None
    return ts[:10]

def _read_tabular_file(content: bytes) -> pd.DataFrame:
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


def _sniff_csv_delimiter(first_line: str) -> str:
    if "\t" in first_line and first_line.count("\t") >= max(1, first_line.count(",")):
        return "\t"
    return ","


def _csv_headers_and_row_count_fast(content: bytes) -> tuple[list[str], int]:
    """
    One streaming pass over bytes — header cells + data row count.
    Used by upload-raw only; avoids pandas (huge RAM + double parse) on large files.
    """
    if not content:
        return [], 0
    sample = content[: min(len(content), 262144)].decode("utf-8-sig", errors="replace")
    lines = sample.splitlines()
    if not lines:
        return [], 0
    delim = _sniff_csv_delimiter(lines[0])
    bio = io.BytesIO(content)
    text_io = io.TextIOWrapper(bio, encoding="utf-8-sig", errors="replace", newline="")
    try:
        reader = csv.reader(text_io, delimiter=delim)
        rows_iter = iter(reader)
        header_row = next(rows_iter, [])
        headers = [str(h).strip() for h in header_row if str(h).strip() != ""]
        if not headers:
            return [], 0
        row_count = sum(1 for _ in rows_iter)
        return headers, row_count
    finally:
        text_io.close()


def _get_from_row(row: dict[str, Any], *candidates: str) -> str | None:
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

    if not _pg_text_unique_field(result.get("sku")):
        fn = _get_from_row(raw, "fnsku", "asin")
        if fn:
            result["sku"] = str(fn).strip()

    for nullable in ("sku", "fnsku", "disposition", "tracking_number"):
        t = _pg_text_unique_field(result.get(nullable))
        result[nullable] = t

    return result

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

_DB_SYSTEM_COLS = frozenset({"id", "created_at", "updated_at"})

def _removal_order_date_key(val: Any) -> str | None:
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

_EP_WORKLIST_AMAZON_FILL_NULL = frozenset({
    "tracking_number",
    "carrier",
    "shipment_date",
    "order_date",
    "fnsku",
    "store_id",
    "order_type",
})

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
    t = _pg_text_unique_field(row.get("order_type"))
    if not t:
        return True
    return t.strip().lower() == "return"

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

@app.get("/")
def read_root():
    return {"status": "Agent Backend is Live!", "service": "AI Logistics"}

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

STAGING_INSERT_BATCH = 500
WORKLIST_FETCH_PAGE = 1000

def _fetch_org_table_all(
    db: Any,
    table: str,
    organization_id: str,
) -> list[dict[str, Any]]:
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

def _run_sync_removals(
    task_id: str,
    organization_id: str,
    upload_id: str | None,
) -> None:
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
        _update_task(task_id, 10, "Fetching rows from amazon_staging...")
        query = db.table("amazon_staging").select("*").eq("organization_id", organization_id)
        if upload_id:
            query = query.eq("upload_id", upload_id)
        staging_rows: list[dict[str, Any]] = (query.execute().data or [])
        log.info("[sync-removals] Fetched %d rows from amazon_staging", len(staging_rows))

        if not staging_rows:
            _update_task(task_id, 100, "No staging rows found. Nothing to sync.", status="completed")
            return

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

def _generate_worklist_core(
    db: Any,
    task_id: str,
    organization_id: str,
    *,
    progress_start: int = 0,
    upload_id: str | None = None,
) -> None:

    def _prog(fraction: float, msg: str) -> None:
        actual = min(int(progress_start + fraction * (100 - progress_start)), 99)
        _update_task(task_id, actual, msg)

    try:
        session_upload = str(upload_id).strip() if upload_id else None

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

        merged_before_dedupe = len(merged_rows)
        merged_rows, merged_collapsed = _dedupe_merged_rows_for_expected_packages_upsert(merged_rows)
        merged_after_dedupe = len(merged_rows)
        log.info("[worklist-core] merged_rows_before_dedupe: %d", merged_before_dedupe)
        log.info("[worklist-core] merged_rows_after_dedupe: %d", merged_after_dedupe)
        log.info("[worklist-core] merged_rows_collapsed_by_conflict_key: %d", merged_collapsed)

        _prog(0.28, f"Upserting {len(merged_rows)} rows into expected_packages...")

        upserted = 0
        n_pkg = len(merged_rows)
        for i in range(0, n_pkg, STAGING_INSERT_BATCH):
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

def _run_generate_worklist(
    task_id: str,
    organization_id: str,
    upload_id: str | None,
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

@app.post("/etl/sync-removals")
async def etl_sync_removals(
    request: SyncRemovalsRequest,
    background_tasks: BackgroundTasks,
):
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
    if task_id not in task_store:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
    return task_store[task_id]

# -------------------------------------------------------------------------
# UI ARCHITECTURE ENDPOINTS & DATA ROUTING
# -------------------------------------------------------------------------

def _get_openai_key(db: Any, org_id: str) -> str | None:
    """Attempts to retrieve the OpenAI key from the environment first, then the database."""
    env_key = os.getenv("OPENAI_API_KEY")
    if env_key and str(env_key).startswith("sk-"):
        return env_key

    try:
        res = db.table("organization_api_keys").select("*").eq("organization_id", org_id).execute()
        if res.data:
            for row in res.data:
                for key_field in ["api_key", "key", "token", "openai_api_key", "key_value"]:
                    if key_field in row and row[key_field]:
                        val = str(row[key_field])
                        if val.startswith("sk-"):
                            return val
    except Exception as e:
        log.warning(f"Failed to fetch API key from DB: {e}")
    
    return None

def _normalize_to_ui_report_slug(raw: str | None) -> str:
    """
    Maps GPT / user / legacy tokens to a small set of UI routing slugs.
    DB CHECK constraints use different spellings — see _ui_slug_to_db_report_type().
    """
    if not raw:
        return "unknown"
    s = str(raw).strip().lower()
    for junk in ('"', "'", "`", ".", ",", ";", ":"):
        s = s.replace(junk, "")
    s = s.split()[0] if s else ""

    synonyms: dict[str, str] = {
        "inventory_ledger": "inventory_ledger",
        "inventory": "inventory_ledger",
        "fba_inventory_ledger": "inventory_ledger",
        "ledger": "inventory_ledger",
        "inventory-ledger": "inventory_ledger",
        "reimbursements": "reimbursements",
        "reimbursement": "reimbursements",
        "fba_reimbursements": "reimbursements",
        "removals": "removals",
        "removal": "removals",
        "removal_shipment": "removals",
        "removal_shipments": "removals",
        "removal_shipment_detail": "removals",
        "removal_order": "removals",
        "removal_orders": "removals",
        "removal-order-id": "removals",
        "amazon_all_orders": "amazon_all_orders",
        "all_orders": "amazon_all_orders",
        # GPT might echo Postgres-safe labels — fold back to UI slug for routing
        "removal_shipment": "removals",
        "unknown": "unknown",
    }
    return synonyms.get(s, "unknown")

def _normalized_header_set(headers: list[str]) -> set[str]:
    return {_normalize_header(str(h)) for h in headers if str(h).strip()}

def _detect_report_type_by_rules(headers: list[str]) -> tuple[str, float, str]:
    """
    Deterministic first pass. This avoids unnecessary GPT calls for the common
    Amazon exports and makes the UI feel instant/reliable.
    """
    h = _normalized_header_set(headers)
    if not h:
        return "unknown", 0.0, "rules"

    if {"date", "fnsku", "asin", "msku", "event_type", "quantity"}.issubset(h):
        return "inventory_ledger", 0.98, "rules"
    if {"date_and_time", "fnsku", "asin", "msku", "event_type", "quantity"}.issubset(h):
        return "inventory_ledger", 0.98, "rules"
    if {"reimbursement_id", "reason", "sku"}.issubset(h) or {"approval_date", "reimbursement_id"}.issubset(h):
        return "reimbursements", 0.98, "rules"
    if {"order_id", "sku", "fnsku", "disposition"}.issubset(h) and (
        "requested_quantity" in h or "shipped_quantity" in h or "tracking_number" in h
    ):
        return "removals", 0.96, "rules"
    if {"amazon_order_id", "purchase_date", "order_status"}.issubset(h):
        return "amazon_all_orders", 0.98, "rules"
    if {"order_id", "purchase_date", "order_status"}.issubset(h):
        return "amazon_all_orders", 0.9, "rules"

    return "unknown", 0.0, "rules"


def _ui_slug_to_db_report_type(ui_slug: str) -> str:
    """Maps UI slug to a value that satisfies raw_report_uploads.report_type CHECK."""
    mapping = {
        "inventory_ledger": "inventory_ledger",
        "reimbursements": "reimbursements",
        "removals": "REMOVAL_SHIPMENT",
        "amazon_all_orders": "ALL_ORDERS",
        "unknown": "UNKNOWN",
    }
    return mapping.get(ui_slug, "UNKNOWN")


ALLOWED_UI_REPORT_SLUGS = frozenset(
    {"inventory_ledger", "reimbursements", "removals", "amazon_all_orders"}
)


def _detect_report_type_with_gpt(headers: list[str], api_key: str) -> tuple[str, float, str]:
    """Uses GPT to classify the report; returns a normalized UI slug."""
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        header_sample = ", ".join(headers[:80])
        prompt = (
            "You classify Amazon Seller Central export CSVs using COLUMN HEADERS ONLY.\n\n"
            "Reply with exactly ONE lowercase slug from this list (no punctuation, no explanation):\n"
            "- inventory_ledger — FBA Inventory Ledger / inventory events (SKU, fulfillment center, quantities, event types)\n"
            "- reimbursements — FBA reimbursements / repayment (reimbursement id, fee/reason, currency amounts)\n"
            "- removals — Removal orders / removal shipments / disposal (removal-order-id, disposition, shipped quantity)\n"
            "- amazon_all_orders — All Orders style report (amazon-order-id, order status, purchase date)\n"
            "- unknown — if none of the above fit\n\n"
            f"Headers:\n{header_sample}"
        )
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=24,
        )
        raw_out = response.choices[0].message.content
        detected = _normalize_to_ui_report_slug(raw_out)
        return detected, 0.8 if detected != "unknown" else 0.0, "gpt"
    except Exception as e:
        log.warning(f"GPT detection failed: {e}")
        return "unknown", 0.0, "gpt"

class DetectHeadersRequest(BaseModel):
    headers: list[str]
    organization_id: str

@app.post("/etl/detect-headers")
async def etl_detect_headers(request: DetectHeadersRequest):
    """Architectural Route for Client-Side Slicing auto-detection."""
    db = _require_supabase()
    rules_type, confidence, method = _detect_report_type_by_rules(request.headers)
    if rules_type != "unknown":
        return {
            "detected_type": rules_type,
            "confidence": confidence,
            "method": method,
        }

    api_key = _get_openai_key(db, request.organization_id)
    if not api_key:
        return {"detected_type": "unknown", "confidence": 0.0, "method": "none"}

    detected, confidence, method = _detect_report_type_with_gpt(request.headers, api_key)
    return {
        "detected_type": detected,
        "confidence": confidence,
        "method": method,
    }

@app.get("/etl/upload-history/{organization_id}")
async def etl_upload_history(organization_id: str):
    """Fetches the upload history for the frontend table + per-upload pipeline progress."""
    db = _require_supabase()
    try:
        uuid.UUID(organization_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID.")

    try:
        res = (
            db.table("raw_report_uploads")
            .select("*")
            .eq("organization_id", organization_id)
            .order("created_at", desc=True)
            .limit(50)
            .execute()
        )
        rows = list(res.data or [])
        ids = [r.get("id") for r in rows if r.get("id")]
        fps_by_upload: dict[str, dict[str, Any]] = {}
        if ids:
            try:
                fps_res = (
                    db.table("file_processing_status")
                    .select("*")
                    .in_("upload_id", ids)
                    .execute()
                )
                for fps in fps_res.data or []:
                    uid = fps.get("upload_id")
                    if isinstance(uid, str):
                        fps_by_upload[uid] = fps
            except Exception as fe:
                log.warning(f"file_processing_status join skipped: {fe}")
        for r in rows:
            uid = r.get("id")
            if isinstance(uid, str) and uid in fps_by_upload:
                r["pipeline"] = fps_by_upload[uid]
        return {"status": "success", "history": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database fetch error: {e}")

@app.delete("/etl/upload/{upload_id}")
async def etl_delete_upload(upload_id: str):
    """Deletes an upload record and its physical file from storage."""
    db = _require_supabase()
    try:
        res = db.table("raw_report_uploads").select("metadata").eq("id", upload_id).execute()
        if res.data and res.data[0].get("metadata"):
            storage_path = res.data[0]["metadata"].get("storage_path")
            if storage_path:
                try:
                    db.storage.from_("raw-reports").remove([storage_path])
                except Exception as e:
                    log.warning(f"Failed to delete physical file: {e}")
                    
        db.table("raw_report_uploads").delete().eq("id", upload_id).execute()
        return {"status": "success", "message": "Record deleted."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {e}")

@app.post("/etl/upload-raw")
async def etl_upload_raw(
    file: UploadFile = File(...),
    report_type: str = Form(...),
    organization_id: str = Form(...),
    store_id: str = Form(...)
):
    db = _require_supabase()
    
    try:
        uuid.UUID(organization_id)
        uuid.UUID(store_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format for organization_id or store_id.")

    raw_bytes = await file.read()
    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Empty file.")
    file_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Idempotency Check. Only completed/synced uploads are considered duplicates.
    # Previous failed or stuck "processing" rows must not block a retry because
    # they may have been created before the raw file reached storage.
    try:
        existing_res = (
            db.table("raw_report_uploads")
            .select("id, report_type, status, metadata")
            .eq("organization_id", organization_id)
            .execute()
        )
        for row in existing_res.data:
            meta = row.get("metadata") or {}
            status = str(row.get("status") or "").lower()
            reusable_status = status in {"complete", "synced", "mapped", "ready", "uploaded"}
            if meta.get("file_hash") == file_hash and reusable_status:
                return {
                    "status": "success",
                    "message": "Duplicate file prevented. File already exists in the system.",
                    "upload_id": row["id"],
                    "detected_type": (row.get("metadata") or {}).get("ui_report_slug") or row.get("report_type"),
                }
    except Exception as e:
        log.warning(f"Duplicate check bypassed: {e}")

    headers, row_count = _csv_headers_and_row_count_fast(raw_bytes)
    if not headers:
        raise HTTPException(
            status_code=400,
            detail="Could not read a header row. Ensure the file is UTF-8 CSV or TSV.",
        )
    if row_count < 1:
        raise HTTPException(status_code=400, detail="No data rows in file.")

    raw_rt = (report_type or "").strip()
    ui_slug = _normalize_to_ui_report_slug(raw_rt)

    if ui_slug == "unknown" or raw_rt.lower() in (
        "auto",
        "auto-detect",
        "",
        "null",
    ):
        api_key = _get_openai_key(db, organization_id)
        if api_key:
            ui_slug, _, _ = _detect_report_type_with_gpt(headers, api_key)
        else:
            ui_slug = "unknown"

    if ui_slug not in ALLOWED_UI_REPORT_SLUGS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Report type could not be determined. "
                "Pick inventory_ledger, reimbursements, removals, or amazon_all_orders from the dropdown."
            ),
        )

    db_report_type = _ui_slug_to_db_report_type(ui_slug)
    target_table_by_slug = {
        "inventory_ledger": "amazon_inventory_ledger",
        "reimbursements": "amazon_reimbursements",
        "removals": "amazon_staging",
        "amazon_all_orders": "amazon_all_orders",
    }
    target_table = target_table_by_slug.get(ui_slug)

    safe_filename = (file.filename or "upload.csv").replace(" ", "_")
    storage_path = f"{organization_id}/{store_id}/{file_hash}_{safe_filename}"

    try:
        db.storage.from_("raw-reports").upload(
            path=storage_path,
            file=raw_bytes,
            file_options={
                "content-type": file.content_type or "text/csv",
                "upsert": "true",
            },
        )
    except Exception as e:
        log.exception(f"Storage upload failed for {storage_path}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Could not save file to storage. Check bucket 'raw-reports' and service role permissions. ({e})",
        ) from e

    upload_id: str | None = None
    try:
        upload_record = {
            "organization_id": organization_id,
            "report_type": db_report_type,
            "file_name": file.filename or "upload.csv",
            "metadata": {
                "file_hash": file_hash,
                "content_sha256": file_hash,
                "row_count": row_count,
                "total_rows": row_count,
                "import_store_id": store_id,
                "ledger_store_id": store_id,
                "storage_path": storage_path,
                "raw_file_path": storage_path,
                "upload_chunks_count": 1,
                "total_parts": 1,
                "upload_progress": 100,
                "total_bytes": len(raw_bytes),
                "uploaded_bytes": len(raw_bytes),
                "csv_headers": headers,
                "headers": headers[:15],
                "ui_report_slug": ui_slug,
                "target_table": target_table,
                "etl_source": "amazon_etl_quick_upload",
            },
            "status": "mapped",
        }

        upload_res = db.table("raw_report_uploads").insert(upload_record).execute()
        if not upload_res.data:
            raise RuntimeError("Insert returned no row — check raw_report_uploads schema and RLS.")
        upload_id = str(upload_res.data[0]["id"])

        return {
            "status": "success",
            "message": (
                f"File saved to storage ({row_count:,} data rows). "
                "Open Imports → find this upload → Process (staging), then Sync (Amazon tables)."
            ),
            "upload_id": upload_id,
            "detected_type": ui_slug,
            "report_type_db": db_report_type,
            "next_step": "imports_process",
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error finalizing upload: {traceback.format_exc()}")
        if upload_id:
            try:
                db.table("raw_report_uploads").update({"status": "failed"}).eq("id", upload_id).execute()
            except Exception as ue:
                log.warning(f"Could not mark upload failed: {ue}")
        raise HTTPException(status_code=500, detail=f"System failed to finalize the upload: {e}") from e