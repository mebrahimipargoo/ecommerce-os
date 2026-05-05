import csv
import io
import json
import logging
import math
import time
import traceback
import uuid
import hashlib
import os
import re
from collections import defaultdict, deque
from typing import Any, Callable, Iterator, List

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


# Align with Next.js Phase 2 staging: transient PostgREST / network hiccups (e.g. Nano).
_IMPORT_RETRY_BACKOFF_SEC = (5.0, 15.0, 30.0, 60.0, 120.0)
_IMPORT_OP_MAX_ATTEMPTS = 6


def _is_transient_supabase_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "timeout",
        "timed out",
        "statement timeout",
        "deadlock",
        "too many connections",
        "connection reset",
        "econnreset",
        "etimedout",
        "eai_again",
        "fetch failed",
        "network",
        "socket",
        "502",
        "503",
        "504",
        "bad gateway",
        "gateway timeout",
        "service unavailable",
        "internal server error",
        "premature",
        "cloudflare",
    )
    return any(n in msg for n in needles)


def _execute_with_import_retries(label: str, fn: Callable[[], Any], max_attempts: int = _IMPORT_OP_MAX_ATTEMPTS) -> Any:
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if not _is_transient_supabase_error(e) or attempt >= max_attempts:
                raise
            idx = min(attempt - 1, len(_IMPORT_RETRY_BACKOFF_SEC) - 1)
            delay = _IMPORT_RETRY_BACKOFF_SEC[idx]
            log.warning(
                "[etl] %s attempt %d/%d failed (%s); sleeping %.1fs before retry",
                label,
                attempt,
                max_attempts,
                e,
                delay,
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label}: retries exhausted with no exception (should not happen)")


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


def _looks_like_xlsx_bytes(content: bytes, filename: str | None) -> bool:
    fn = (filename or "").strip().lower()
    if fn.endswith((".xlsx", ".xlsm")):
        return True
    return len(content) >= 4 and content[:4] == b"PK\x03\x04"


def _iter_seed_product_frames(content: bytes, filename: str | None) -> Iterator[tuple[str | None, pd.DataFrame]]:
    """
    Yield (sheet_name_or_none, dataframe) for seed-products.
    CSV/TSV: one frame with sheet None. Excel: one frame per non-empty sheet (header row required).
    """
    if not _looks_like_xlsx_bytes(content, filename):
        yield None, _read_tabular_file(content)
        return
    try:
        import openpyxl  # noqa: F401 — pandas read_excel(engine="openpyxl") needs this package at runtime
    except ModuleNotFoundError as e:
        raise HTTPException(
            status_code=503,
            detail=(
                "Excel (.xlsx/.xlsm) requires the 'openpyxl' package on the ETL server. "
                "Install: pip install openpyxl   "
                "or from repo root: pip install -r backend-python/requirements.txt"
            ),
        ) from e
    bio = io.BytesIO(content)
    try:
        xl = pd.ExcelFile(bio, engine="openpyxl")
    except ImportError as e:
        raise HTTPException(
            status_code=503,
            detail=(
                "Could not load Excel engine. Install openpyxl: pip install openpyxl "
                "(see backend-python/requirements.txt)."
            ),
        ) from e
    for sheet in xl.sheet_names:
        df = pd.read_excel(
            xl,
            sheet_name=sheet,
            header=0,
            dtype=object,
            keep_default_na=False,
        )
        headers = [str(c).strip() for c in df.columns if str(c).strip() != ""]
        if df.shape[0] == 0 or not headers:
            continue
        yield sheet, df


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
            _execute_with_import_retries(
                f"amazon_staging insert offset={i} size={len(chunk)}",
                lambda c=chunk: db.table("amazon_staging").insert(c).execute(),
            )
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

        pal_res = _execute_with_import_retries(
            "expected_pallets bulk insert",
            lambda: db.table("expected_pallets").insert(pallet_payloads).select("id").execute(),
        )
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
                _execute_with_import_retries(
                    f"expected_items insert offset={i} size={len(chunk)}",
                    lambda c=chunk: db.table("expected_items").insert(c).execute(),
                )
                items_created += len(chunk)

        _execute_with_import_retries(
            "amazon_staging delete by organization_id",
            lambda: db.table("amazon_staging").delete().eq("organization_id", organization_id).execute(),
        )

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






# API GOOGLE SHEETS
# -------------------------------------------------------------------------
# PHASE 1: ENTERPRISE CATALOG SEEDING (Live Amazon, GPT Mapping, & Vendors)
# -------------------------------------------------------------------------
import requests
import time
from datetime import datetime, timezone

def _get_api_credentials(db: Any, org_id: str, api_name: str) -> Any:
    try:
        res = db.table("organization_api_keys").select("api_key").eq("organization_id", org_id).eq("name", api_name).execute()
        if res.data and res.data[0].get("api_key"):
            key_val = res.data[0]["api_key"]
            if api_name == "amazon_sp_api":
                return json.loads(key_val) if isinstance(key_val, str) else key_val
            return key_val
    except Exception as e:
        log.warning(f"Failed to fetch {api_name} key from DB: {e}")
    return os.getenv("OPENAI_API_KEY") if api_name == "openai_api_key" else None

def _fetch_amazon_catalog_data(asin: str, creds: dict) -> dict:
    try:
        token_res = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": creds.get("refresh_token"),
                "client_id": creds.get("client_id"),
                "client_secret": creds.get("client_secret"),
            },
        )
        token_res.raise_for_status()
        access_token = token_res.json().get("access_token")

        headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}
        params = {"marketplaceIds": "ATVPDKIKX0DER", "includedData": "summaries,images"}
        cat_res = requests.get(
            f"https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items/{asin}",
            headers=headers,
            params=params,
        )
        cat_res.raise_for_status()
        item = cat_res.json()

        title = item.get("summaries", [{}])[0].get("itemName")
        brand = item.get("summaries", [{}])[0].get("brand")
        images = item.get("images", [{}])[0].get("images", [])
        image_url = images[0].get("link") if images else None

        return {"product_name": title, "brand": brand, "main_image_url": image_url, "amazon_raw": item}
    except Exception as e:
        log.warning(
            "Amazon SP-API catalog fetch failed asin=%s error=%s",
            asin,
            e,
            exc_info=True,
        )
        return {}


_PIM_MAP_STANDARD_KEYS = (
    "vendor",
    "category",
    "mfg_part",
    "product_name",
    "seller_sku",
    "asin",
    "fnsku",
    "upc",
    "cost",
)

def _validate_pim_org_store(organization_id: str, store_id: str) -> tuple[str, str]:
    """Accept any 128-bit UUID string (matches TS `isUuidString` / `uuid.UUID`), not RFC version/variant only."""
    oid = (organization_id or "").strip()
    sid = (store_id or "").strip()
    if not oid:
        raise HTTPException(status_code=400, detail="organization_id is required (UUID).")
    if not sid:
        raise HTTPException(status_code=400, detail="store_id is required (imports target store UUID).")
    try:
        uuid.UUID(oid)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"organization_id must be a valid UUID (got {oid[:48]!r}).",
        ) from None
    try:
        uuid.UUID(sid)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"store_id must be a valid UUID (got {sid[:48]!r}).",
        ) from None
    return oid, sid


def _empty_pim_seed_metrics() -> dict[str, Any]:
    return {
        "rows_processed": 0,
        "sheets_processed": 0,
        "rows_per_sheet": {},
        "vendors_created": 0,
        "categories_created": 0,
        "products_created": 0,
        "products_updated": 0,
        "identifiers_created": 0,
        "identifiers_updated": 0,
        "prices_inserted": 0,
        "products_enriched_by_amazon": 0,
        "skipped_no_identity": 0,
        "skipped_ambiguous": 0,
        "errors": [],
    }


def _pim_append_error(errors: list[str], row_index: int | str | None, message: str, cap: int = 200) -> None:
    if len(errors) >= cap:
        return
    prefix = f"row {row_index}: " if row_index is not None else ""
    errors.append(f"{prefix}{message}")


def _gpt_map_catalog_columns_or_raise(headers: list[str], api_key: str) -> dict[str, Any]:
    if not api_key or not str(api_key).strip():
        raise HTTPException(
            status_code=400,
            detail="OpenAI API key is required for column mapping. Configure organization_api_keys (openai_api_key).",
        )
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        prompt = (
            "You are a data mapping AI. Map these CSV/Sheet headers to standard keys: "
            "'vendor', 'category', 'mfg_part', 'product_name', 'seller_sku', 'asin', 'fnsku', 'upc', 'cost'. "
            "Values must be EXACT header strings from the list (character-for-character match). "
            "Omit a key if no column applies. "
            "Reply ONLY with a valid JSON object mapping standard keys to exact header names. "
            f"Headers: {json.dumps(headers)}"
        )
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        raw = res.choices[0].message.content
        parsed = json.loads(raw) if raw else {}
        if not isinstance(parsed, dict):
            raise ValueError("OpenAI returned non-object JSON")
        return parsed
    except HTTPException:
        raise
    except Exception as e:
        log.exception("OpenAI column mapping failed")
        raise HTTPException(
            status_code=502,
            detail={
                "error": "column_mapping_failed",
                "message": str(e),
                "headers": headers,
            },
        ) from e


def _validate_gpt_column_map(column_map: dict[str, Any], original_headers: list[str]) -> None:
    inv = set(original_headers)
    bad: list[str] = []
    for std in _PIM_MAP_STANDARD_KEYS:
        v = column_map.get(std)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        h = str(v).strip()
        if h not in inv:
            bad.append(f"{std} -> {h!r} (not in headers)")
    if bad:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_column_mapping",
                "message": "Mapped column names must exist exactly in the file headers.",
                "invalid_mappings": bad,
                "headers": original_headers,
                "mapping": {k: column_map.get(k) for k in _PIM_MAP_STANDARD_KEYS if column_map.get(k) is not None},
            },
        )


def _pim_handles_from_map(column_map: dict[str, Any], original_headers: list[str]) -> dict[str, str | None]:
    inv = set(original_headers)
    out: dict[str, str | None] = {k: None for k in _PIM_MAP_STANDARD_KEYS}
    for std in _PIM_MAP_STANDARD_KEYS:
        v = column_map.get(std)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        h = str(v).strip()
        if h in inv:
            out[std] = h
    return out


def _pim_load_vendor_index(db: Any, organization_id: str) -> dict[str, str]:
    idx: dict[str, str] = {}
    try:
        r = db.table("vendors").select("id,name").eq("organization_id", organization_id).execute()
        for row in r.data or []:
            n = str(row.get("name") or "").strip()
            if n:
                idx[n.lower()] = str(row["id"])
    except Exception as e:
        log.exception("Failed to load vendors for org=%s", organization_id)
        raise HTTPException(status_code=500, detail=f"Failed to load vendors: {e}") from e
    return idx


def _pim_ensure_vendor(
    db: Any,
    organization_id: str,
    raw_label: str | None,
    index: dict[str, str],
    metrics: dict[str, Any],
    errors: list[str],
    row_index: int | str | None,
) -> str | None:
    name = (raw_label or "").strip() or "Unknown Vendor"
    key = name.lower()
    if key in index:
        return index[key]
    try:
        ins = db.table("vendors").insert({"organization_id": organization_id, "name": name}).execute()
        if not ins.data:
            _pim_append_error(errors, row_index, "vendor insert returned no row")
            return None
        vid = str(ins.data[0]["id"])
        index[key] = vid
        metrics["vendors_created"] += 1
        return vid
    except Exception as e:
        log.warning("vendor insert failed (possible duplicate) org=%s name=%s: %s", organization_id, name, e)
        try:
            r = db.table("vendors").select("id,name").eq("organization_id", organization_id).execute()
            for row in r.data or []:
                n = str(row.get("name") or "").strip()
                if n:
                    index[n.lower()] = str(row["id"])
            if key in index:
                return index[key]
        except Exception as e2:
            log.exception("vendor refetch after insert failure")
            _pim_append_error(errors, row_index, f"vendor resolution failed: {e2}")
            return None
        _pim_append_error(errors, row_index, f"vendor upsert failed: {name}: {e}")
        return None


def _pim_load_category_index(db: Any, organization_id: str) -> dict[str, str]:
    idx: dict[str, str] = {}
    try:
        r = db.table("product_categories").select("id,name").eq("organization_id", organization_id).execute()
        for row in r.data or []:
            n = str(row.get("name") or "").strip()
            if n:
                idx[n.lower()] = str(row["id"])
    except Exception as e:
        log.exception("Failed to load product_categories for org=%s", organization_id)
        raise HTTPException(status_code=500, detail=f"Failed to load categories: {e}") from e
    return idx


def _pim_ensure_category(
    db: Any,
    organization_id: str,
    raw_label: str | None,
    index: dict[str, str],
    metrics: dict[str, Any],
    errors: list[str],
    row_index: int | str | None,
) -> str | None:
    name = (raw_label or "").strip()
    if not name:
        return None
    key = name.lower()
    if key in index:
        return index[key]
    try:
        ins = db.table("product_categories").insert({"organization_id": organization_id, "name": name}).execute()
        if not ins.data:
            _pim_append_error(errors, row_index, "category insert returned no row")
            return None
        cid = str(ins.data[0]["id"])
        index[key] = cid
        metrics["categories_created"] += 1
        return cid
    except Exception as e:
        log.warning("category insert failed org=%s name=%s: %s", organization_id, name, e)
        try:
            r = db.table("product_categories").select("id,name").eq("organization_id", organization_id).execute()
            for row in r.data or []:
                n = str(row.get("name") or "").strip()
                if n:
                    index[n.lower()] = str(row["id"])
            if key in index:
                return index[key]
        except Exception as e2:
            log.exception("category refetch failed")
            _pim_append_error(errors, row_index, f"category resolution failed: {e2}")
            return None
        _pim_append_error(errors, row_index, f"category upsert failed: {name}: {e}")
        return None


def _clean_identifier(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ["x", "n/a", "na", "-", "none", "null", ""] or (len(s) < 2 and s.lower() == "x"):
        return None
    return s


def _pim_cell(row_cells: dict[str, Any], header: str | None) -> Any:
    if not header:
        return None
    return row_cells.get(header)


def _pim_resolve_product(
    db: Any,
    organization_id: str,
    store_id: str,
    seller_sku: str | None,
    fnsku: str | None,
    asin: str | None,
) -> tuple[str | None, str, str | None]:
    """Returns (product_id_or_none, resolution, sku_for_insert_or_none).
    resolution: update | insert | ambiguous | no_identity
    """
    if seller_sku:
        r = (
            db.table("products")
            .select("id")
            .eq("organization_id", organization_id)
            .eq("store_id", store_id)
            .eq("sku", seller_sku)
            .limit(10)
            .execute()
        )
        rows = r.data or []
        if len(rows) > 1:
            return None, "ambiguous", None
        if len(rows) == 1:
            return str(rows[0]["id"]), "update", None
        return None, "insert", seller_sku
    if fnsku:
        r = (
            db.table("products")
            .select("id")
            .eq("organization_id", organization_id)
            .eq("store_id", store_id)
            .eq("fnsku", fnsku)
            .limit(10)
            .execute()
        )
        rows = r.data or []
        if len(rows) > 1:
            return None, "ambiguous", None
        if len(rows) == 1:
            return str(rows[0]["id"]), "update", None
        return None, "insert", fnsku
    if asin:
        r = (
            db.table("products")
            .select("id")
            .eq("organization_id", organization_id)
            .eq("store_id", store_id)
            .eq("asin", asin)
            .limit(20)
            .execute()
        )
        rows = r.data or []
        if len(rows) > 1:
            return None, "ambiguous", None
        if len(rows) == 1:
            return str(rows[0]["id"]), "update", None
        return None, "insert", asin
    return None, "no_identity", None


def _pim_upsert_identifier_map(
    db: Any,
    organization_id: str,
    store_id: str,
    product_id: str,
    seller_sku: str | None,
    asin: str | None,
    fnsku: str | None,
    upc: str | None,
    match_source: str,
    metrics: dict[str, Any],
    errors: list[str],
    row_index: int | str,
) -> None:
    if not (seller_sku or asin or fnsku or upc):
        return
    try:
        q = (
            db.table("product_identifier_map")
            .select("*")
            .eq("organization_id", organization_id)
            .eq("store_id", store_id)
            .eq("product_id", product_id)
        )
        if seller_sku:
            q = q.eq("seller_sku", seller_sku)
        elif fnsku:
            q = q.eq("fnsku", fnsku)
        elif asin:
            q = q.eq("asin", asin)
        existing = q.limit(5).execute()
        rows = existing.data or []
        if not rows:
            db.table("product_identifier_map").insert(
                {
                    "organization_id": organization_id,
                    "store_id": store_id,
                    "product_id": product_id,
                    "seller_sku": seller_sku,
                    "asin": asin,
                    "fnsku": fnsku,
                    "upc_code": upc,
                    "match_source": match_source,
                }
            ).execute()
            metrics["identifiers_created"] += 1
            return
        rec = rows[0]
        mid = str(rec["id"])
        payload: dict[str, Any] = {}
        if asin and not rec.get("asin"):
            payload["asin"] = asin
        if fnsku and not rec.get("fnsku"):
            payload["fnsku"] = fnsku
        if seller_sku and not rec.get("seller_sku"):
            payload["seller_sku"] = seller_sku
        if upc and not rec.get("upc_code"):
            payload["upc_code"] = upc
        if payload:
            db.table("product_identifier_map").update(payload).eq("id", mid).eq("organization_id", organization_id).eq(
                "store_id", store_id
            ).execute()
            metrics["identifiers_updated"] += 1
    except Exception as e:
        log.exception("product_identifier_map upsert failed product_id=%s", product_id)
        _pim_append_error(errors, row_index, f"identifier map: {e}")


def _pim_insert_price_if_present(
    db: Any,
    organization_id: str,
    store_id: str,
    product_id: str,
    cost_raw: Any,
    price_source: str,
    metrics: dict[str, Any],
    errors: list[str],
    row_index: int | str,
) -> None:
    if cost_raw is None:
        return
    s = str(cost_raw).strip()
    if not s or s.lower() in ("x", "n/a", "na", "-", "none", "null"):
        return
    try:
        amt = float(s.replace("$", "").replace(",", ""))
    except (TypeError, ValueError):
        _pim_append_error(errors, row_index, f"invalid price/cost value: {s[:40]!r}")
        return
    try:
        db.table("product_prices").insert(
            {
                "organization_id": organization_id,
                "store_id": store_id,
                "product_id": product_id,
                "amount": amt,
                "currency": "USD",
                "source": price_source,
            }
        ).execute()
        metrics["prices_inserted"] += 1
    except Exception as e:
        log.exception("product_prices insert failed product_id=%s", product_id)
        _pim_append_error(errors, row_index, f"price insert: {e}")


def _process_pim_seed_row(
    db: Any,
    organization_id: str,
    store_id: str,
    row_cells: dict[str, Any],
    handles: dict[str, str | None],
    amazon_creds: Any,
    price_source: str,
    match_source: str,
    metrics: dict[str, Any],
    errors: list[str],
    row_index: int | str,
    vendor_index: dict[str, str],
    category_index: dict[str, str],
) -> None:
    metrics["rows_processed"] += 1

    seller_sku = _clean_identifier(_pim_cell(row_cells, handles.get("seller_sku")))
    asin = _clean_identifier(_pim_cell(row_cells, handles.get("asin")))
    fnsku = _clean_identifier(_pim_cell(row_cells, handles.get("fnsku")))
    upc = _clean_identifier(_pim_cell(row_cells, handles.get("upc")))
    sheet_product_name = _clean_identifier(_pim_cell(row_cells, handles.get("product_name")))
    mfg_part = _clean_identifier(_pim_cell(row_cells, handles.get("mfg_part")))
    cost_col = handles.get("cost")
    cost_raw = _pim_cell(row_cells, cost_col) if cost_col else None

    vendor_id = _pim_ensure_vendor(
        db,
        organization_id,
        _clean_identifier(_pim_cell(row_cells, handles.get("vendor"))),
        vendor_index,
        metrics,
        errors,
        row_index,
    )
    category_id = _pim_ensure_category(
        db,
        organization_id,
        _clean_identifier(_pim_cell(row_cells, handles.get("category"))),
        category_index,
        metrics,
        errors,
        row_index,
    )

    if not seller_sku and not fnsku and not asin:
        metrics["skipped_no_identity"] += 1
        _pim_append_error(
            errors,
            row_index,
            "skipped: need at least one of seller_sku, fnsku, or asin for store-scoped product identity",
        )
        return

    amazon_data: dict[str, Any] = {}
    if asin and amazon_creds:
        amazon_data = _fetch_amazon_catalog_data(asin, amazon_creds)
        time.sleep(0.2)
        if amazon_data:
            metrics["products_enriched_by_amazon"] += 1
        elif asin:
            _pim_append_error(errors, row_index, f"Amazon enrichment returned no data for ASIN {asin}")

    final_name = (
        amazon_data.get("product_name")
        or sheet_product_name
        or f"Pending Details ({asin or seller_sku or fnsku})"
    )
    final_brand = amazon_data.get("brand")
    main_image = amazon_data.get("main_image_url")
    amazon_raw = amazon_data.get("amazon_raw") if amazon_data else None

    prod_id, resolution, sku_insert = _pim_resolve_product(db, organization_id, store_id, seller_sku, fnsku, asin)
    if resolution == "ambiguous":
        metrics["skipped_ambiguous"] += 1
        _pim_append_error(
            errors,
            row_index,
            f"ambiguous product match org+store+identifiers (sku={seller_sku!r} fnsku={fnsku!r} asin={asin!r})",
        )
        return
    if resolution == "no_identity":
        metrics["skipped_no_identity"] += 1
        _pim_append_error(errors, row_index, "no_identity: missing seller_sku, fnsku, and asin")
        return

    sync_iso = datetime.now(timezone.utc).isoformat()

    try:
        if resolution == "update" and prod_id:
            upd: dict[str, Any] = {
                "vendor_id": vendor_id,
                "category_id": category_id,
                "mfg_part_number": mfg_part,
                "product_name": final_name,
                "brand": final_brand,
                "main_image_url": main_image,
                "amazon_raw": amazon_raw or {},
                "last_catalog_sync_at": sync_iso,
            }
            if seller_sku:
                upd["sku"] = seller_sku
            if asin:
                upd["asin"] = asin
            if fnsku:
                upd["fnsku"] = fnsku
            if upc:
                upd["upc_code"] = upc
            db.table("products").update(upd).eq("id", prod_id).eq("organization_id", organization_id).eq(
                "store_id", store_id
            ).execute()
            metrics["products_updated"] += 1
        elif resolution == "insert" and sku_insert:
            ins = (
                db.table("products")
                .insert(
                    {
                        "organization_id": organization_id,
                        "store_id": store_id,
                        "sku": sku_insert,
                        "vendor_id": vendor_id,
                        "category_id": category_id,
                        "mfg_part_number": mfg_part,
                        "product_name": final_name,
                        "brand": final_brand,
                        "main_image_url": main_image,
                        "amazon_raw": amazon_raw if amazon_raw is not None else {},
                        "asin": asin,
                        "fnsku": fnsku,
                        "upc_code": upc,
                        "last_catalog_sync_at": sync_iso,
                    }
                )
                .execute()
            )
            if not ins.data:
                _pim_append_error(errors, row_index, "product insert returned no row")
                return
            prod_id = str(ins.data[0]["id"])
            metrics["products_created"] += 1
        else:
            metrics["skipped_no_identity"] += 1
            _pim_append_error(errors, row_index, "could not determine sku for new product")
            return
    except Exception as e:
        log.exception("product upsert failed row=%s", row_index)
        _pim_append_error(errors, row_index, f"product upsert: {e}")
        return

    _pim_upsert_identifier_map(
        db,
        organization_id,
        store_id,
        prod_id,
        seller_sku,
        asin,
        fnsku,
        upc,
        match_source,
        metrics,
        errors,
        row_index,
    )
    _pim_insert_price_if_present(
        db, organization_id, store_id, prod_id, cost_raw, price_source, metrics, errors, row_index
    )


def _pim_row_cells_from_series(row: Any, headers: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for h in headers:
        try:
            if h in row.index:
                out[h] = _cell_to_json_safe(row[h])
            else:
                out[h] = None
        except Exception:
            out[h] = None
    return out


# --- Google Sheets Sync helpers (used by etl_sync_google_sheets) ---
def _extract_google_sheet_id_from_module_configs(module_configs: Any) -> str | None:
    if not module_configs or not isinstance(module_configs, dict):
        return None
    cat = module_configs.get("catalog")
    if isinstance(cat, dict):
        sid = cat.get("google_sheet_id")
        if isinstance(sid, str) and sid.strip():
            return sid.strip()
    return None


def _get_google_sheet_id_from_workspace(db: Any, organization_id: str) -> str | None:
    """Resolve google_sheet_id from workspace_settings.module_configs.catalog (Next app shape), with fallbacks."""
    try:
        res = (
            db.table("workspace_settings")
            .select("module_configs")
            .eq("organization_id", organization_id)
            .limit(1)
            .execute()
        )
        if res.data:
            found = _extract_google_sheet_id_from_module_configs(res.data[0].get("module_configs"))
            if found:
                return found
    except Exception as e:
        log.debug("workspace_settings by organization_id: %s", e)
    try:
        res = db.table("workspace_settings").select("module_configs").limit(1).execute()
        if res.data:
            found = _extract_google_sheet_id_from_module_configs(res.data[0].get("module_configs"))
            if found:
                return found
    except Exception as e:
        log.debug("workspace_settings singleton: %s", e)
    try:
        res = (
            db.table("workspace_settings")
            .select("value")
            .eq("organization_id", organization_id)
            .eq("key", "google_sheet_id")
            .limit(1)
            .execute()
        )
        if res.data:
            v = res.data[0].get("value")
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception as e:
        log.debug("workspace_settings legacy key/value: %s", e)
    return None


def _enrich_products_with_vendor_names(db: Any, rows: List[dict[str, Any]]) -> List[dict[str, Any]]:
    if not rows:
        return rows
    seen: set[str] = set()
    vendor_ids: list[str] = []
    for r in rows:
        vid = r.get("vendor_id")
        if vid is None:
            continue
        sid = str(vid)
        if sid not in seen:
            seen.add(sid)
            vendor_ids.append(sid)
    id_to_name: dict[str, str] = {}
    if vendor_ids:
        try:
            vr = db.table("vendors").select("id, name").in_("id", vendor_ids).execute()
            for v in vr.data or []:
                if v.get("id") is not None:
                    id_to_name[str(v["id"])] = str(v.get("name") or "").strip()
        except Exception as e:
            log.warning("Vendor name enrichment failed: %s", e)
    for r in rows:
        if r.get("vendor_name"):
            continue
        vid = r.get("vendor_id")
        r["vendor_name"] = id_to_name.get(str(vid)) if vid else None
    return rows


def _get_google_creds(db: Any, org_id: str):
    """Fetches Google Service Account JSON from organization_api_keys."""
    try:
        from google.oauth2 import service_account
    except ModuleNotFoundError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "Google Sheets dependencies are not installed. Run: "
                "python -m pip install google-api-python-client google-auth"
            ),
        ) from e

    try:
        res = (
            db.table("organization_api_keys")
            .select("api_key")
            .eq("organization_id", org_id)
            .eq("name", "google_sheets_api")
            .execute()
        )
        if res.data:
            creds_info = json.loads(res.data[0]["api_key"])
            return service_account.Credentials.from_service_account_info(creds_info)
    except HTTPException:
        raise
    except Exception as e:
        log.error("Failed to load Google credentials: %s", e)
    return None


@app.post("/etl/seed-products")
async def etl_seed_products(file: UploadFile = File(...), organization_id: str = Form(...), store_id: str = Form(...)):
    organization_id, store_id = _validate_pim_org_store(organization_id, store_id)
    db = _require_supabase()
    try:
        raw_bytes = await file.read()
        if not raw_bytes:
            raise HTTPException(status_code=400, detail="Empty file.")

        openai_key = _get_api_credentials(db, organization_id, "openai_api_key")
        amazon_creds = _get_api_credentials(db, organization_id, "amazon_sp_api")

        metrics = _empty_pim_seed_metrics()
        errors: list[str] = metrics["errors"]
        vendor_index = _pim_load_vendor_index(db, organization_id)
        category_index = _pim_load_category_index(db, organization_id)

        column_map: dict[str, Any] | None = None
        handles: dict[str, str | None] | None = None
        reference_headers: list[str] | None = None
        saw_any_yield = False
        saw_frame = False
        fname = file.filename

        for sheet_label, df in _iter_seed_product_frames(raw_bytes, fname):
            saw_any_yield = True
            original_headers = [str(c).strip() for c in df.columns]
            if not any(h for h in original_headers if h):
                continue
            saw_frame = True

            rows_before = int(metrics["rows_processed"])
            # Reuse GPT map only when header list matches the first sheet exactly (order-sensitive).
            if column_map is None or reference_headers != original_headers:
                column_map = _gpt_map_catalog_columns_or_raise(original_headers, openai_key)
                _validate_gpt_column_map(column_map, original_headers)
                handles = _pim_handles_from_map(column_map, original_headers)
                reference_headers = list(original_headers)

            assert handles is not None
            metrics["sheets_processed"] = int(metrics["sheets_processed"]) + 1
            match_src = f"etl_seed_products:{sheet_label}" if sheet_label else "etl_seed_products_csv"

            for i, (_, row) in enumerate(df.iterrows()):
                row_cells = _pim_row_cells_from_series(row, original_headers)
                row_lbl: int | str = f"{sheet_label}!{i + 2}" if sheet_label else i + 2
                _process_pim_seed_row(
                    db,
                    organization_id,
                    store_id,
                    row_cells,
                    handles,
                    amazon_creds,
                    "etl_seed_products",
                    match_src,
                    metrics,
                    errors,
                    row_lbl,
                    vendor_index,
                    category_index,
                )

            sheet_key = sheet_label or "(csv)"
            metrics["rows_per_sheet"][sheet_key] = int(metrics["rows_processed"]) - rows_before

        if not saw_any_yield:
            raise HTTPException(status_code=400, detail="Empty file.")
        if not saw_frame:
            raise HTTPException(
                status_code=400,
                detail="No readable tabular data: for Excel, add at least one sheet with a header row and data.",
            )

        return {"status": "success", "message": "Catalog import finished.", "metrics": metrics}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("etl_seed_products failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/etl/sync-google-sheets")
async def etl_sync_google_sheets(organization_id: str = Form(...), store_id: str = Form(...)):
    organization_id, store_id = _validate_pim_org_store(organization_id, store_id)
    db = _require_supabase()

    sheet_id = _get_google_sheet_id_from_workspace(db, organization_id)
    if not sheet_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "Google Sheet ID not found. Configure catalog.google_sheet_id in workspace module_configs "
                "or legacy workspace_settings key google_sheet_id."
            ),
        )

    creds = _get_google_creds(db, organization_id)
    if not creds:
        raise HTTPException(status_code=401, detail="Google API Key not configured in Organization Keys.")

    try:
        try:
            from googleapiclient.discovery import build
        except ModuleNotFoundError as e:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Google Sheets dependencies are not installed. Run: "
                    "python -m pip install google-api-python-client google-auth"
                ),
            ) from e

        service = build("sheets", "v4", credentials=creds)
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get("sheets", [])

        metrics = _empty_pim_seed_metrics()
        metrics["sheets_processed"] = 0
        errors: list[str] = metrics["errors"]

        vendor_index = _pim_load_vendor_index(db, organization_id)
        category_index = _pim_load_category_index(db, organization_id)
        openai_key = _get_api_credentials(db, organization_id, "openai_api_key")
        amazon_creds = _get_api_credentials(db, organization_id, "amazon_sp_api")

        for sheet in sheets:
            sheet_name = sheet["properties"]["title"]
            result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"'{sheet_name}'!A:Z").execute()
            values = result.get("values", [])

            if not values or len(values) < 2:
                log.info(
                    "Skipping empty Google Sheet tab tab=%r spreadsheet=%r",
                    sheet_name,
                    sheet_id,
                )
                continue

            metrics["sheets_processed"] += 1
            headers = [str(c).strip() for c in values[0]]
            rows = [row + [""] * (len(headers) - len(row)) for row in values[1:]]
            df = pd.DataFrame(rows, columns=headers)
            original_headers = headers

            column_map = _gpt_map_catalog_columns_or_raise(original_headers, openai_key)
            _validate_gpt_column_map(column_map, original_headers)
            handles = _pim_handles_from_map(column_map, original_headers)

            for j, (_, row) in enumerate(df.iterrows()):
                row_cells = _pim_row_cells_from_series(row, original_headers)
                _process_pim_seed_row(
                    db,
                    organization_id,
                    store_id,
                    row_cells,
                    handles,
                    amazon_creds,
                    "etl_google_sheets",
                    "etl_google_sheets",
                    metrics,
                    errors,
                    j + 2,
                    vendor_index,
                    category_index,
                )

        return {
            "status": "success",
            "message": f"Google Sheets sync finished ({metrics['sheets_processed']} non-empty tabs).",
            "metrics": metrics,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("etl_sync_google_sheets failed")
        raise HTTPException(status_code=500, detail=f"Google Sync Failed: {e}") from e


# --- 2. Real Product List Endpoint ---
@app.get("/etl/products/{organization_id}")
async def get_real_products(organization_id: str):
    db = _require_supabase()
    # Fetch from products table with their current prices and identifiers
    res = db.table("products").select("*, product_identifier_map(*), product_prices(*)").eq("organization_id", organization_id).execute()
    rows = list(res.data or [])
    rows = _enrich_products_with_vendor_names(db, rows)
    return {"status": "success", "products": rows}