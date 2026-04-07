import io
import json
import uuid
from typing import Any

import pandas as pd
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
    try:
        return pd.read_csv(
            buf,
            sep=None,
            engine="python",
            encoding="utf-8-sig",
            dtype=object,
        )
    except Exception:
        buf.seek(0)
        return pd.read_csv(buf, sep="\t", encoding="utf-8-sig", dtype=object)


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
    "order_id":           ["order-id", "order_id", "removal_order_id", "removal-order-id"],
    "order_type":         ["order-type", "order_type"],
    "order_status":       ["order-status", "order_status"],
    "sku":                ["sku"],
    "fnsku":              ["fnsku"],
    "disposition":        ["disposition"],
    "shipped_quantity":   ["shipped-quantity", "shipped_quantity"],
    "requested_quantity": ["requested-quantity", "requested_quantity", "quantity"],
    "cancelled_quantity": ["cancelled-quantity", "cancelled_quantity"],
    "disposed_quantity":  ["disposed-quantity", "disposed_quantity"],
    "tracking_number":    ["tracking-number", "tracking_number"],
    "carrier":            ["carrier", "carrier-name", "carrier_name"],
    "shipment_date":      ["carrier-shipment-date", "shipment-date", "ship-date", "shipped-date"],
    "order_date":         ["request-date", "order-date", "order_date", "request_date"],
}

_INT_REMOVAL_COLS = {"shipped_quantity", "requested_quantity", "cancelled_quantity", "disposed_quantity"}


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
            if val is not None:
                try:
                    result[db_col] = int(float(str(val).replace(",", "")))
                except (ValueError, TypeError):
                    result[db_col] = None
            else:
                result[db_col] = None
        elif db_col == "order_date":
            if val is not None:
                try:
                    d = pd.to_datetime(val, errors="coerce")
                    result[db_col] = d.date().isoformat() if pd.notna(d) else None
                except Exception:
                    result[db_col] = None
            else:
                result[db_col] = None
        else:
            result[db_col] = val

    return result


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
    print(f"[sync-removals] START task_id={task_id} org={organization_id} upload_id={upload_id}")
    _update_task(task_id, 10, "Task is alive. Connecting to database...")

    _url = os.getenv("SUPABASE_URL")
    _key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not _url or not _key:
        print("[sync-removals] ERROR: Missing Supabase credentials")
        _update_task(task_id, 0, "Missing Supabase credentials in environment.", status="failed")
        return

    try:
        db = create_client(_url, _key)
        print("[sync-removals] Supabase client created successfully")
    except Exception as conn_err:
        print(f"[sync-removals] CONNECTION ERROR: {conn_err!s}")
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    try:
        _update_task(task_id, 10, "Fetching rows from amazon_staging...")
        print(f"[sync-removals] Querying amazon_staging (upload_id filter: {upload_id})")

        query = db.table("amazon_staging").select("*").eq("organization_id", organization_id)
        if upload_id:
            query = query.eq("upload_id", upload_id)
        res = query.execute()
        staging_rows: list[dict[str, Any]] = res.data or []
        print(f"[sync-removals] Fetched {len(staging_rows)} rows from amazon_staging")

        if not staging_rows:
            print("[sync-removals] No staging rows found — completing early")
            _update_task(task_id, 100, "No staging rows found. Nothing to sync.", status="completed")
            return

        _update_task(task_id, 20, f"Extracting {len(staging_rows)} staging rows...")

        removal_rows: list[dict[str, Any]] = []
        staging_ids: list[str] = []
        skipped = 0

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

            if not extracted.get("order_id"):
                skipped += 1
                continue

            removal_rows.append(extracted)
            staging_ids.append(row["id"])

        print(f"[sync-removals] Extracted {len(removal_rows)} valid rows; skipped {skipped}")

        if not removal_rows:
            _update_task(task_id, 100, "No valid removal rows extracted (missing order_id).", status="completed")
            return

        # ── Smart Merge: fetch existing (order_id, sku) keys to enable UPSERT-like
        # behaviour without requiring a DB unique constraint.
        # File 1 (Removal Order Detail): has quantities/disposition, NO tracking_number
        # File 2 (Removal Shipment Detail): has tracking_number/carrier, fills the gap
        _update_task(task_id, 38, "Fetching existing amazon_removals keys for merge check...")
        print(f"[sync-removals] Fetching existing (order_id, sku) keys from amazon_removals...")
        try:
            existing_res = (
                db.table("amazon_removals")
                .select("id, order_id, sku")
                .eq("organization_id", organization_id)
                .execute()
            )
            existing_map: dict[tuple[str, str], str] = {
                (str(r["order_id"]), str(r["sku"])): str(r["id"])
                for r in (existing_res.data or [])
                if r.get("order_id") and r.get("sku")
            }
        except Exception as fetch_ex:
            print(f"[sync-removals] WARNING: could not fetch existing rows — will INSERT all: {fetch_ex!s}")
            existing_map = {}

        print(f"[sync-removals] Found {len(existing_map)} existing rows to merge against")

        rows_to_insert: list[dict[str, Any]] = []
        rows_to_update: list[tuple[str, dict[str, Any]]] = []  # (id, patch)

        for row in removal_rows:
            key = (str(row.get("order_id", "")), str(row.get("sku", "")))
            if key[0] and key[1] and key in existing_map:
                # Merge — only send non-null fields so existing data is never wiped
                patch = {
                    k: v for k, v in row.items()
                    if v is not None and k not in ("organization_id", "upload_id")
                }
                rows_to_update.append((existing_map[key], patch))
            else:
                rows_to_insert.append(row)

        print(f"[sync-removals] {len(rows_to_insert)} to INSERT, {len(rows_to_update)} to MERGE-UPDATE")

        _update_task(task_id, 40, f"Writing {len(removal_rows)} rows to amazon_removals...")

        inserted = 0
        total_insert = len(rows_to_insert)
        for i in range(0, total_insert, STAGING_INSERT_BATCH):
            chunk = rows_to_insert[i : i + STAGING_INSERT_BATCH]
            print(f"[sync-removals] Inserting chunk {i}–{i + len(chunk)}")
            db.table("amazon_removals").insert(chunk).execute()
            inserted += len(chunk)
            progress = 40 + int((inserted / max(len(removal_rows), 1)) * 35)
            _update_task(task_id, progress, f"Inserted {inserted}/{total_insert} new rows...")

        merged_count = 0
        for row_id, patch in rows_to_update:
            db.table("amazon_removals").update(patch).eq("id", row_id).execute()
            merged_count += 1
            if merged_count % 50 == 0:
                progress = 75 + int((merged_count / max(len(rows_to_update), 1)) * 10)
                _update_task(task_id, progress, f"Merged {merged_count}/{len(rows_to_update)} existing rows...")

        _update_task(task_id, 85, f"Cleaning up {len(staging_ids)} rows from amazon_staging...")
        print(f"[sync-removals] Deleting {len(staging_ids)} staging rows by id")

        for i in range(0, len(staging_ids), STAGING_INSERT_BATCH):
            id_chunk = staging_ids[i : i + STAGING_INSERT_BATCH]
            db.table("amazon_staging").delete().in_("id", id_chunk).execute()

        summary = f"inserted={inserted}, merged={merged_count}"
        print(f"[sync-removals] DONE — {summary}")
        _update_task(
            task_id,
            100,
            f"Sync complete. {summary} rows written to amazon_removals.",
            status="completed",
        )

    except Exception as e:
        print(f"[sync-removals] ERROR: {e!s}")
        _update_task(task_id, 0, f"Sync failed: {e!s}", status="failed")


# --- Background task: amazon_removals -> expected_packages ---
# Must be a plain def (not async) so FastAPI runs it in a thread pool,
# keeping the synchronous supabase-py client off the event loop.
# IMPORTANT: never call _require_supabase() here — it raises HTTPException,
# which is a Starlette request-level exception and will cause
# "RuntimeError: Caught handled exception, but response already started"
# when raised outside a request context (i.e., in a background thread).

def _safe_int(val: Any) -> int | None:
    """Coerce any scalar to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return None


def _run_generate_worklist(
    task_id: str,
    organization_id: str,
    upload_id: str | None,
) -> None:
    print(f"[generate-worklist] START task_id={task_id} org={organization_id} upload_id={upload_id}")

    # Immediate heartbeat — proves the thread is alive before any DB work
    _update_task(task_id, 10, "Task is alive. Connecting to database...")

    _url = os.getenv("SUPABASE_URL")
    _key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not _url or not _key:
        print("[generate-worklist] ERROR: Missing Supabase credentials")
        _update_task(task_id, 0, "Missing Supabase credentials in environment.", status="failed")
        return

    try:
        db = create_client(_url, _key)
        print("[generate-worklist] Supabase client created successfully")
    except Exception as conn_err:
        print(f"[generate-worklist] CONNECTION ERROR: {conn_err!s}")
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    # --- Fetch from amazon_removals (isolated try-except for clear error reporting) ---
    _update_task(task_id, 15, "Fetching rows from amazon_removals...")
    try:
        print("DEBUG: Starting fetch from removals...")
        print(f"[generate-worklist] Querying amazon_removals (upload_id filter: {upload_id})")
        query = db.table("amazon_removals").select("*").eq("organization_id", organization_id)
        if upload_id:
            query = query.eq("upload_id", upload_id)
        res = query.execute()
        removal_rows: list[dict[str, Any]] = res.data or []
        print(f"[generate-worklist] Fetched {len(removal_rows)} rows from amazon_removals")
    except Exception as fetch_err:
        print(f"[generate-worklist] FETCH ERROR: {fetch_err!s}")
        _update_task(task_id, 0, f"Database fetch failed: {fetch_err!s}", status="failed")
        return

    if not removal_rows:
        print("[generate-worklist] No rows found — completing early")
        _update_task(task_id, 100, "No removal rows found. Worklist is empty.", status="completed")
        return

    # --- Build package rows ---
    try:
        _update_task(task_id, 25, f"Building package rows from {len(removal_rows)} removal rows...")
        print("[generate-worklist] Building expected_packages rows...")

        package_rows: list[dict[str, Any]] = []
        skipped = 0
        for row in removal_rows:
            order_id = row.get("order_id")
            sku = row.get("sku")
            if not order_id or not sku:
                print(f"[generate-worklist] Skipping row — missing order_id or sku: {row}")
                skipped += 1
                continue

            shipped_qty = _safe_int(row.get("shipped_quantity"))
            print(
                f"[generate-worklist] order_id={order_id} sku={sku} "
                f"shipped_quantity raw={row.get('shipped_quantity')!r} coerced={shipped_qty}"
            )

            pkg: dict[str, Any] = {
                "organization_id": organization_id,
                "order_id": order_id,
                "sku": sku,
                "tracking_number": row.get("tracking_number"),
                "shipped_quantity": shipped_qty,
                "requested_quantity": _safe_int(row.get("requested_quantity")),
                "disposed_quantity": _safe_int(row.get("disposed_quantity")),
                "cancelled_quantity": _safe_int(row.get("cancelled_quantity")),
                "order_status": "Pending",
                "disposition": row.get("disposition"),
                "order_date": row.get("order_date"),
            }
            if upload_id:
                pkg["upload_id"] = upload_id

            package_rows.append(pkg)

        print(f"[generate-worklist] Built {len(package_rows)} package rows; skipped {skipped}")

        if not package_rows:
            _update_task(task_id, 100, "No valid package rows to insert (missing order_id or sku).", status="completed")
            return

    except Exception as build_err:
        print(f"[generate-worklist] BUILD ERROR: {build_err!s}")
        _update_task(task_id, 0, f"Row-building failed: {build_err!s}", status="failed")
        return

    # --- Sync into expected_packages (fetch → compare → insert new / update existing) ---
    # Strategy: fetch existing (organization_id, order_id, sku) keys first, then:
    #   • Row doesn't exist yet   → INSERT the full package row
    #   • Row already exists      → UPDATE with all non-null fields from amazon_removals
    #     This is what fills in tracking_number / carrier when the Shipment file arrives
    #     after the Order Detail file has already created the expected_packages rows.
    try:
        _update_task(task_id, 38, "Fetching existing expected_packages keys for sync check...")
        print("[generate-worklist] Fetching existing (order_id, sku) keys from expected_packages...")

        existing_pkg_res = (
            db.table("expected_packages")
            .select("id, order_id, sku")
            .eq("organization_id", organization_id)
            .execute()
        )
        existing_pkg_map: dict[tuple[str, str], str] = {
            (str(r["order_id"]), str(r["sku"])): str(r["id"])
            for r in (existing_pkg_res.data or [])
            if r.get("order_id") and r.get("sku")
        }
        print(f"[generate-worklist] Found {len(existing_pkg_map)} existing expected_packages rows")

    except Exception as fetch_existing_err:
        print(f"[generate-worklist] WARNING: could not fetch existing expected_packages — will INSERT all: {fetch_existing_err!s}")
        existing_pkg_map = {}

    try:
        pkg_to_insert: list[dict[str, Any]] = []
        pkg_to_update: list[tuple[str, dict[str, Any]]] = []  # (id, patch)

        for pkg in package_rows:
            key = (str(pkg.get("order_id", "")), str(pkg.get("sku", "")))
            if key[0] and key[1] and key in existing_pkg_map:
                # Build a patch from non-null fields only — never wipe existing data
                # Crucially: this overwrites NULL tracking_number with the real value
                patch = {
                    k: v for k, v in pkg.items()
                    if v is not None and k not in ("organization_id", "upload_id", "order_status")
                }
                if patch:
                    pkg_to_update.append((existing_pkg_map[key], patch))
            else:
                pkg_to_insert.append(pkg)

        print(f"[generate-worklist] {len(pkg_to_insert)} to INSERT, {len(pkg_to_update)} to UPDATE")

        _update_task(task_id, 40, f"Writing {len(package_rows)} rows to expected_packages...")

        # Batch INSERT new rows
        inserted = 0
        for i in range(0, len(pkg_to_insert), STAGING_INSERT_BATCH):
            chunk = pkg_to_insert[i : i + STAGING_INSERT_BATCH]
            print(f"[generate-worklist] Inserting chunk {i}–{i + len(chunk)} into expected_packages")
            db.table("expected_packages").insert(chunk).execute()
            inserted += len(chunk)
            progress = 40 + int((inserted / max(len(package_rows), 1)) * 30)
            _update_task(task_id, progress, f"Inserted {inserted}/{len(pkg_to_insert)} new rows...")

        # UPDATE existing rows (fills in tracking_number, carrier, etc.)
        updated = 0
        for row_id, patch in pkg_to_update:
            print(f"[generate-worklist] Updating expected_packages id={row_id} patch_keys={list(patch.keys())}")
            db.table("expected_packages").update(patch).eq("id", row_id).execute()
            updated += 1
            if updated % 50 == 0:
                progress = 70 + int((updated / max(len(pkg_to_update), 1)) * 25)
                _update_task(task_id, progress, f"Updated {updated}/{len(pkg_to_update)} existing rows...")

        summary = f"inserted={inserted}, updated={updated}"
        print(f"[generate-worklist] DONE — {summary}")
        _update_task(
            task_id,
            100,
            f"Worklist synced. {summary} rows in expected_packages.",
            status="completed",
        )

    except Exception as sync_err:
        print(f"[generate-worklist] SYNC ERROR: {sync_err!s}")
        _update_task(task_id, 0, f"Sync into expected_packages failed: {sync_err!s}", status="failed")


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
    Uses UPSERT on (order_id, sku, tracking_number).
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
