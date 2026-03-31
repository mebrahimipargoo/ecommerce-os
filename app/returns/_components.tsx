"use client";

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { usePhysicalScanner } from "../../hooks/usePhysicalScanner";
import { createPortal } from "react-dom";
import Link from "next/link";
import {
  AlertTriangle, ArrowLeft, ArrowRight, Barcode, Boxes, Calendar, CalendarX2,
  Camera, CheckCircle2, CheckSquare, ChevronDown, ChevronRight, ChevronUp, CircleDot, ClipboardCheck,
  Clock, Copy, ExternalLink, Eye, FileImage, FileText, Loader2, Minus, MoreHorizontal, Package2, Store,
  PackageCheck, PackageX, Pencil, Plus, QrCode, Save, ScanLine, Search,
  ShieldAlert, ShieldCheck, Sparkles, Tag, Trash2, Truck, User, X, XCircle, Zap, ZoomIn,
} from "lucide-react";
import { ReturnIdentifiersColumn } from "../../components/ReturnIdentifiersColumn";
import { SmartCameraUpload } from "../../components/ui/SmartCameraUpload";
import { BarcodeScannerModal } from "../../components/ui/BarcodeScannerModal";
import {
  insertReturn, updateReturn, deleteReturn,
  createPallet, updatePallet, updatePalletStatus, deletePallet,
  createPackage, updatePackage, closePackage, deletePackage,
  listReturnsByPackage,
} from "./actions";
import type {
  ExpectedItem,
  OrgSettings,
  PackageRecord,
  PackageStatus,
  PalletRecord,
  PalletStatus,
  ReturnRecord,
  ReturnUpdatePayload,
} from "./returns-action-types";
import { marketplaceSearchUrl as marketplaceSearchUrlLib } from "../../lib/marketplace-search-url";
import { itemMatchesPackageExpectation } from "../../lib/package-expectations";
import { getBarcodeModeFromStorage, getDefaultStoreIdFromStorage } from "../../lib/openai-settings";
import { classifyProductBarcode } from "../../lib/product-barcode-classify";
import { parseBarcodeSource } from "../../lib/utils/barcode-parser";
import { supabase as supabaseBrowser } from "../../src/lib/supabase";
import { uploadToIncidentPhotos, uploadToStorage } from "../../lib/supabase/storage";
import { MasterUploader } from "../../components/MasterUploader";
import {
  buildEntityPhotoEvidence,
  mergeEntityPhotoEvidence,
  normalizeEntityPhotoEvidenceUrls,
  resolvePackageClaimPhotoUrls,
  setPackageClaimEvidenceSlot,
} from "../../lib/entity-photo-evidence";
import { fetchProductFromAmazon } from "../../lib/api/amazon-mock";
import { operatorDisplayLabel } from "../../lib/operator-display";
import {
  getReturnPhotoEvidenceUrls,
  mergeReturnPhotoEvidence,
  photoEvidenceCategoryCounts,
  photoEvidenceNumericTotal,
} from "../../lib/return-photo-evidence";
import { listStores } from "../settings/adapters/actions";
import { isUuidString, uuidFkInvalidMessage } from "../../lib/uuid";
import { isAdminRole, type UserRole } from "../../components/UserRoleContext";

/** Seeded MVP org — use in client `stores` queries so RLS returns rows for local dev. */
export const MVP_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

/** Simulated org setting: show expiry-label upload when expiration is within this many days (FEFO). */
export const CLAIM_EXPIRY_EVIDENCE_THRESHOLD_DAYS = 90;

export function shouldShowExpiryLabelPhoto(state: { condition_keys: string[]; expiration_date: string }): boolean {
  if (state.condition_keys.includes("expired")) return true;
  const raw = state.expiration_date?.trim();
  if (!raw) return false;
  const d = new Date(`${raw}T12:00:00`);
  if (Number.isNaN(d.getTime())) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const exp = new Date(d);
  exp.setHours(0, 0, 0, 0);
  const diffDays = (exp.getTime() - today.getTime()) / 86400000;
  return diffDays >= 0 && diffDays <= CLAIM_EXPIRY_EVIDENCE_THRESHOLD_DAYS;
}

/** When true, the expiry-label slot is shown and a photo is required (Expired tag or critical FEFO window). */
export function isExpiryLabelPhotoMandatory(state: { condition_keys: string[]; expiration_date: string }): boolean {
  return shouldShowExpiryLabelPhoto(state);
}

// ─── Contextual Scan Button ────────────────────────────────────────────────────
//
// Physical / keyboard-wedge mode (e.g. Netum C750):
//   Clicking the button does NOT open a camera modal. It shows a "ready" badge
//   on the input and waits — the wedge scanner types characters into the page
//   at high speed and the global usePhysicalScanner hook captures the barcode.
//
// Web / Camera mode:
//   Clicking the button opens the BarcodeScannerModal with a live camera feed.

interface ContextualScanButtonProps {
  /** Called with the scanned code — consumed by both modes. */
  onDetected: (code: string) => void;
  /** Title shown in the camera modal. */
  modalTitle?: string;
  /** Extra class names on the trigger button. */
  className?: string;
}

function ContextualScanButton({ onDetected, modalTitle = "Scan Barcode", className = "" }: ContextualScanButtonProps) {
  const [modalOpen,  setModalOpen]  = useState(false);
  const [scanReady,  setScanReady]  = useState(false);  // physical-mode ready state

  // Read barcode mode fresh from localStorage each click (avoids stale closure).
  function handleClick() {
    const mode = getBarcodeModeFromStorage();
    if (mode === "physical") {
      // Show "ready" indicator — wedge will fire into whatever input has focus.
      setScanReady(true);
      // Auto-dismiss after 10 seconds if no scan detected.
      const t = setTimeout(() => setScanReady(false), 10_000);
      return () => clearTimeout(t);
    } else {
      setModalOpen(true);
    }
  }

  return (
    <>
      <button
        type="button"
        onClick={handleClick}
        title={scanReady ? "Point scanner and pull trigger" : "Scan barcode"}
        className={[
          "inline-flex h-12 items-center gap-1.5 rounded-xl border px-3 text-xs font-semibold transition",
          scanReady
            ? "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-600/60 dark:bg-emerald-950/30 dark:text-emerald-300 animate-pulse"
            : "border-sky-200 bg-sky-50 text-sky-700 hover:bg-sky-100 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300",
          className,
        ].join(" ")}
      >
        <ScanLine className="h-4 w-4" />
        {scanReady ? "Ready…" : "Scan"}
      </button>

      {/* Physical-mode: small floating badge under the input */}
      {scanReady && (
        <div className="mt-1 flex items-center gap-1.5 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-medium text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-300">
          <ScanLine className="h-3.5 w-3.5 shrink-0 animate-pulse" />
          Point &amp; pull trigger — scanner will fill the field automatically.
          <button
            type="button"
            onClick={() => setScanReady(false)}
            className="ml-auto rounded-full p-0.5 hover:bg-emerald-100 dark:hover:bg-emerald-900"
            aria-label="Cancel scan ready state"
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      )}

      {/* Camera mode: live QR modal */}
      {modalOpen && (
        <BarcodeScannerModal
          title={modalTitle}
          onDetected={(code) => { onDetected(code); setModalOpen(false); setScanReady(false); }}
          onClose={() => setModalOpen(false)}
        />
      )}
    </>
  );
}

// ─── RBAC ──────────────────────────────────────────────────────────────────────

export type { UserRole };
export interface MockUser { name: string; role: UserRole }
export const DEFAULT_USER: MockUser = { name: "Warehouse Op", role: "operator" };
export const canEdit   = (r: UserRole) => isAdminRole(r);
export const canDelete = (r: UserRole) => isAdminRole(r);

// ─── Marketplace & Carriers ────────────────────────────────────────────────────

export const MARKETPLACES = ["amazon", "walmart", "ebay"] as const;
export type Marketplace = (typeof MARKETPLACES)[number];
export const MP_LABELS: Record<Marketplace, string> = { amazon: "Amazon", walmart: "Walmart", ebay: "eBay" };
export const CARRIERS = ["UPS", "FedEx", "USPS", "DHL", "OnTrac", "Amazon Logistics", "Other"];

// ─── Condition Tree ────────────────────────────────────────────────────────────

export type ConditionPrimary   = "sellable" | "wrong_item" | "empty_box" | "damaged" | "missing_parts" | "expired";
export type ConditionSecondary = "expired" | "missing_parts" | "damaged";
type SubOption = { value: string; label: string; sublabel: string };
export type ConditionPrimaryDef = {
  value: ConditionPrimary; label: string; sublabel: string; icon: React.ElementType;
  border: string; bg: string; iconColor: string; badge: string;
  sub?: SubOption[]; exclusive?: boolean; noSecondary?: boolean;
};

export const CONDITION_TREE: ConditionPrimaryDef[] = [
  { value: "sellable",      label: "Sellable",       sublabel: "Clean · ready to relist",    icon: PackageCheck, border: "border-emerald-400 dark:border-emerald-500/60", bg: "bg-emerald-50 dark:bg-emerald-950/40", iconColor: "text-emerald-600 dark:text-emerald-400", badge: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300", noSecondary: true },
  { value: "wrong_item",    label: "Wrong Item",     sublabel: "Incorrect product returned", icon: Package2,     border: "border-violet-400 dark:border-violet-500/60",  bg: "bg-violet-50 dark:bg-violet-950/40",  iconColor: "text-violet-600 dark:text-violet-400",  badge: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300",  noSecondary: true, sub: [{ value: "junk", label: "Junk / Garbage", sublabel: "Unrelated trash" }, { value: "different", label: "Different Product", sublabel: "Different ASIN" }] },
  { value: "empty_box",     label: "Empty Box",      sublabel: "No item inside",             icon: PackageX,     border: "border-rose-400 dark:border-rose-500/60",      bg: "bg-rose-50 dark:bg-rose-950/40",      iconColor: "text-rose-600 dark:text-rose-400",      badge: "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/40 dark:text-rose-300",              exclusive: true },
  { value: "damaged",       label: "Damaged",        sublabel: "Physical damage present",    icon: ShieldAlert,  border: "border-amber-400 dark:border-amber-500/60",    bg: "bg-amber-50 dark:bg-amber-950/40",    iconColor: "text-amber-600 dark:text-amber-400",    badge: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300",        sub: [{ value: "customer", label: "Customer", sublabel: "Returned damaged" }, { value: "carrier", label: "Carrier", sublabel: "Shipping damage" }, { value: "warehouse", label: "Warehouse", sublabel: "FC damage" }] },
  { value: "missing_parts", label: "Missing Parts",  sublabel: "Accessories / parts absent", icon: Minus,        border: "border-orange-400 dark:border-orange-500/60",  bg: "bg-orange-50 dark:bg-orange-950/40",  iconColor: "text-orange-600 dark:text-orange-400",  badge: "border-orange-200 bg-orange-50 text-orange-700 dark:border-orange-700/50 dark:bg-orange-950/40 dark:text-orange-300" },
  { value: "expired",       label: "Expired (FIFO)", sublabel: "Amazon FIFO violation",      icon: CalendarX2,   border: "border-orange-400 dark:border-orange-500/60",  bg: "bg-orange-50 dark:bg-orange-950/40",  iconColor: "text-orange-600 dark:text-orange-400",  badge: "border-orange-200 bg-orange-50 text-orange-700 dark:border-orange-700/50 dark:bg-orange-950/40 dark:text-orange-300" },
];

export const SECONDARIES_FOR: Partial<Record<ConditionPrimary, ConditionSecondary[]>> = {
  damaged:       ["expired", "missing_parts"],
  missing_parts: ["expired", "damaged"],
  expired:       ["damaged", "missing_parts"],
};

export const DAMAGE_SUB: SubOption[] = [
  { value: "customer", label: "Customer", sublabel: "Returned damaged" },
  { value: "carrier",  label: "Carrier",  sublabel: "Shipping damage" },
  { value: "warehouse",label: "Warehouse",sublabel: "FC damage" },
];

export const CONDITION_META: Record<string, { label: string; icon: React.ElementType; badge: string }> = {
  sellable:             { label: "Sellable",            icon: PackageCheck, badge: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300" },
  empty_box:            { label: "Empty Box",           icon: PackageX,     badge: "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/40 dark:text-rose-300" },
  damaged_customer:     { label: "Damaged (Customer)",  icon: ShieldAlert,  badge: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  damaged_carrier:      { label: "Damaged (Carrier)",   icon: ShieldAlert,  badge: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  damaged_warehouse:    { label: "Damaged (Warehouse)", icon: ShieldAlert,  badge: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  wrong_item_junk:      { label: "Junk Return",         icon: Package2,     badge: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300" },
  wrong_item_different: { label: "Wrong Product",       icon: Package2,     badge: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300" },
  missing_parts:        { label: "Missing Parts",       icon: Minus,        badge: "border-orange-200 bg-orange-50 text-orange-700 dark:border-orange-700/50 dark:bg-orange-950/40 dark:text-orange-300" },
  expired:              { label: "Expired (FIFO)",      icon: CalendarX2,   badge: "border-orange-200 bg-orange-50 text-orange-700 dark:border-orange-700/50 dark:bg-orange-950/40 dark:text-orange-300" },
  damaged_box:          { label: "Damaged Box",         icon: Package2,     badge: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  scratched:            { label: "Scratched",           icon: CircleDot,    badge: "border-slate-300 bg-slate-50 text-slate-700 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-300" },
  missing_item:         { label: "Missing Item",        icon: PackageX,     badge: "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/40 dark:text-rose-300" },
};

// ─── Photo categories ──────────────────────────────────────────────────────────

type PhotoCategoryDef = { id: string; label: string; hint: string; optional?: boolean; accentClass: string; iconColor: string; icon: React.ElementType };

const ALL_PHOTO_CATEGORIES: Record<string, PhotoCategoryDef> = {
  shipping_label:  { id: "shipping_label",  label: "Shipping Label & LPN",          hint: "Full label with LPN and tracking.",                          accentClass: "border-sky-200 dark:border-sky-800/50",       iconColor: "text-sky-600 dark:text-sky-400",    icon: Barcode },
  outer_box:       { id: "outer_box",       label: "Outer Box Condition",            hint: "All sides — how the package arrived.",                       accentClass: "border-slate-300 dark:border-slate-700",      iconColor: "text-muted-foreground", icon: Package2 },
  empty_interior:  { id: "empty_interior",  label: "Empty Box Interior",             hint: "Open interior — confirms no item is present.",               accentClass: "border-rose-200 dark:border-rose-800/50",     iconColor: "text-rose-600 dark:text-rose-400",   icon: PackageX },
  damage_closeup:  { id: "damage_closeup",  label: "Close-up of Damage",             hint: "Multiple angles showing every damage area.",                 accentClass: "border-amber-200 dark:border-amber-800/50",   iconColor: "text-amber-600 dark:text-amber-400", icon: ShieldAlert },
  incorrect_item:  { id: "incorrect_item",  label: "Incorrect Item",                 hint: "Wrong product — label and brand clearly visible.",           accentClass: "border-violet-200 dark:border-violet-800/50", iconColor: "text-violet-600 dark:text-violet-400", icon: Package2 },
  expiry_label:    { id: "expiry_label",    label: "Expiration Date Label",          hint: "Close-up of the expiry date on packaging.",                 accentClass: "border-orange-200 dark:border-orange-800/50", iconColor: "text-orange-600 dark:text-orange-400", icon: Calendar },
  fnsku_label:     { id: "fnsku_label",     label: "FNSKU / ASIN Barcode",           hint: "Product barcode (ASIN / UPC / FNSKU).",                      accentClass: "border-sky-200 dark:border-sky-800/50",       iconColor: "text-sky-600 dark:text-sky-400",    icon: Barcode },
  orphan_label:    { id: "orphan_label",    label: "Return label (optional)",        hint: "Only when this item is not linked to a package.",            optional: true,                                                accentClass: "border-sky-200 dark:border-sky-800/50",       iconColor: "text-sky-600 dark:text-sky-400",    icon: Barcode },
  pallet_manifest: { id: "pallet_manifest", label: "Pallet Manifest / Packing Slip", hint: "Supporting docs.", optional: true,                           accentClass: "border-border",      iconColor: "text-muted-foreground", icon: FileText },
};

// ─── Status configs ────────────────────────────────────────────────────────────

export const STATUS_CFG: Record<string, { label: string; icon: React.ElementType; cls: string }> = {
  received:         { label: "Received",         icon: Clock,          cls: "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300" },
  pending_evidence: { label: "Pending Evidence", icon: AlertTriangle,  cls: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/50 dark:text-amber-300" },
  ready_for_claim:  { label: "Ready for Claim",  icon: ClipboardCheck, cls: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/50 dark:text-violet-300" },
  completed:        { label: "Completed",        icon: CheckCircle2,   cls: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/60 dark:bg-emerald-950/50 dark:text-emerald-300" },
  flagged:          { label: "Flagged",           icon: AlertTriangle,  cls: "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/60 dark:bg-rose-950/50 dark:text-rose-300" },
};

export const PKG_STATUS_CFG: Record<PackageStatus, { label: string; cls: string }> = {
  open:       { label: "Open",        cls: "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300" },
  closed:     { label: "Closed",      cls: "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400" },
  suspicious: { label: "Discrepancy", cls: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/50 dark:text-amber-300" },
  submitted:  { label: "Submitted",   cls: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/50 dark:text-violet-300" },
};

export const PALLET_STATUS_CFG: Record<PalletStatus, { label: string; cls: string }> = {
  open:      { label: "Open",      cls: "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300" },
  closed:    { label: "Closed",    cls: "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400" },
  submitted: { label: "Submitted", cls: "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/50 dark:text-violet-300" },
};

// ─── CSS constants ─────────────────────────────────────────────────────────────

export const INPUT      = "h-14 w-full rounded-2xl border border-slate-200 bg-white px-4 text-base text-slate-900 placeholder:text-slate-400 transition focus:border-sky-400 focus:outline-none focus:ring-2 focus:ring-sky-400/30 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500 dark:focus:border-sky-500/60";
export const LABEL      = "mb-2 block text-sm font-semibold text-slate-700 dark:text-slate-300";
export const INPUT_SM   = "h-10 w-full rounded-xl border border-slate-200 bg-white px-3 text-sm text-slate-900 placeholder:text-slate-400 focus:border-sky-400 focus:outline-none focus:ring-2 focus:ring-sky-400/20 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500";
export const BTN_PRIMARY = "flex h-14 w-full items-center justify-center gap-2 rounded-2xl bg-sky-500 font-semibold text-white transition hover:bg-sky-600 active:scale-[0.98] disabled:opacity-50 dark:bg-sky-600 dark:hover:bg-sky-500";
/** Primary actions in drawer/modal footers — avoids `w-full` collapsing in flex layouts. */
export const BTN_PRIMARY_INLINE = "inline-flex h-14 shrink-0 min-w-[12rem] items-center justify-center gap-2 rounded-2xl bg-sky-500 px-6 font-semibold text-white transition hover:bg-sky-600 active:scale-[0.98] disabled:opacity-50 dark:bg-sky-600 dark:hover:bg-sky-500";
/** Modal / drawer footers — balanced h-10 primary + secondary (use with `flex flex-wrap items-center justify-end gap-2`). */
export const BTN_FOOTER_PRIMARY = "inline-flex h-10 min-w-[5.5rem] shrink-0 items-center justify-center gap-2 rounded-md bg-sky-500 px-4 text-sm font-semibold text-white shadow-sm transition hover:bg-sky-600 disabled:opacity-50 dark:bg-sky-600 dark:hover:bg-sky-500";
export const BTN_FOOTER_GHOST = "inline-flex h-10 min-w-[5.5rem] shrink-0 items-center justify-center gap-2 rounded-md border border-slate-200 bg-white px-4 text-sm font-medium text-slate-700 transition hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800";
export const BTN_GHOST   = "flex h-10 items-center gap-1.5 rounded-xl border border-slate-200 bg-white px-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:bg-slate-800";

/** Checkbox column — fixed width + centered so TableHead matches TableCell */
export const TH_CHK = "w-10 min-w-[2.5rem] px-0 py-3 text-center align-middle";
export const TD_CHK = "w-10 min-w-[2.5rem] px-0 py-3 align-middle";
export const CHK_FLEX = "flex w-full items-center justify-center";
/** Expand row chevron column (packages / pallets) */
export const TH_EXP = "w-8 min-w-[2rem] max-w-[2rem] px-2 py-3 align-middle";
export const TD_EXP = "w-8 min-w-[2rem] max-w-[2rem] px-2 py-3 align-middle";

// ─── Helpers ───────────────────────────────────────────────────────────────────

export function fmt(iso: string) { return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(new Date(iso)); }
export function generatePackageNumber() { return `PKG-${Date.now().toString(36).toUpperCase()}`; }
export function generatePalletNumber() { const d = new Date(); return `PLT-${d.getFullYear()}${String(d.getMonth()+1).padStart(2,"0")}${String(d.getDate()).padStart(2,"0")}-${Math.floor(Math.random()*900+100)}`; }

/** @deprecated Legacy tree builder — use {@link conditionsFromFlatCondition} for the item wizard. */
export function buildConditionsArray(primary: ConditionPrimary | "", primarySub: string, secondaries: ConditionSecondary[], secDamagedSub: string): string[] {
  if (!primary) return [];
  const out: string[] = [];
  const def = CONDITION_TREE.find((c) => c.value === primary);
  if (def?.sub?.length && primarySub) out.push(`${primary}_${primarySub}`);
  else out.push(primary);
  for (const sec of secondaries) {
    if (sec === "damaged" && secDamagedSub) out.push(`damaged_${secDamagedSub}`);
    else if (sec !== "damaged") out.push(sec);
  }
  return out;
}

/** @deprecated Use {@link conditionsFromKeys} with the multi-select wizard. */
export function conditionsFromFlatCondition(flat: string): string[] {
  if (!flat) return [];
  return conditionsFromKeys([flat]);
}

const NO_PHYSICAL_ITEM = new Set(["empty_box", "missing_item"]);
const REQUIRES_PHYSICAL_ITEM = new Set([
  "expired", "scratched", "damaged_customer", "wrong_item_different",
  "missing_parts", "damaged_box",
]);

export function chipDisabledForConditions(key: string, selected: string[]): boolean {
  if (selected.includes("sellable")) return key !== "sellable";
  if (NO_PHYSICAL_ITEM.has(key) && selected.some((k) => REQUIRES_PHYSICAL_ITEM.has(k))) return true;
  if (REQUIRES_PHYSICAL_ITEM.has(key) && selected.some((k) => NO_PHYSICAL_ITEM.has(k))) return true;
  return false;
}

export function toggleConditionKey(selected: string[], key: string): string[] {
  const s = new Set(selected);
  if (s.has(key)) {
    s.delete(key);
    return [...s];
  }
  if (key === "sellable") return ["sellable"];
  s.delete("sellable");
  if (NO_PHYSICAL_ITEM.has(key)) REQUIRES_PHYSICAL_ITEM.forEach((k) => s.delete(k));
  if (REQUIRES_PHYSICAL_ITEM.has(key)) NO_PHYSICAL_ITEM.forEach((k) => s.delete(k));
  s.add(key);
  return [...s];
}

/** Multi-select condition keys → DB `conditions` array (sellable stays exclusive). */
export function conditionsFromKeys(keys: string[]): string[] {
  if (!keys?.length) return [];
  if (keys.includes("sellable")) return ["sellable"];
  return [...keys];
}

/** Toggleable chips — multiple selections allowed (e.g. Damaged Box + Missing Parts). */
export const CONDITION_CHIP_DEFS: {
  key: string;
  label: string;
  sublabel: string;
  icon: React.ElementType;
  border: string;
  bg: string;
  iconColor: string;
}[] = [
  { key: "damaged_box", label: "Damaged Box", sublabel: "Carton / retail box", icon: Package2, border: "border-amber-400 dark:border-amber-500/60", bg: "bg-amber-50 dark:bg-amber-950/40", iconColor: "text-amber-600 dark:text-amber-400" },
  { key: "damaged_customer", label: "Damaged Product", sublabel: "Unit damage", icon: ShieldAlert, border: "border-amber-400 dark:border-amber-500/60", bg: "bg-amber-50 dark:bg-amber-950/40", iconColor: "text-amber-600 dark:text-amber-400" },
  { key: "scratched", label: "Scratched", sublabel: "Surface wear", icon: CircleDot, border: "border-slate-400 dark:border-slate-500/60", bg: "bg-slate-50 dark:bg-slate-950/40", iconColor: "text-slate-600 dark:text-slate-400" },
  { key: "wrong_item_different", label: "Wrong Item", sublabel: "Not what was ordered", icon: Package2, border: "border-violet-400 dark:border-violet-500/60", bg: "bg-violet-50 dark:bg-violet-950/40", iconColor: "text-violet-600 dark:text-violet-400" },
  { key: "expired", label: "Expired", sublabel: "FIFO / date issue", icon: CalendarX2, border: "border-orange-400 dark:border-orange-500/60", bg: "bg-orange-50 dark:bg-orange-950/40", iconColor: "text-orange-600 dark:text-orange-400" },
  { key: "missing_parts", label: "Missing Parts", sublabel: "Incomplete", icon: Minus, border: "border-orange-400 dark:border-orange-500/60", bg: "bg-orange-50 dark:bg-orange-950/50", iconColor: "text-orange-600 dark:text-orange-400" },
  { key: "empty_box", label: "Empty Box", sublabel: "No product inside", icon: PackageX, border: "border-rose-400 dark:border-rose-500/60", bg: "bg-rose-50 dark:bg-rose-950/40", iconColor: "text-rose-600 dark:text-rose-400" },
  { key: "missing_item", label: "Missing Item", sublabel: "Unit not in return", icon: PackageX, border: "border-rose-400 dark:border-rose-500/60", bg: "bg-rose-50 dark:bg-rose-950/40", iconColor: "text-rose-600 dark:text-rose-400" },
  { key: "sellable", label: "Sellable / OK", sublabel: "Good to resell", icon: PackageCheck, border: "border-emerald-400 dark:border-emerald-500/60", bg: "bg-emerald-50 dark:bg-emerald-950/40", iconColor: "text-emerald-600 dark:text-emerald-400" },
];

/** @deprecated Alias for {@link CONDITION_CHIP_DEFS}. */
export const FLAT_CONDITION_CARDS = CONDITION_CHIP_DEFS;

export function getCategoriesForConditions(
  conditions: string[],
  ctx?: { hasPackageLink?: boolean; orphanLpn?: boolean; packageInheritsBoxPhotos?: boolean; looseItem?: boolean },
): PhotoCategoryDef[] {
  if (ctx?.looseItem) return [];
  if (!conditions.length || conditions.includes("sellable")) return [];
  const ids = new Set<string>();
  // Outer box / return label are inherited from Package only — never prompt for per-item box uploads.
  if (!ctx?.packageInheritsBoxPhotos) {
    ids.add("fnsku_label");
  }
  for (const c of conditions) {
    if (c === "empty_box" || c === "missing_item") ids.add("empty_interior");
    if (c === "damaged_box" || c.startsWith("damaged_") || c === "scratched") ids.add("damage_closeup");
    if (c.startsWith("wrong_item_")) ids.add("incorrect_item");
    // Expired: dedicated expiry URL slot in `photo_evidence.expiry_url` — avoid duplicate SmartCamera category.
  }
  if (ctx?.orphanLpn && !ctx?.hasPackageLink) ids.add("orphan_label");
  return [...ids].map((id) => ALL_PHOTO_CATEGORIES[id]).filter(Boolean);
}

// ─── Mock OCR ──────────────────────────────────────────────────────────────────

type PalletOcrResult  = { pallet_number: string; total_items: number; confidence: number };
type PackageOcrResult = { expected_item_count: number; carrier_name: string; tracking_number: string; confidence: number };

export async function mockPalletOcr(_f: File): Promise<{ ok: boolean; data?: PalletOcrResult; error?: string }> {
  await new Promise((r) => setTimeout(r, 2400));
  if (Math.random() < 0.12) return { ok: false, error: "Handwriting unclear — please enter manually." };
  const d = new Date(); const ds = `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,"0")}${String(d.getDate()).padStart(2,"0")}`;
  return { ok: true, data: { pallet_number: `PLT-${ds}-${Math.floor(Math.random()*900+100)}`, total_items: Math.floor(Math.random()*45+6), confidence: 0.76 + Math.random()*0.22 } };
}

export async function mockPackageOcr(_f: File): Promise<{ ok: boolean; data?: PackageOcrResult; error?: string }> {
  await new Promise((r) => setTimeout(r, 2200));
  if (Math.random() < 0.1) return { ok: false, error: "Could not read packing slip clearly." };
  const carriers = ["UPS", "FedEx", "USPS", "DHL", "Amazon Logistics"];
  const tracking = `1Z${Math.random().toString(36).toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 10)}`;
  return { ok: true, data: { expected_item_count: Math.floor(Math.random()*20+2), carrier_name: carriers[Math.floor(Math.random()*carriers.length)], tracking_number: tracking, confidence: 0.78 + Math.random()*0.2 } };
}

/** Mock line items for packing-slip reconciliation — shared by create package + package edit. */
export async function mockManifestLineItems(_file: File): Promise<SlipExpectedItem[]> {
  await new Promise((r) => setTimeout(r, 1400));
  return [
    { barcode: "111", name: "Item 111", expected_qty: 1 },
    { barcode: "222", name: "Item 222", expected_qty: 2 },
  ];
}

/** One line from an AI-read packing slip (barcode + display name + optional qty from Vision JSON). */
export type SlipExpectedItem = { barcode: string; name: string; expected_qty?: number };

type SlipOcrResult = { items: SlipExpectedItem[]; confidence: number };

/**
 * Simulates server-side OCR on a captured packing-slip image.
 * Runs exactly 2s so the UI can show "AI Reading Slip..." — then returns expected line-items.
 */
export async function simulatePackingSlipOcr(_file: File): Promise<{ ok: boolean; data?: SlipOcrResult; error?: string }> {
  await new Promise((r) => setTimeout(r, 2000));
  if (Math.random() < 0.05) return { ok: false, error: "Could not read packing slip — try a clearer photo." };
  const pool: SlipExpectedItem[] = [
    { barcode: "12345", name: "Item A", expected_qty: 1 },
    { barcode: "67890", name: "Item B", expected_qty: 1 },
    { barcode: "B08N5WRWNW", name: "Echo Dot (4th Gen)", expected_qty: 1 },
    { barcode: "UPC-44100210", name: "USB-C Cable 6ft", expected_qty: 1 },
    { barcode: "SKU-HDMI-4K", name: "4K HDMI Cable", expected_qty: 1 },
  ];
  const count = Math.floor(Math.random() * 2) + 2;
  const shuffled = [...pool].sort(() => Math.random() - 0.5).slice(0, count);
  return { ok: true, data: { items: shuffled, confidence: 0.88 + Math.random() * 0.1 } };
}

function normalizeManifestToken(s: string | null | undefined): string {
  return String(s ?? "").trim().toLowerCase();
}

/** Normalized barcode / catalog fields only (no free-text item title) — for substring fallbacks. */
function itemBarcodeFields(it: ReturnRecord): string[] {
  const raw = [
    it.asin,
    it.fnsku,
    it.sku,
    it.lpn,
    (it as { product_identifier?: string | null }).product_identifier,
    (it as { scanned_barcode?: string | null }).scanned_barcode,
  ];
  return raw.map((x) => normalizeManifestToken(x)).filter(Boolean);
}

/** All tokens that may exactly match a manifest barcode or slip line (includes item title). */
function itemManifestIdentifierSet(it: ReturnRecord): Set<string> {
  const out = new Set<string>(itemBarcodeFields(it));
  const t = normalizeManifestToken(it.item_name);
  if (t) out.add(t);
  return out;
}

/** Tokens from one manifest row (barcode + slip name). */
function manifestLineTokens(exp: SlipExpectedItem): string[] {
  const raw = [exp.barcode, exp.name];
  const out = new Set<string>();
  for (const x of raw) {
    const t = normalizeManifestToken(x == null ? "" : String(x));
    if (t) out.add(t);
  }
  return [...out];
}

/** Whether a scanned item matches an expected slip line — trimmed, case-insensitive; exact first, then light OCR fallbacks. */
function physicalItemMatchesExpectedLine(it: ReturnRecord, exp: SlipExpectedItem): boolean {
  const itemIds = itemManifestIdentifierSet(it);
  const lineToks = manifestLineTokens(exp);
  for (const lt of lineToks) {
    if (itemIds.has(lt)) return true;
  }
  const name = (it.item_name ?? "").toLowerCase();
  const bc = normalizeManifestToken(String(exp.barcode ?? ""));
  const nameChunk = normalizeManifestToken(exp.name).split(/\s+/).slice(0, 3).join(" ");
  const codes = itemBarcodeFields(it);
  return (
    (bc.length >= 2 && (name.includes(bc) || codes.some((id) => id.includes(bc) || bc.includes(id)))) ||
    (nameChunk.length >= 3 && name.includes(nameChunk))
  );
}

type LabelOcrResult = { lpn: string; scan_code?: string; marketplace: string; confidence: number };

export async function mockLabelOcr(_f: File): Promise<{ ok: boolean; data?: LabelOcrResult; error?: string }> {
  await new Promise((r) => setTimeout(r, 1900));
  if (Math.random() < 0.08) return { ok: false, error: "Label unclear — please enter manually." };
  return {
    ok: true,
    data: {
      lpn:        `LPN${Math.random().toString(36).toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 9)}`,
      scan_code: `B0${String(Math.floor(Math.random() * 1e7)).padStart(7, "0")}`,
      marketplace: "amazon",
      confidence:  0.82 + Math.random() * 0.16,
    },
  };
}

// Re-export OrgSettings defaults for use in page.tsx
export const DEFAULT_ORG_SETTINGS: OrgSettings = {
  is_ai_label_ocr_enabled: false,
  is_ai_packing_slip_ocr_enabled: false,
};

// ─── Wizard State ──────────────────────────────────────────────────────────────

export interface WizardInheritedContext { packageId?: string; packageLabel?: string; palletId?: string; palletLabel?: string }

export type WizardState = {
  lpn: string;
  /** ASIN / UPC / FNSKU — required product identity at item level. */
  product_identifier: string;
  marketplace: Marketplace | ""; item_name: string;
  /** Amazon Standard Identification Number */
  asin: string;
  /** Fulfillment Network SKU */
  fnsku: string;
  /** Seller SKU — Amazon warehouse / Seller Central (MSKU) */
  sku: string;
  /** Multi-select condition keys — see {@link CONDITION_CHIP_DEFS}. */
  condition_keys: string[];
  expiration_date: string; batch_number: string;
  /** Items link to Packages only. Pallet is inherited from the Package. */
  package_link_id: string;
  /** When true, item has no parent package (loose/orphan flow). */
  loose_item: boolean;
  notes: string; photos: Record<string, File[]>;
  /** Claim evidence photo URLs — merged into `photo_evidence` JSONB on submit (not standalone DB columns). */
  photo_item_url: string;
  photo_expiry_url: string;
  /** Loose-item optional return label (no package to inherit from). */
  photo_return_label_url: string;
  /** Extra gallery URLs in `photo_evidence.urls` (incident-photos bucket). */
  evidence_gallery_urls: string[];
  /** Connected store UUID — links this item to a specific store account. */
  store_id: string;
  /** Optional Amazon order ID — stored on `returns.order_id` and `claim_submissions.source_payload.amazon_order_id`. */
  amazon_order_id: string;
  /** Product catalog lookup — when `unknown`, Step 1 allows Next without a resolved ASIN/UPC (manual item name). */
  catalog_resolution: "idle" | "loading" | "local" | "amazon" | "unknown";
};

export const EMPTY_WIZARD: WizardState = {
  lpn: "", product_identifier: "", marketplace: "", item_name: "",
  asin: "", fnsku: "", sku: "",
  condition_keys: [],
  expiration_date: "", batch_number: "", package_link_id: "",
  loose_item: false,
  notes: "", photos: {},
  photo_item_url: "", photo_expiry_url: "", photo_return_label_url: "",
  evidence_gallery_urls: [],
  store_id: "",
  amazon_order_id: "",
  catalog_resolution: "idle",
};

// ─── Toast ─────────────────────────────────────────────────────────────────────

export type ToastKind = "success" | "error" | "warning";
interface Toast { id: number; kind: ToastKind; msg: string }
export function useToast() {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const show = useCallback((msg: string, kind: ToastKind = "success") => {
    const id = Date.now();
    setToasts((p) => [...p, { id, kind, msg }]);
    setTimeout(() => setToasts((p) => p.filter((t) => t.id !== id)), 5000);
  }, []);
  return { toasts, show };
}
export function ToastStack({ toasts }: { toasts: Toast[] }) {
  const CLS: Record<ToastKind, string> = { success: "bg-emerald-600 text-white", error: "bg-rose-600 text-white", warning: "bg-amber-500 text-white" };
  return (
    <div className="pointer-events-none fixed bottom-6 left-1/2 z-[600] flex -translate-x-1/2 flex-col items-center gap-2">
      {toasts.map((t) => (
        <div key={t.id} className={`pointer-events-auto flex max-w-sm items-center gap-2 rounded-2xl px-5 py-3 text-sm font-semibold shadow-2xl ${CLS[t.kind]}`}>
          {t.kind === "success" && <CheckCircle2 className="h-4 w-4 shrink-0" />}
          {t.kind === "error"   && <XCircle className="h-4 w-4 shrink-0" />}
          {t.kind === "warning" && <AlertTriangle className="h-4 w-4 shrink-0" />}
          {t.msg}
        </div>
      ))}
    </div>
  );
}

// ─── Drawer Content Type ───────────────────────────────────────────────────────

export type DrawerContent =
  | { type: "item";    record: ReturnRecord  }
  | { type: "package"; record: PackageRecord }
  | { type: "pallet";  record: PalletRecord  };

// ─── Badge components ──────────────────────────────────────────────────────────

export function ConditionBadge({ value }: { value: string }) {
  const m = CONDITION_META[value];
  if (!m) return <span className="text-[10px] text-slate-400 capitalize">{value}</span>;
  const Icon = m.icon;
  return <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-semibold ${m.badge}`}><Icon className="h-3 w-3" />{m.label}</span>;
}
export function StatusBadge({ status }: { status: string }) {
  const cfg = STATUS_CFG[status] ?? { label: status, icon: Clock, cls: "border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400" };
  const Icon = cfg.icon;
  return <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-[11px] font-semibold ${cfg.cls}`}><Icon className="h-3 w-3" />{cfg.label}</span>;
}
export function PkgStatusBadge({ status }: { status: PackageStatus }) {
  const cfg = PKG_STATUS_CFG[status];
  return <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-semibold ${cfg.cls}`}>{cfg.label}</span>;
}
export function PalletStatusBadge({ status }: { status: PalletStatus }) {
  const cfg = PALLET_STATUS_CFG[status];
  return <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-semibold ${cfg.cls}`}>{cfg.label}</span>;
}
export function RoleBadge({ user, onToggle }: { user: MockUser; onToggle: () => void }) {
  const adminish = isAdminRole(user.role);
  return (
    <button onClick={onToggle} title="Toggle role (demo)"
      className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-bold transition ${adminish ? "border-violet-300 bg-violet-50 text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/50 dark:text-violet-300" : "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300"}`}>
      {user.role === "super_admin" ? <Sparkles className="h-3 w-3" /> : adminish ? <ShieldCheck className="h-3 w-3" /> : <User className="h-3 w-3" />}
      {user.role.toUpperCase()}
    </button>
  );
}
export function StepIndicator({ step, total }: { step: number; total: number }) {
  return (
    <div className="flex items-center gap-2">
      {Array.from({ length: total }, (_, i) => (
        <React.Fragment key={i}>
          <div className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-bold transition-all ${i+1===step ? "bg-sky-500 text-white shadow-md" : i+1<step ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-400" : "bg-slate-100 text-slate-400 dark:bg-slate-800 dark:text-slate-500"}`}>
            {i+1 < step ? <CheckCircle2 className="h-4 w-4" /> : i+1}
          </div>
          {i < total-1 && <div className={`h-0.5 w-8 rounded-full ${i+1<step ? "bg-emerald-300 dark:bg-emerald-700" : "bg-muted"}`} />}
        </React.Fragment>
      ))}
    </div>
  );
}

// ─── SortButton ────────────────────────────────────────────────────────────────

export function SortButton({ field, label, sortField, sortAsc, onSort }: {
  field: string; label: string; sortField: string; sortAsc: boolean; onSort: (f: string) => void;
}) {
  const active = sortField === field;
  return (
    <button onClick={() => onSort(field)} className="flex items-center gap-1 text-xs font-bold uppercase tracking-wide text-slate-400 hover:text-slate-700 dark:hover:text-slate-200">
      {label}
      {active ? (sortAsc ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />) : <span className="h-3 w-3 opacity-40">↕</span>}
    </button>
  );
}

/** Display label for `returns.marketplace` (Source column / filters). */
function formatMarketplaceSource(marketplace: string): string {
  const m = marketplace?.toLowerCase();
  if (m && (MARKETPLACES as readonly string[]).includes(m)) return MP_LABELS[m as Marketplace];
  return marketplace?.trim() || "—";
}

/** Map connected store `platform` string to `returns.marketplace` enum. */
export function platformToMarketplace(platform: string): Marketplace {
  const p = platform.toLowerCase();
  if (p.includes("walmart")) return "walmart";
  if (p.includes("ebay")) return "ebay";
  return "amazon";
}

/** Opens the marketplace catalog search with the given product code (prefills site search). */
export function marketplaceSearchUrl(platformOrMarketplace: string | null | undefined, query: string): string | null {
  return marketplaceSearchUrlLib(platformOrMarketplace, query);
}

/** Amazon search results for a product code (ASIN / FNSKU / SKU) — paste-friendly in the site search box. */
export function amazonProductSearchUrl(query: string): string | null {
  const q = query.trim();
  if (!q) return null;
  return `https://www.amazon.com/s?k=${encodeURIComponent(q)}`;
}

// ─── FEFO Expiry Status ────────────────────────────────────────────────────────
//  criticalDays / warningDays are dynamic — fetched from workspace_settings JSONB.
//  Defaults: critical ≤ 30d (🔴), warning ≤ 90d (🟡), OK > 90d (🟢).

function getExpiryStatus(
  dateStr:      string | null | undefined,
  criticalDays: number = 30,
  warningDays:  number = 90,
): {
  label: string; daysLabel: string; cls: string; dotCls: string;
} | null {
  if (!dateStr) return null;
  const now = new Date(); now.setHours(0, 0, 0, 0);
  const exp = new Date(dateStr); exp.setHours(0, 0, 0, 0);
  const diffDays = Math.floor((exp.getTime() - now.getTime()) / 86400000);
  if (diffDays < 0)              return { label: "Expired",  daysLabel: `${Math.abs(diffDays)}d ago`, cls: "border-red-200 bg-red-50 text-red-700 dark:border-red-700/60 dark:bg-red-950/50 dark:text-red-300",       dotCls: "bg-red-500"     };
  if (diffDays <= criticalDays)  return { label: "🔴 Critical", daysLabel: `${diffDays}d left`,       cls: "border-red-200 bg-red-50 text-red-700 dark:border-red-700/60 dark:bg-red-950/50 dark:text-red-300",       dotCls: "bg-red-500"     };
  if (diffDays <= warningDays)   return { label: "🟡 Warning",  daysLabel: `${diffDays}d left`,       cls: "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/50 dark:text-amber-300", dotCls: "bg-amber-400" };
  return                                { label: "🟢 OK",       daysLabel: `${diffDays}d left`,       cls: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/60 dark:bg-emerald-950/50 dark:text-emerald-300", dotCls: "bg-emerald-500" };
}

// ─── Platform Badge ────────────────────────────────────────────────────────────

function PlatformBadge({ platform }: { platform?: string | null }) {
  const p = platform?.toLowerCase() ?? "";
  const cfg: Record<string, { label: string; cls: string }> = {
    amazon:  { label: "AMZ",  cls: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300" },
    walmart: { label: "WMT",  cls: "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300" },
    ebay:    { label: "eBay", cls: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300" },
    target:  { label: "TGT",  cls: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300" },
  };
  const c = cfg[p] ?? { label: p.toUpperCase().slice(0, 3) || "?", cls: "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300" };
  return <span className={`rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide ${c.cls}`}>{c.label}</span>;
}

// ─── Evidence Photo Thumbnail ──────────────────────────────────────────────────

/** One-tap copy for tracking / LPN / pallet / package numbers (tables & drawers). Parent should use `group` for hover-reveal. */
export function InlineCopy({
  value,
  label,
  onToast,
  stopPropagation,
  revealOnHover = true,
  className = "inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-white text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800",
}: {
  value: string;
  label?: string;
  onToast?: (msg: string, kind?: ToastKind) => void;
  stopPropagation?: boolean;
  /** When true (default), hide the icon until the parent `group` is hovered (lazy mode). */
  revealOnHover?: boolean;
  className?: string;
}) {
  const v = value.trim();
  if (!v) return null;
  const hoverCls = revealOnHover
    ? "opacity-0 transition-opacity duration-150 group-hover:opacity-100 focus-visible:opacity-100"
    : "";
  return (
    <button
      type="button"
      title={label ? `Copy ${label}` : "Copy"}
      className={`${className} ${hoverCls}`.trim()}
      onClick={(e) => {
        if (stopPropagation) e.stopPropagation();
        void navigator.clipboard
          .writeText(v)
          .then(() => onToast?.(`Copied ${label ?? ""}`.trim(), "success"))
          .catch(() => onToast?.("Copy failed", "error"));
      }}
    >
      <Copy className="h-3.5 w-3.5" />
    </button>
  );
}

function PhotoThumb({ url, alt = "Evidence photo" }: { url: string; alt?: string }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        type="button"
        onClick={(e) => { e.stopPropagation(); setOpen(true); }}
        className="group relative inline-block h-8 w-8 overflow-hidden rounded-lg border border-slate-200 shadow-sm hover:ring-2 hover:ring-sky-400 dark:border-slate-700"
        title="View evidence photo"
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={url} alt={alt} className="h-full w-full object-cover transition group-hover:scale-110" />
        <span className="absolute inset-0 flex items-center justify-center bg-black/0 opacity-0 transition group-hover:bg-black/20 group-hover:opacity-100">
          <ZoomIn className="h-3.5 w-3.5 text-white" />
        </span>
      </button>

      {/* Full-size lightbox */}
      {open && (
        <div
          className="fixed inset-0 z-[500] flex items-center justify-center bg-black/75 p-4 backdrop-blur-sm"
          onClick={() => setOpen(false)}
        >
          <div className="relative max-h-[90vh] max-w-[90vw]" onClick={(e) => e.stopPropagation()}>
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img src={url} alt={alt} className="max-h-[85vh] max-w-[88vw] rounded-2xl object-contain shadow-2xl" />
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="absolute -right-3 -top-3 flex h-8 w-8 items-center justify-center rounded-full bg-white shadow-lg hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700"
            >
              <X className="h-4 w-4 text-slate-700 dark:text-slate-300" />
            </button>
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="absolute -bottom-3 left-1/2 -translate-x-1/2 flex items-center gap-1.5 rounded-full bg-white px-4 py-1.5 text-xs font-semibold text-slate-700 shadow-lg hover:bg-slate-50 dark:bg-slate-800 dark:text-slate-300"
            >
              <Eye className="h-3.5 w-3.5" /> Open full size
            </a>
          </div>
        </div>
      )}
    </>
  );
}

function compareSortKeys(a: string | number, b: string | number, asc: boolean): number {
  if (typeof a === "number" && typeof b === "number") return asc ? a - b : b - a;
  const sa = String(a);
  const sb = String(b);
  return asc ? sa.localeCompare(sb) : sb.localeCompare(sa);
}

function sortKeyItem(
  r: ReturnRecord,
  field: string,
  pkgMap: Map<string, PackageRecord>,
  pltMap: Map<string, PalletRecord>,
): string | number {
  const linkedPkg = r.package_id ? pkgMap.get(r.package_id) : undefined;
  const linkedPlt = r.pallet_id ? pltMap.get(r.pallet_id) : undefined;
  const trackEff = (r.inherited_tracking_number ?? linkedPkg?.tracking_number ?? "").trim();
  switch (field) {
    case "product_identifier": return (r.asin ?? r.fnsku ?? r.sku ?? "").toLowerCase();
    case "inherited_tracking_number":
    case "tracking_effective": return trackEff.toLowerCase();
    case "lpn": return (r.lpn ?? "").toLowerCase();
    case "marketplace":
    case "store_name": return (r.stores?.name ?? r.marketplace ?? "").toLowerCase();
    case "item_name": return r.item_name.toLowerCase();
    case "item_conditions": return [...r.conditions].sort().join(",");
    case "status": return r.status.toLowerCase();
    case "hierarchy_key": {
      if (!linkedPkg) return "\uffff";
      const pltPart = linkedPlt?.pallet_number ?? "";
      return `${linkedPkg.package_number}\0${pltPart}`.toLowerCase();
    }
    case "expiration_date": return r.expiration_date ? r.expiration_date : "\uffff";
    case "created_by": return operatorDisplayLabel(r).toLowerCase();
    case "created_at": return new Date(r.created_at).getTime();
    default:
      return String((r as unknown as Record<string, unknown>)[field] ?? "").toLowerCase();
  }
}

function sortKeyPackage(p: PackageRecord, field: string): string | number {
  switch (field) {
    case "package_number": return p.package_number.toLowerCase();
    case "carrier_name": return (p.carrier_name ?? "").toLowerCase();
    case "tracking_number": return (p.tracking_number ?? "").toLowerCase();
    case "carrier_tracking": return `${p.carrier_name ?? ""}\0${p.tracking_number ?? ""}`.toLowerCase();
    case "actual_item_count": return p.actual_item_count;
    case "expected_item_count": return p.expected_item_count;
    case "pkg_items_sort": return p.actual_item_count * 1_000_000 + p.expected_item_count;
    case "status": return p.status.toLowerCase();
    case "store_name": return (p.stores?.name ?? "").toLowerCase();
    case "created_by": return operatorDisplayLabel(p).toLowerCase();
    case "created_at": return new Date(p.created_at).getTime();
    default: return String((p as unknown as Record<string, unknown>)[field] ?? "").toLowerCase();
  }
}

type PalletSortRow = PalletRecord & { _rollupPkgs: number; _rollupItems: number };

function sortKeyPallet(p: PalletSortRow, field: string): string | number {
  switch (field) {
    case "pallet_number": return p.pallet_number.toLowerCase();
    case "rollup_pkgs": return p._rollupPkgs;
    case "rollup_items": return p._rollupItems;
    case "status": return p.status.toLowerCase();
    case "store_name": return (p.stores?.name ?? "").toLowerCase();
    case "created_by": return operatorDisplayLabel(p).toLowerCase();
    case "created_at": return new Date(p.created_at).getTime();
    default: return String((p as unknown as Record<string, unknown>)[field] ?? "").toLowerCase();
  }
}

// ─── ComboboxField ─────────────────────────────────────────────────────────────

interface ComboboxOption {
  id: string;
  label: string;
  sublabel?: string;
  /** Scan / search keys (e.g. tracking #, RMA) — never used as `value`; only `id` is the DB UUID. */
  tracking?: string | null;
  rma?: string | null;
}
export function ComboboxField({ label, hint, icon: Icon, options, value, onChange, onClear, placeholder, onCreateNew, createLabel }: {
  label: string; hint?: string; icon?: React.ElementType;
  options: ComboboxOption[]; value: string;
  onChange: (id: string) => void; onClear?: () => void;
  placeholder: string; onCreateNew?: () => void; createLabel?: string;
}) {
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const selected = options.find((o) => o.id === value);
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return options;
    return options.filter((o) => {
      const labelOk = o.label.toLowerCase().includes(q);
      const subOk = (o.sublabel ?? "").toLowerCase().includes(q);
      const tr = (o.tracking ?? "").trim().toLowerCase();
      const rma = (o.rma ?? "").trim().toLowerCase();
      return labelOk || subOk || (tr && tr.includes(q)) || (rma && rma.includes(q));
    });
  }, [options, search]);

  useEffect(() => {
    const raw = search.trim();
    if (!raw) return;
    const key = raw.toLowerCase();
    const exact = options.find((o) => {
      const l = o.label.toLowerCase();
      const sub = (o.sublabel ?? "").toLowerCase();
      const tr = (o.tracking ?? "").trim().toLowerCase();
      const rma = (o.rma ?? "").trim().toLowerCase();
      return l === key || sub === key || (!!tr && tr === key) || (!!rma && rma === key);
    });
    if (exact) { onChange(exact.id); setSearch(""); setOpen(false); }
  }, [search, options, onChange]);

  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  return (
    <div ref={ref} className="relative">
      <label className={LABEL}>{label}{hint && <span className="ml-2 text-xs font-normal text-slate-400">{hint}</span>}</label>
      <div className="relative">
        {Icon && <Icon className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />}
        <input type="text" autoComplete="off" className={`${INPUT} ${Icon ? "pl-11" : ""} pr-10`}
          value={open ? search : (selected?.label ?? "")}
          onChange={(e) => { setSearch(e.target.value); setOpen(true); }}
          onFocus={() => { setOpen(true); setSearch(""); }} placeholder={placeholder}
        />
        {value && onClear
          ? <button type="button" onClick={() => { onClear(); setSearch(""); setOpen(false); }} className="absolute right-3 top-1/2 -translate-y-1/2 rounded-full p-1 text-slate-400 hover:text-rose-500"><X className="h-4 w-4" /></button>
          : <ChevronDown className="pointer-events-none absolute right-4 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
        }
      </div>
      {open && (
        <div className="absolute z-50 mt-1 w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-900">
          <div className="max-h-52 overflow-y-auto divide-y divide-slate-100 dark:divide-slate-800">
            {filtered.length === 0 ? <p className="px-4 py-4 text-center text-sm text-slate-400">No matches</p>
              : filtered.map((opt) => (
                <button key={opt.id} type="button" onClick={() => { onChange(opt.id); setSearch(""); setOpen(false); }}
                  className={`flex w-full flex-col px-4 py-3 text-left transition hover:bg-sky-50 dark:hover:bg-sky-950/30 ${value === opt.id ? "bg-sky-50/50 dark:bg-sky-950/20" : ""}`}>
                  <span className={`text-sm font-semibold ${value === opt.id ? "text-sky-600 dark:text-sky-400" : "text-foreground"}`}>{opt.label}</span>
                  {opt.sublabel && <span className="text-xs text-slate-400">{opt.sublabel}</span>}
                </button>
              ))}
          </div>
          {onCreateNew && (
            <div className="border-t border-border">
              <button type="button" onClick={() => { onCreateNew(); setOpen(false); }}
                className="flex w-full items-center gap-2 px-4 py-3 text-sm font-semibold text-sky-600 transition hover:bg-sky-50 dark:text-sky-400 dark:hover:bg-sky-950/30">
                <Plus className="h-4 w-4" />{createLabel ?? "Create new…"}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── RowActionMenu ─────────────────────────────────────────────────────────────

const ROW_ACTION_MENU_W = 176;

export function RowActionMenu({ onView, onEdit, onDelete }: {
  onView?: () => void; onEdit?: () => void; onDelete?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const [coords, setCoords] = useState<{ top: number; left: number } | null>(null);

  const placeMenu = useCallback(() => {
    const btn = btnRef.current;
    const menu = menuRef.current;
    if (!btn) return;
    const rect = btn.getBoundingClientRect();
    const menuH = menu?.offsetHeight ?? 148;
    const gap = 8;
    let top = rect.bottom + gap;
    if (top + menuH > window.innerHeight - gap && rect.top - menuH - gap > gap) {
      top = rect.top - menuH - gap;
    }
    let left = rect.right - ROW_ACTION_MENU_W;
    left = Math.max(gap, Math.min(left, window.innerWidth - ROW_ACTION_MENU_W - gap));
    setCoords({ top, left });
  }, []);

  useEffect(() => {
    if (!open) setCoords(null);
  }, [open]);

  useLayoutEffect(() => {
    if (!open) return;
    placeMenu();
    const id = requestAnimationFrame(() => placeMenu());
    return () => cancelAnimationFrame(id);
  }, [open, onView, onEdit, onDelete, placeMenu]);

  useEffect(() => {
    if (!open) return;
    const onScroll = () => setOpen(false);
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", placeMenu);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", placeMenu);
    };
  }, [open, placeMenu]);

  useEffect(() => {
    const h = (e: MouseEvent) => {
      const t = e.target as Node;
      if (btnRef.current?.contains(t)) return;
      if (menuRef.current?.contains(t)) return;
      setOpen(false);
    };
    if (open) document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, [open]);

  const menuPanel = (
    <div
      ref={menuRef}
      role="menu"
      className="fixed z-[220] w-44 overflow-hidden rounded-2xl border border-slate-200 bg-white py-1 shadow-xl dark:border-slate-700 dark:bg-slate-900"
      style={coords ? { top: coords.top, left: coords.left } : { top: -9999, left: 0, visibility: "hidden" as const }}
    >
      {onView && <button type="button" onClick={() => { onView(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"><Eye className="h-4 w-4 text-slate-400" />View Detail</button>}
      {onEdit && <button type="button" onClick={() => { onEdit(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"><Pencil className="h-4 w-4 text-slate-400" />Edit</button>}
      {onDelete && (
        <>
          <div className="my-1 border-t border-border" />
          <button type="button" onClick={() => { void onDelete(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-rose-600 transition hover:bg-rose-50 dark:text-rose-400 dark:hover:bg-rose-950/30"><Trash2 className="h-4 w-4" />Delete</button>
        </>
      )}
    </div>
  );

  return (
    <>
      <div className="relative" onClick={(e) => e.stopPropagation()}>
        <button
          ref={btnRef}
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            setOpen((o) => !o);
          }}
          className="flex h-8 w-8 items-center justify-center rounded-lg text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800 dark:hover:text-slate-300"
        >
          <MoreHorizontal className="h-4 w-4" />
        </button>
      </div>
      {open && typeof document !== "undefined" && createPortal(menuPanel, document.body)}
    </>
  );
}

// ─── BulkActionsBar ────────────────────────────────────────────────────────────

export function BulkActionsBar({ count, onDelete, onMove, onAssignPallet, onClear, deleting }: {
  count: number; onDelete?: () => void; onMove?: () => void; /** Bulk assign packages → pallet (same hierarchy as item → package move) */
  onAssignPallet?: () => void; onClear: () => void; deleting?: boolean;
}) {
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-700/60 dark:bg-sky-950/30">
      <div className="flex items-center gap-2">
        <CheckSquare className="h-4 w-4 text-sky-600 dark:text-sky-400" />
        <span className="text-sm font-bold text-sky-700 dark:text-sky-300">{count} selected</span>
        <button onClick={onClear} className="text-xs text-sky-500 underline hover:text-sky-700 dark:text-sky-400">Clear</button>
      </div>
      <div className="ml-auto flex flex-wrap gap-2">
        {onMove && <button onClick={onMove} className="flex h-9 items-center gap-1.5 rounded-xl bg-sky-500 px-3 text-sm font-semibold text-white transition hover:bg-sky-600"><ArrowRight className="h-4 w-4" />Move / Reassign</button>}
        {onAssignPallet && <button onClick={onAssignPallet} className="flex h-9 items-center gap-1.5 rounded-xl bg-violet-600 px-3 text-sm font-semibold text-white transition hover:bg-violet-700"><Boxes className="h-4 w-4" />Assign to Pallet</button>}
        {onDelete && (
          <button onClick={onDelete} disabled={deleting}
            className="flex h-9 items-center gap-1.5 rounded-xl border border-rose-200 px-3 text-sm font-semibold text-rose-600 transition hover:bg-rose-50 disabled:opacity-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30">
            {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}Delete ({count})
          </button>
        )}
      </div>
    </div>
  );
}

// ─── BulkMoveModal ─────────────────────────────────────────────────────────────

export function BulkMoveModal({ selectedIds, packages: allPkgs, pallets: allPlts, actor, onClose, onMoved }: {
  selectedIds: string[]; packages: PackageRecord[]; pallets: PalletRecord[];
  actor: string; onClose: () => void;
  onMoved: (updated: ReturnRecord[], failed: number) => void;
}) {
  const [targetPkgId, setTargetPkgId] = useState("");
  const [targetPltId, setTargetPltId] = useState("");
  const [moving, setMoving] = useState(false);
  const [error,  setError]  = useState("");
  const openPkgs = allPkgs.filter((p) => p.status === "open");
  const openPlts = allPlts.filter((p) => p.status === "open");
  const pkgOpts  = openPkgs.map((p) => ({
    id: p.id,
    label: p.package_number,
    sublabel: `${p.actual_item_count} items`,
    tracking: p.tracking_number ?? undefined,
    rma: p.rma_number ?? undefined,
  }));
  const pltOpts  = openPlts.map((p) => ({
    id: p.id,
    label: p.pallet_number,
    sublabel: `${p.item_count} items`,
    tracking: p.tracking_number ?? undefined,
  }));

  async function handleMove() {
    if (!targetPkgId && !targetPltId) { setError("Select at least a target package or pallet."); return; }
    const pkgErr = uuidFkInvalidMessage(targetPkgId, "Package");
    const pltErr = uuidFkInvalidMessage(targetPltId, "Pallet");
    if (pkgErr || pltErr) {
      setError(pkgErr ?? pltErr ?? "Invalid selection.");
      return;
    }
    setMoving(true); setError("");
    const results = await Promise.all(
      selectedIds.map((id) => updateReturn(id, { package_id: targetPkgId || undefined, pallet_id: targetPltId || undefined }, actor))
    );
    setMoving(false);
    const succeeded = results.filter((r) => r.ok).map((r) => r.data!);
    onMoved(succeeded, results.filter((r) => !r.ok).length);
  }

  return (
    <div className="fixed inset-0 z-[400] flex items-center justify-center bg-black/50 p-2 sm:p-4 backdrop-blur-sm">
      <div className="w-[95vw] max-w-lg overflow-hidden rounded-2xl sm:rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950">
        <div className="flex items-center justify-between border-b border-slate-200 p-4 sm:p-6 dark:border-slate-700">
          <div><p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Bulk Action</p><h2 className="mt-0.5 text-xl font-bold text-foreground">Move {selectedIds.length} Items</h2></div>
          <button onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
        </div>
        <div className="p-4 sm:p-6 space-y-5">
          <div className="rounded-2xl border border-sky-200 bg-sky-50 p-3 dark:border-sky-700/60 dark:bg-sky-950/30">
            <p className="text-xs text-sky-700 dark:text-sky-300"><span className="font-bold">{selectedIds.length} items</span> will be reassigned. Fields left blank are unchanged.</p>
          </div>
          <ComboboxField label="Move to Package" hint="(optional)" icon={Tag} options={pkgOpts} value={targetPkgId} onChange={setTargetPkgId} onClear={() => setTargetPkgId("")} placeholder="Select open package…" />
          <ComboboxField label="Move to Pallet"  hint="(optional)" icon={Boxes} options={pltOpts} value={targetPltId} onChange={setTargetPltId} onClear={() => setTargetPltId("")} placeholder="Select open pallet…" />
          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="flex gap-3 border-t border-slate-200 p-4 dark:border-slate-700">
          <button onClick={onClose} className={`${BTN_GHOST} flex-1 h-12`}>Cancel</button>
          <button onClick={handleMove} disabled={moving}
            className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl bg-sky-500 font-semibold text-white transition hover:bg-sky-600 disabled:opacity-50">
            {moving ? <Loader2 className="h-5 w-5 animate-spin" /> : <ArrowRight className="h-5 w-5" />}{moving ? "Moving…" : "Confirm Move"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── BulkAssignPackagesModal (packages → pallet) ────────────────────────────────

export function BulkAssignPackagesModal({ selectedIds, pallets: allPlts, actor, onClose, onDone }: {
  selectedIds: string[]; pallets: PalletRecord[];
  actor: string; onClose: () => void;
  onDone: (updated: PackageRecord[], failed: number) => void;
}) {
  const [targetPalletId, setTargetPalletId] = useState("");
  const [assigning, setAssigning] = useState(false);
  const [error, setError]   = useState("");
  const openPlts = allPlts.filter((p) => p.status === "open");

  async function handleAssign() {
    const raw = targetPalletId.trim();
    if (raw) {
      const msg = uuidFkInvalidMessage(raw, "Pallet");
      if (msg) {
        setError(msg);
        return;
      }
    }
    setAssigning(true); setError("");
    const palletId = raw || null;
    const results = await Promise.all(
      selectedIds.map((id) => updatePackage(id, { pallet_id: palletId }, actor)),
    );
    setAssigning(false);
    const succeeded = results.filter((r) => r.ok && r.data).map((r) => r.data!);
    onDone(succeeded, results.filter((r) => !r.ok).length);
  }

  return (
    <div className="fixed inset-0 z-[400] flex items-center justify-center bg-black/50 p-2 sm:p-4 backdrop-blur-sm">
      <div className="w-[95vw] max-w-lg overflow-hidden rounded-2xl sm:rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950">
        <div className="flex items-center justify-between border-b border-slate-200 p-4 sm:p-6 dark:border-slate-700">
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Bulk Action</p>
            <h2 className="mt-0.5 text-xl font-bold text-foreground">Assign {selectedIds.length} Packages to Pallet</h2>
          </div>
          <button type="button" onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
        </div>
        <div className="space-y-5 p-4 sm:p-6">
          <div className="rounded-2xl border border-violet-200 bg-violet-50 p-3 dark:border-violet-700/60 dark:bg-violet-950/30">
            <p className="text-xs text-violet-800 dark:text-violet-200">
              <span className="font-bold">{selectedIds.length} package(s)</span> will get the same <span className="font-semibold">pallet_id</span> in the database (same as editing a single package).
            </p>
          </div>
          <div>
            <label className={LABEL}>Target pallet</label>
            <select
              className={INPUT_SM}
              value={targetPalletId}
              onChange={(e) => setTargetPalletId(e.target.value)}
            >
              <option value="">— No pallet (unassign) —</option>
              {openPlts.map((p) => (
                <option key={p.id} value={p.id}>{p.pallet_number} ({p.item_count} items)</option>
              ))}
            </select>
          </div>
          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="flex gap-3 border-t border-slate-200 p-4 dark:border-slate-700">
          <button type="button" onClick={onClose} className={`${BTN_GHOST} h-12 flex-1`}>Cancel</button>
          <button
            type="button"
            onClick={handleAssign}
            disabled={assigning}
            className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl bg-violet-600 font-semibold text-white transition hover:bg-violet-700 disabled:opacity-50"
          >
            {assigning ? <Loader2 className="h-5 w-5 animate-spin" /> : <Boxes className="h-5 w-5" />}
            {assigning ? "Saving…" : "Confirm Assign"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── RightDrawer ───────────────────────────────────────────────────────────────

export function RightDrawer({ open, onClose, onBack, title, subtitle, children }: {
  open: boolean; onClose: () => void; onBack?: () => void;
  title: string; subtitle?: string; children: React.ReactNode;
}) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => { setMounted(true); }, []);
  useEffect(() => {
    if (open) document.body.style.overflow = "hidden";
    else document.body.style.overflow = "";
    return () => { document.body.style.overflow = ""; };
  }, [open]);
  if (!mounted) return null;
  return createPortal(
    <>
      {/* Backdrop — clicking closes the drawer */}
      <div className={`fixed inset-0 z-[200] bg-black/40 backdrop-blur-sm transition-opacity duration-300 ${open ? "opacity-100" : "pointer-events-none opacity-0"}`} onClick={onClose} />
      {/* Panel — right-side slide. z-[210] so it sits above the backdrop. */}
      <div className={`fixed right-0 top-0 z-[210] flex h-full w-full max-w-xl flex-col bg-white shadow-2xl transition-transform duration-300 dark:bg-slate-950 sm:max-w-2xl ${open ? "translate-x-0" : "translate-x-full"}`}>
        <div className="flex shrink-0 items-center border-b border-slate-200 px-5 py-4 dark:border-slate-700">
          {onBack && <button onClick={onBack} className="mr-3 rounded-xl p-1.5 text-slate-400 transition hover:bg-accent hover:text-accent-foreground"><ArrowLeft className="h-5 w-5" /></button>}
          <div className="flex-1 min-w-0">
            {subtitle && <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">{subtitle}</p>}
            <h2 className="truncate font-bold text-foreground">{title}</h2>
          </div>
          <button onClick={onClose} className="ml-3 rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
        </div>
        <div className="flex-1 overflow-y-auto">{children}</div>
      </div>
    </>,
    document.body,
  );
}

// ─── Photo Gallery + Lightbox ──────────────────────────────────────────────────

export interface PhotoItem { src: string; label: string }

function PhotoLightbox({ photos, startIdx, onClose }: {
  photos: PhotoItem[]; startIdx: number; onClose: () => void;
}) {
  const [idx, setIdx] = useState(startIdx);
  const [mounted, setMounted] = useState(false);
  useEffect(() => { setMounted(true); }, []);
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if (e.key === "Escape")      { onClose(); }
      if (e.key === "ArrowLeft"  && idx > 0)                { setIdx((i) => i - 1); }
      if (e.key === "ArrowRight" && idx < photos.length - 1) { setIdx((i) => i + 1); }
    };
    document.addEventListener("keydown", h);
    return () => document.removeEventListener("keydown", h);
  }, [idx, onClose, photos.length]);

  if (!mounted) return null;
  return createPortal(
    <div className="fixed inset-0 z-[500] flex items-center justify-center bg-black/90 backdrop-blur-md" onClick={onClose}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img src={photos[idx].src} alt={photos[idx].label} onClick={(e) => e.stopPropagation()}
        className="max-h-[88vh] max-w-[92vw] select-none rounded-2xl object-contain shadow-2xl" />
      {/* Label */}
      <div className="pointer-events-none absolute bottom-6 left-1/2 -translate-x-1/2 rounded-full bg-black/70 px-4 py-2 text-sm text-white backdrop-blur-sm">
        {photos[idx].label} · {idx + 1}/{photos.length}
      </div>
      {/* Close */}
      <button onClick={onClose} className="absolute right-4 top-4 flex h-10 w-10 items-center justify-center rounded-full bg-black/60 text-white hover:bg-black/80"><X className="h-5 w-5" /></button>
      {/* Prev */}
      {idx > 0 && <button onClick={(e) => { e.stopPropagation(); setIdx((i) => i - 1); }} className="absolute left-4 top-1/2 flex h-12 w-12 -translate-y-1/2 items-center justify-center rounded-full bg-black/50 text-white hover:bg-black/80"><ArrowLeft className="h-5 w-5" /></button>}
      {/* Next */}
      {idx < photos.length - 1 && <button onClick={(e) => { e.stopPropagation(); setIdx((i) => i + 1); }} className="absolute right-4 top-1/2 flex h-12 w-12 -translate-y-1/2 items-center justify-center rounded-full bg-black/50 text-white hover:bg-black/80"><ArrowRight className="h-5 w-5" /></button>}
    </div>,
    document.body,
  );
}

/** Thumbnail grid that opens a fullscreen lightbox on click. */
export function PhotoGallery({ photos, emptyText }: { photos: PhotoItem[]; emptyText?: string }) {
  const [lightboxIdx, setLightboxIdx] = useState<number | null>(null);
  if (!photos.length) {
    return emptyText
      ? <p className="text-xs text-slate-400 italic">{emptyText}</p>
      : null;
  }
  return (
    <>
      <div className="grid grid-cols-3 gap-2 sm:grid-cols-4">
        {photos.map((p, i) => (
          <button key={i} type="button" onClick={() => setLightboxIdx(i)}
            className="group relative aspect-square overflow-hidden rounded-xl border border-slate-200 bg-slate-100 transition hover:border-sky-400 hover:shadow-md dark:border-slate-700 dark:bg-slate-800">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img src={p.src} alt={p.label} className="h-full w-full object-contain transition group-hover:scale-[1.02]" />
            <div className="absolute inset-0 flex items-center justify-center bg-black/25 opacity-0 transition group-hover:opacity-100 rounded-xl">
              <ZoomIn className="h-5 w-5 text-white drop-shadow" />
            </div>
          </button>
        ))}
      </div>
      {lightboxIdx !== null && (
        <PhotoLightbox photos={photos} startIdx={lightboxIdx} onClose={() => setLightboxIdx(null)} />
      )}
    </>
  );
}

export interface ClaimEvidenceLineItem extends PhotoItem {
  id: string;
  kind: "pallet" | "package" | "item" | "category";
}

/** Optional grid with per-image Include toggles — intended for Claims Management admin (not the receiving wizard). */
export function ClaimEvidencePhotoGrid({
  lines,
  selection,
  onToggle,
}: {
  lines: ClaimEvidenceLineItem[];
  selection: Record<string, boolean>;
  onToggle: (id: string) => void;
}) {
  const [lightboxIdx, setLightboxIdx] = useState<number | null>(null);
  const photos = useMemo(() => lines.map(({ src, label }) => ({ src, label })), [lines]);
  if (!lines.length) {
    return <p className="text-xs text-slate-400 italic">No photos to attach.</p>;
  }
  return (
    <>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        {lines.map((line, i) => {
          const included = selection[line.id] !== false;
          return (
            <div
              key={line.id}
              className="relative aspect-square overflow-hidden rounded-xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800"
            >
              <button
                type="button"
                onClick={() => setLightboxIdx(i)}
                className="relative z-0 block h-full w-full"
                aria-label={`Open ${line.label}`}
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src={line.src} alt={line.label} className="h-full w-full object-contain" />
                <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-black/20 opacity-0 transition hover:opacity-100">
                  <ZoomIn className="h-5 w-5 text-white drop-shadow" />
                </div>
              </button>
              <div
                className="absolute left-1.5 top-1.5 z-10 flex items-center gap-1 rounded-md bg-black/60 px-2 py-1 text-[10px] font-semibold text-white shadow backdrop-blur-sm"
                onClick={(e) => e.stopPropagation()}
                onKeyDown={(e) => e.stopPropagation()}
              >
                <input
                  type="checkbox"
                  checked={included}
                  onChange={() => onToggle(line.id)}
                  className="h-3.5 w-3.5 shrink-0 rounded border-white/40"
                  aria-label={`Include ${line.label} in claim report`}
                />
                <span>Include</span>
              </div>
            </div>
          );
        })}
      </div>
      {lightboxIdx !== null && (
        <PhotoLightbox photos={photos} startIdx={lightboxIdx} onClose={() => setLightboxIdx(null)} />
      )}
    </>
  );
}

// ─── Items Sub-Table (inside Package Drawer) ────────────────────────────────────

function ItemsSubTable({ items, role, actor, onItemClick, onItemDeleted, showToast }: {
  items: ReturnRecord[]; role: UserRole; actor: string;
  onItemClick: (r: ReturnRecord) => void;
  onItemDeleted: (id: string) => void;
  showToast: (msg: string, kind?: ToastKind) => void;
}) {
  const [search,  setSearch]  = useState("");
  const [statusF, setStatusF] = useState("");
  const filtered = useMemo(() => {
    let d = [...items];
    if (search) {
      const q = search.toLowerCase();
      d = d.filter((r) => [r.lpn ?? "", r.item_name, r.asin ?? "", r.fnsku ?? "", r.sku ?? "", r.id].some((v) => v.toLowerCase().includes(q)));
    }
    if (statusF) d = d.filter((r) => r.status === statusF);
    return d;
  }, [items, search, statusF]);

  return (
    <div className="space-y-3">
      <div className="flex gap-2">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" />
          <input placeholder="Search LPN, RMA, item…" value={search} onChange={(e) => setSearch(e.target.value)} className={`${INPUT_SM} pl-8 text-xs`} />
        </div>
        <select value={statusF} onChange={(e) => setStatusF(e.target.value)} className={`${INPUT_SM} w-auto text-xs`}>
          <option value="">All</option>
          {Object.entries(STATUS_CFG).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
        </select>
      </div>
      {filtered.length === 0
        ? <p className="py-4 text-center text-xs text-slate-400">No items{search || statusF ? " match your filters" : " scanned yet"}.</p>
        : (
          <div className="rounded-xl border border-border">
            <table className="w-full text-xs">
              <thead>
                <tr className="rounded-t-xl border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">LPN</th>
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Item</th>
                  <th className="hidden px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400 sm:table-cell">Status</th>
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Date</th>
                  <th className="px-3 py-2" />
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {filtered.map((r) => (
                  <tr key={r.id} onClick={() => onItemClick(r)} className="group cursor-pointer transition hover:bg-sky-50/50 dark:hover:bg-sky-950/20">
                    <td className="px-3 py-2.5" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-center gap-1.5 font-mono font-bold text-slate-700 dark:text-slate-300">
                        <span>{r.lpn ?? "—"}</span>
                        {r.lpn ? <InlineCopy value={r.lpn} label="LPN" onToast={showToast} stopPropagation /> : null}
                      </div>
                    </td>
                    <td className="min-w-0 max-w-none truncate px-3 py-2.5 text-slate-600 dark:text-slate-300">{r.item_name}</td>
                    <td className="hidden px-3 py-2.5 sm:table-cell"><StatusBadge status={r.status} /></td>
                    <td className="px-3 py-2.5 text-slate-400">{fmt(r.created_at)}</td>
                    <td className="px-3 py-2.5">
                      <RowActionMenu
                        onView={() => onItemClick(r)}
                        onDelete={canDelete(role) ? async () => {
                          const res = await deleteReturn(r.id, actor);
                          if (res.ok) onItemDeleted(r.id);
                          else showToast(res.error ?? "Delete failed.", "error");
                        } : undefined}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
    </div>
  );
}

// ─── Packages Sub-Table (inside Pallet Drawer) ─────────────────────────────────

function PackagesSubTable({ palletId, packages, onPackageClick, showToast }: {
  palletId: string; packages: PackageRecord[]; onPackageClick: (p: PackageRecord) => void;
  showToast?: (msg: string, kind?: ToastKind) => void;
}) {
  const rows = useMemo(() => packages.filter((p) => p.pallet_id === palletId), [packages, palletId]);
  if (rows.length === 0) return <p className="py-4 text-center text-xs text-slate-400">No packages linked to this pallet yet.</p>;
  return (
    <div className="overflow-hidden rounded-xl border border-border">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
            <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Package #</th>
            <th className="hidden px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400 sm:table-cell">Carrier</th>
            <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Count</th>
            <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {rows.map((p) => (
            <tr key={p.id} onClick={() => onPackageClick(p)} className="group cursor-pointer transition hover:bg-violet-50/50 dark:hover:bg-violet-950/20">
              <td className="px-3 py-2.5" onClick={(e) => e.stopPropagation()}>
                <div className="flex items-center gap-1.5 font-mono font-bold text-foreground">
                  <span>{p.package_number}</span>
                  <InlineCopy value={p.package_number} label="Package #" onToast={showToast} stopPropagation />
                </div>
              </td>
              <td className="hidden px-3 py-2.5 text-muted-foreground sm:table-cell">{p.carrier_name ?? "—"}</td>
              <td className="px-3 py-2.5 font-bold text-slate-700 dark:text-slate-300">{p.actual_item_count}/{p.expected_item_count > 0 ? p.expected_item_count : "?"}</td>
              <td className="px-3 py-2.5"><PkgStatusBadge status={p.status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Item Drawer Content ───────────────────────────────────────────────────────

export function ItemDrawerContent({ record, role, actor, packages, pallets, onUpdated, onDeleted, startInEditMode = false, sessionPhotos, onToast }: {
  record: ReturnRecord; role: UserRole; actor: string;
  packages: PackageRecord[]; pallets: PalletRecord[];
  onUpdated: (r: ReturnRecord) => void; onDeleted: (id: string) => void;
  startInEditMode?: boolean;
  /** File objects captured in the current browser session — enables live gallery. */
  sessionPhotos?: Record<string, File[]>;
  onToast?: (msg: string, kind?: ToastKind) => void;
}) {
  const [editing,    setEditing]    = useState(startInEditMode);
  const [saving,     setSaving]     = useState(false);
  const [confirmDel, setConfirmDel] = useState(false);
  const [deleting,   setDeleting]   = useState(false);
  const [err,        setErr]        = useState("");
  const [editLpn,    setEditLpn]    = useState(record.lpn ?? "");
  const [editProductId, setEditProductId] = useState(record.asin ?? record.fnsku ?? record.sku ?? "");
  const [editAsin,   setEditAsin]   = useState(record.asin ?? "");
  const [editFnsku,  setEditFnsku]  = useState(record.fnsku ?? "");
  const [editSku, setEditSku] = useState(record.sku ?? "");
  const [editStoreId, setEditStoreId] = useState(record.store_id ?? "");
  const [itemStoresList, setItemStoresList] = useState<{ id: string; name: string; platform: string }[]>([]);
  const [editItem,   setEditItem]   = useState(record.item_name);
  const [editNotes,  setEditNotes]  = useState(record.notes ?? "");
  const [editOrderId, setEditOrderId] = useState(record.order_id ?? "");
  /**
   * Mutable copy of `photo_evidence` counts for edit mode.
   * Each key is a category slug, value is the remaining count of photos in that bucket.
   * Operator clicks X on a placeholder thumbnail → decrements the count.
   * New photos captured in edit mode are appended to `editNewPhotos`.
   */
  const [editPhotoEvidence, setEditPhotoEvidence] = useState<Record<string, number>>(
    () => photoEvidenceCategoryCounts(record.photo_evidence),
  );
  const [editNewPhotos, setEditNewPhotos] = useState<Record<string, File[]>>({});
  const [editCatalogStatus, setEditCatalogStatus] = useState<"idle" | "loading" | "local" | "amazon" | "unknown">("idle");
  const [editCatalogPreview, setEditCatalogPreview] = useState<{ name: string; price?: number; image_url?: string } | null>(null);
  const [editExpiryDate,     setEditExpiryDate]     = useState(record.expiration_date ?? "");
  const [editPhotoItemUrl,   setEditPhotoItemUrl]   = useState(
    () => getReturnPhotoEvidenceUrls(record.photo_evidence).item_url,
  );
  const [editPhotoExpiryUrl, setEditPhotoExpiryUrl] = useState(
    () => getReturnPhotoEvidenceUrls(record.photo_evidence).expiry_url,
  );
  const [expiryEditFiles, setExpiryEditFiles] = useState<File[]>([]);
  const [itemEditFiles, setItemEditFiles] = useState<File[]>([]);
  const [itemPhotoUploading,   setItemPhotoUploading]   = useState(false);
  const [expiryPhotoUploading, setExpiryPhotoUploading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const res = await listStores();
      if (cancelled || !res.ok || !res.data) return;
      setItemStoresList(
        res.data
          .filter((s) => s.is_active !== false)
          .map((s) => ({ id: s.id, name: s.name, platform: s.platform })),
      );
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    setEditLpn(record.lpn ?? "");
    setEditProductId(record.asin ?? record.fnsku ?? record.sku ?? "");
    setEditAsin(record.asin ?? "");
    setEditFnsku(record.fnsku ?? "");
    setEditSku(record.sku ?? "");
    setEditStoreId(record.store_id ?? "");
    setEditItem(record.item_name);
    setEditNotes(record.notes ?? "");
    setEditOrderId(record.order_id ?? "");
    setEditPhotoEvidence(photoEvidenceCategoryCounts(record.photo_evidence));
    setEditExpiryDate(record.expiration_date ?? "");
    setEditPhotoItemUrl(getReturnPhotoEvidenceUrls(record.photo_evidence).item_url);
    setEditPhotoExpiryUrl(getReturnPhotoEvidenceUrls(record.photo_evidence).expiry_url);
    setExpiryEditFiles([]);
    setItemEditFiles([]);
    setEditCatalogStatus("idle");
    setEditCatalogPreview(null);
  }, [record.id, record.lpn, record.asin, record.fnsku, record.sku, record.store_id, record.item_name, record.notes, record.order_id, record.photo_evidence, record.expiration_date]);

  async function handleEditBarcodeLookup(barcode: string) {
    if (!barcode.trim()) { setEditCatalogStatus("idle"); return; }
    setEditCatalogStatus("loading");
    setEditCatalogPreview(null);

    const classified = classifyProductBarcode(barcode.trim());
    if (classified.kind === "fnsku") {
      setEditFnsku(classified.normalized);
      setEditProductId(classified.normalized);
    } else if (classified.kind === "asin") {
      setEditAsin(classified.normalized);
      setEditProductId(classified.normalized);
    } else if (classified.kind === "upc_ean") {
      setEditProductId(classified.normalized);
    }

    const { data: local } = await supabaseBrowser
      .from("products")
      .select("*")
      .eq("barcode", barcode.trim())
      .maybeSingle();

    if (local) {
      setEditItem(local.name);
      setEditCatalogPreview({ name: local.name, price: local.price, image_url: local.image_url });
      setEditCatalogStatus("local");
      return;
    }

    const amazon = await fetchProductFromAmazon(barcode.trim());
    if (amazon) {
      setEditItem(amazon.name);
      setEditCatalogPreview({ name: amazon.name, price: amazon.price, image_url: amazon.image_url });
      setEditCatalogStatus("amazon");
      try {
        await supabaseBrowser
          .from("products")
          .insert({ barcode: barcode.trim(), name: amazon.name, price: amazon.price, image_url: amazon.image_url, source: "Amazon" });
      } catch {
        // ignore duplicate/insert errors
      }
      return;
    }

    setEditCatalogStatus("unknown");
  }

  const { onKeyDown: editBarcodeKeyDown } = usePhysicalScanner({
    enabled: editing,
    onScan: (code) => { setEditProductId(code); void handleEditBarcodeLookup(code); },
  });

  function removeExistingPhotoSlot(cat: string) {
    setEditPhotoEvidence((prev) => {
      const next = { ...prev, [cat]: Math.max(0, (prev[cat] ?? 0) - 1) };
      if (next[cat] === 0) delete next[cat];
      return next;
    });
  }

  const linkedPkg    = packages.find((p) => p.id === record.package_id);
  const linkedPallet = pallets.find((p) => p.id === record.pallet_id);
  /** Order ID is stored on the package when this item is in a package — not edited on the item row. */
  const isLooseItem = !record.package_id;
  const showClaimLinkage =
    record.status === "ready_for_claim" || record.status === "pending_evidence";
  const editStorePlatform =
    itemStoresList.find((s) => s.id === editStoreId)?.platform ?? record.stores?.platform ?? null;
  const photoTotal   = photoEvidenceNumericTotal(record.photo_evidence);
  const drawerEditIdBtn =
    "inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border border-slate-200 bg-white text-slate-500 transition hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800";
  const expiryLabelCtxForEdit = useMemo(
    () => ({ condition_keys: record.conditions ?? [], expiration_date: editExpiryDate }),
    [record.conditions, editExpiryDate],
  );
  const showExpiryLabelSlot = shouldShowExpiryLabelPhoto(expiryLabelCtxForEdit);

  async function copyEditCode(v: string, label: string) {
    if (!v.trim()) return;
    try {
      await navigator.clipboard.writeText(v.trim());
      onToast?.(`Copied ${label}`, "success");
    } catch {
      onToast?.("Copy failed", "error");
    }
  }

  async function handleSave() {
    const storeMsg = uuidFkInvalidMessage(editStoreId, "Store");
    if (storeMsg) {
      setErr(storeMsg);
      onToast?.(storeMsg, "error");
      return;
    }
    if (showExpiryLabelSlot && !editPhotoExpiryUrl.trim()) {
      const msg = "Expiry label photo is required when the item is expired or the expiry date is within the critical window.";
      setErr(msg);
      onToast?.(msg, "error");
      return;
    }
    setSaving(true); setErr("");
    // Merge new photos (counts only — actual files not yet uploaded to cloud storage)
    const newPhotoCount: Record<string, number> = {};
    Object.entries(editNewPhotos).forEach(([cat, files]) => {
      if (files.length) newPhotoCount[cat] = (newPhotoCount[cat] ?? 0) + files.length;
    });
    const mergedCounts: Record<string, number> = { ...editPhotoEvidence };
    Object.entries(newPhotoCount).forEach(([cat, n]) => {
      mergedCounts[cat] = (mergedCounts[cat] ?? 0) + n;
    });
    const mergedPhotoEvidence = mergeReturnPhotoEvidence(mergedCounts, {
      item_url: editPhotoItemUrl,
      expiry_url: editPhotoExpiryUrl,
    });
    const pickedStore = itemStoresList.find((s) => s.id === editStoreId);
    const res = await updateReturn(record.id, {
      lpn: editLpn || undefined,
      item_name: editItem,
      notes: editNotes || undefined,
      order_id: isLooseItem ? (editOrderId.trim() || null) : null,
      expiration_date:  editExpiryDate  || undefined,
      photo_evidence:   mergedPhotoEvidence ?? undefined,
      asin: editAsin.trim() || editProductId.trim() || null,
      fnsku: editFnsku.trim() || null,
      sku: editSku.trim() || null,
      store_id: editStoreId.trim() || null,
      marketplace: pickedStore ? platformToMarketplace(pickedStore.platform) : record.marketplace,
    }, actor);
    setSaving(false);
    if (res.ok && res.data) { onUpdated(res.data); setEditing(false); setEditNewPhotos({}); }
    else setErr(res.error ?? "Save failed.");
  }

  async function handleDelete() {
    setDeleting(true);
    const res = await deleteReturn(record.id, actor);
    if (res.ok) onDeleted(record.id);
    else setDeleting(false);
  }

  return (
    <div className="p-4 sm:p-6 space-y-5">
      <div className="flex flex-wrap gap-2">
        <StatusBadge status={record.status} />
        <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
          {record.stores?.name ? `${record.stores.name} (${record.stores.platform})` : formatMarketplaceSource(record.marketplace)}
        </span>
        {linkedPkg    && <span className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-2.5 py-0.5 text-xs font-semibold text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300"><Tag className="h-3 w-3" />{linkedPkg.package_number}</span>}
        {linkedPallet && <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"><Boxes className="h-3 w-3" />{linkedPallet.pallet_number}</span>}
      </div>

      {editing ? (
        <div className="space-y-4">
          <div>
            <label className={LABEL}>Product barcode (UPC / scan)</label>
            <input
              className={`${INPUT} transition-all ${editCatalogStatus === "unknown" ? "border-yellow-400 ring-2 ring-yellow-300 focus:border-yellow-400 focus:ring-yellow-300" : ""}`}
              value={editProductId}
              onChange={(e) => { setEditProductId(e.target.value); setEditCatalogStatus("idle"); }}
              placeholder="Product identifier…"
              onKeyDown={editBarcodeKeyDown}
              onBlur={(e) => { if (e.target.value.trim()) void handleEditBarcodeLookup(e.target.value); }}
            />
            {editCatalogStatus === "loading" && (
              <div className="mt-1.5 flex items-center gap-2 text-xs text-slate-400">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />Looking up barcode…
              </div>
            )}
            {editCatalogStatus === "local" && editCatalogPreview && (
              <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-300">
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />Found in Catalog — {editCatalogPreview.name}
              </div>
            )}
            {editCatalogStatus === "amazon" && editCatalogPreview && (
              <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-3 py-1.5 text-xs font-semibold text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/30 dark:text-sky-300">
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />Found on Amazon — {editCatalogPreview.name}{editCatalogPreview.price != null ? ` · $${editCatalogPreview.price.toFixed(2)}` : ""}
              </div>
            )}
            {editCatalogStatus === "unknown" && (
              <div className="mt-1.5 rounded-xl border-2 border-yellow-400 bg-yellow-50 px-3 py-2 text-xs font-bold text-yellow-800 dark:border-yellow-500/60 dark:bg-yellow-950/20 dark:text-yellow-300">
                ⚠️ Unknown Item — Not found locally or on Amazon.
              </div>
            )}
          </div>
          <div className="space-y-3">
            <div>
              <label className={LABEL}>
                ASIN <span className="text-xs font-normal text-slate-400">(optional — Amazon catalog ID)</span>
              </label>
              <div className="flex gap-2">
                <input type="text" className={INPUT} placeholder="e.g. B08N5WRWNW" value={editAsin} onChange={(e) => setEditAsin(e.target.value)} />
                <button type="button" title="Copy ASIN" className={drawerEditIdBtn} onClick={() => void copyEditCode(editAsin, "ASIN")} disabled={!editAsin.trim()}><Copy className="h-4 w-4" /></button>
                <button
                  type="button"
                  title="Open marketplace search"
                  className={drawerEditIdBtn}
                  disabled={!marketplaceSearchUrl(editStorePlatform, editAsin)}
                  onClick={() => {
                    const u = marketplaceSearchUrl(editStorePlatform, editAsin);
                    if (u) window.open(u, "_blank", "noopener,noreferrer");
                  }}
                >
                  <Store className="h-4 w-4" aria-hidden />
                </button>
              </div>
            </div>
            <div>
              <label className={LABEL}>
                FNSKU <span className="text-xs font-normal text-slate-400">(optional — your FBA label on Amazon)</span>
              </label>
              <div className="flex gap-2">
                <input type="text" className={INPUT} placeholder="e.g. X001ABC123" value={editFnsku} onChange={(e) => setEditFnsku(e.target.value)} />
                <button type="button" title="Copy FNSKU" className={drawerEditIdBtn} onClick={() => void copyEditCode(editFnsku, "FNSKU")} disabled={!editFnsku.trim()}><Copy className="h-4 w-4" /></button>
                <button
                  type="button"
                  title="Open marketplace search"
                  className={drawerEditIdBtn}
                  disabled={!marketplaceSearchUrl(editStorePlatform, editFnsku)}
                  onClick={() => {
                    const u = marketplaceSearchUrl(editStorePlatform, editFnsku);
                    if (u) window.open(u, "_blank", "noopener,noreferrer");
                  }}
                >
                  <Store className="h-4 w-4" aria-hidden />
                </button>
              </div>
            </div>
            <div>
              <label className={LABEL}>
                SKU <span className="text-xs font-normal text-slate-400">(optional — Seller / warehouse SKU, MSKU)</span>
              </label>
              <div className="flex gap-2">
                <input type="text" className={INPUT} placeholder="Seller Central SKU…" value={editSku} onChange={(e) => setEditSku(e.target.value)} />
                <button type="button" title="Copy SKU" className={drawerEditIdBtn} onClick={() => void copyEditCode(editSku, "SKU")} disabled={!editSku.trim()}><Copy className="h-4 w-4" /></button>
                <button
                  type="button"
                  title="Open marketplace search"
                  className={drawerEditIdBtn}
                  disabled={!marketplaceSearchUrl(editStorePlatform, editSku)}
                  onClick={() => {
                    const u = marketplaceSearchUrl(editStorePlatform, editSku);
                    if (u) window.open(u, "_blank", "noopener,noreferrer");
                  }}
                >
                  <Store className="h-4 w-4" aria-hidden />
                </button>
              </div>
            </div>
            <div>
              <label className={LABEL}>Store <span className="text-rose-500">*</span></label>
              <select className={INPUT} value={editStoreId} onChange={(e) => setEditStoreId(e.target.value)}>
                <option value="">— Select Store —</option>
                {itemStoresList.map((s) => (
                  <option key={s.id} value={s.id}>{s.name} ({s.platform})</option>
                ))}
              </select>
              {itemStoresList.length === 0 && (
                <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">No active stores found. Add a store in Settings → Stores.</p>
              )}
            </div>
          </div>
          {!record.package_id && (
            <div><label className={LABEL}>LPN <span className="text-xs font-normal text-slate-400">(optional)</span></label><input className={INPUT} value={editLpn} onChange={(e) => setEditLpn(e.target.value)} placeholder="Orphan label scan…" /></div>
          )}
          <div><label className={LABEL}>Item Name <span className="text-rose-500">*</span></label><input className={INPUT} value={editItem} onChange={(e) => setEditItem(e.target.value)} /></div>

          {/* ── Expiry Date (FEFO) ──────────────────────────────────────────── */}
          <div className="rounded-2xl border border-orange-200 bg-orange-50 p-4 space-y-3 dark:border-orange-700/40 dark:bg-orange-950/30">
            <p className="flex items-center gap-2 text-sm font-bold text-orange-700 dark:text-orange-400">
              <CalendarX2 className="h-4 w-4" />Expiry Date
            </p>
            <input
              type="date"
              className={INPUT}
              value={editExpiryDate}
              onChange={(e) => setEditExpiryDate(e.target.value)}
            />
            {showExpiryLabelSlot && (
              <p className="text-xs text-orange-600 dark:text-orange-400 flex items-center gap-1">
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                An expiry label photo is required for claim evidence (expired item or date in the critical window).
              </p>
            )}
          </div>

          {/* ── Inherited Package Evidence (intake / package-linked categories) ─────────── */}
          {Object.keys(editPhotoEvidence).length > 0 && (
            <div className="space-y-4 rounded-2xl border-2 border-violet-200 bg-violet-50/80 p-4 dark:border-violet-800/50 dark:bg-violet-950/25">
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-2">
                  <Package2 className="h-4 w-4 text-violet-600 dark:text-violet-400" />
                  <p className="text-sm font-bold text-violet-900 dark:text-violet-100">Inherited Package Evidence</p>
                </div>
                <p className="text-xs leading-relaxed text-violet-800/90 dark:text-violet-200/90">
                  Photos tied to claim categories from the linked package flow. Manage existing slots or add more by category below.
                </p>
              </div>
              <div className="space-y-3 rounded-xl border border-violet-200/90 bg-white/70 p-4 dark:border-violet-800/50 dark:bg-slate-900/50">
                <p className="text-xs font-semibold text-violet-900 dark:text-violet-100">Registered slots</p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Click <span className="font-semibold">✕</span> on a thumbnail to remove that photo slot before saving.</p>
                {Object.entries(editPhotoEvidence).map(([cat, count]) => {
                  const catDef = ALL_PHOTO_CATEGORIES[cat];
                  return (
                    <div key={cat}>
                      <p className="mb-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
                        {catDef?.label ?? cat}
                        <span className="ml-2 font-normal text-slate-400">{count} photo{count !== 1 ? "s" : ""}</span>
                      </p>
                      <div className="flex flex-wrap gap-2">
                        {Array.from({ length: count }).map((_, i) => (
                          <div key={i} className="relative flex h-16 w-16 items-center justify-center overflow-hidden rounded-xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800">
                            <Camera className="h-6 w-6 text-slate-300 dark:text-slate-600" />
                            <span className="absolute bottom-1 right-1 text-[10px] font-bold text-slate-400">#{i + 1}</span>
                            <button
                              type="button"
                              onClick={() => removeExistingPhotoSlot(cat)}
                              aria-label={`Remove photo ${i + 1} of ${catDef?.label ?? cat}`}
                              className="absolute right-0.5 top-0.5 flex h-5 w-5 items-center justify-center rounded-full bg-rose-600 text-white shadow transition hover:bg-rose-700 active:scale-90"
                            >
                              <X className="h-3 w-3" />
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
              <div className="space-y-3 rounded-xl border border-violet-200/90 bg-white/70 p-4 dark:border-violet-800/50 dark:bg-slate-900/50">
                <p className="text-xs font-semibold text-violet-900 dark:text-violet-100">Add more by category</p>
                {Object.keys(editPhotoEvidence).map((cat) => {
                  const catDef = ALL_PHOTO_CATEGORIES[cat];
                  if (!catDef) return null;
                  return (
                    <SmartCameraUpload
                      key={cat}
                      label={catDef.label}
                      hint={`Add more ${catDef.label} photos`}
                      icon={catDef.icon}
                      iconColor={catDef.iconColor}
                      accentClass={catDef.accentClass}
                      files={editNewPhotos[cat] ?? []}
                      onChange={(files) => setEditNewPhotos((p) => ({ ...p, [cat]: files }))}
                    />
                  );
                })}
              </div>
            </div>
          )}

          {/* ── Item Condition Photos ─────────────────────────────────────── */}
          <div className="rounded-2xl border-2 border-sky-200 bg-sky-50 p-4 space-y-4 dark:border-sky-700/50 dark:bg-sky-950/20">
            <div className="flex flex-col gap-1">
              <div className="flex items-center gap-2">
                <Camera className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                <p className="text-sm font-bold text-sky-800 dark:text-sky-200">Item Condition Photos</p>
              </div>
              <p className="text-xs leading-relaxed text-sky-800/80 dark:text-sky-200/80">
                Photos of this unit’s physical condition (overall item shot and expiry label when required).
              </p>
            </div>

            {/* Item photo */}
            {editPhotoItemUrl ? (
              <SavedUrlEvidenceCard
                label="Item photo"
                hint="Overall shot of the product (optional)."
                imageUrl={editPhotoItemUrl}
                onRemove={() => {
                  setEditPhotoItemUrl("");
                  setItemEditFiles([]);
                }}
                Icon={Camera}
                iconColor="text-sky-600 dark:text-sky-400"
              />
            ) : (
              <SmartCameraUpload
                label="Item photo"
                hint="Overall shot of the product (optional)."
                required={false}
                maxPhotos={1}
                files={itemEditFiles}
                onChange={async (files) => {
                  setItemEditFiles(files);
                  if (files.length === 0) {
                    setEditPhotoItemUrl("");
                    return;
                  }
                  setItemPhotoUploading(true);
                  try {
                    const url = await uploadToStorage(files[files.length - 1], "evidence/wizard", record.organization_id);
                    setEditPhotoItemUrl(url);
                    setItemEditFiles([]);
                  } catch {
                    setErr("Photo upload failed. Try again.");
                    setItemEditFiles([]);
                  } finally {
                    setItemPhotoUploading(false);
                  }
                }}
                accentClass="border-sky-200 dark:border-sky-800/50"
                icon={Camera}
                iconColor="text-sky-600 dark:text-sky-400"
              />
            )}
            {itemPhotoUploading && (
              <p className="flex items-center gap-2 text-xs text-sky-700 dark:text-sky-300">
                <Loader2 className="h-3.5 w-3.5 animate-spin" /> Uploading…
              </p>
            )}

            {showExpiryLabelSlot && (
              <div className="rounded-2xl border border-orange-200 bg-orange-50/80 p-4 dark:border-orange-800/50 dark:bg-orange-950/25">
                {editPhotoExpiryUrl ? (
                  <SavedUrlEvidenceCard
                    label="Expiry label photo"
                    hint={ALL_PHOTO_CATEGORIES.expiry_label.hint}
                    imageUrl={editPhotoExpiryUrl}
                    onRemove={() => {
                      setEditPhotoExpiryUrl("");
                      setExpiryEditFiles([]);
                    }}
                    Icon={Calendar}
                    iconColor="text-orange-600 dark:text-orange-400"
                    required
                  />
                ) : (
                  <SmartCameraUpload
                    label="Expiry label photo"
                    hint={ALL_PHOTO_CATEGORIES.expiry_label.hint}
                    required
                    maxPhotos={1}
                    files={expiryEditFiles}
                    onChange={async (files) => {
                      setExpiryEditFiles(files);
                      const last = files[files.length - 1];
                      if (!last) return;
                      setExpiryPhotoUploading(true);
                      try {
                        const url = await uploadToStorage(last, "evidence/wizard", record.organization_id);
                        setEditPhotoExpiryUrl(url);
                        setExpiryEditFiles([]);
                      } catch {
                        setErr("Photo upload failed. Try again.");
                        setExpiryEditFiles([]);
                      } finally {
                        setExpiryPhotoUploading(false);
                      }
                    }}
                    accentClass="border-orange-200 dark:border-orange-800/50"
                    icon={Calendar}
                    iconColor="text-orange-600 dark:text-orange-400"
                  />
                )}
                {expiryPhotoUploading && (
                  <p className="mt-2 flex items-center gap-2 text-xs text-orange-700 dark:text-orange-300">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> Uploading…
                  </p>
                )}
              </div>
            )}
          </div>

          {isLooseItem && (
            <div>
              <label className={LABEL}>Amazon order ID <span className="text-xs font-normal text-slate-400">(optional)</span></label>
              <input
                className={`${INPUT} font-mono`}
                value={editOrderId}
                onChange={(e) => setEditOrderId(e.target.value)}
                placeholder="e.g. 111-1234567-8901234"
                autoComplete="off"
              />
            </div>
          )}
          <p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:border-slate-700 dark:bg-slate-900/80 dark:text-slate-300">
            Status is recalculated on save from defect reasons, photos, and linked package box/label shots (for claims).
          </p>
          <div><label className={LABEL}>Notes</label><textarea rows={3} className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" value={editNotes} onChange={(e) => setEditNotes(e.target.value)} /></div>

          {err && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{err}</p>}
          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-slate-200 pt-4 dark:border-slate-800">
            <button
              type="button"
              onClick={() => {
                setEditing(false);
                setEditLpn(record.lpn ?? "");
                setEditProductId(record.asin ?? record.fnsku ?? record.sku ?? "");
                setEditAsin(record.asin ?? "");
                setEditFnsku(record.fnsku ?? "");
                setEditSku(record.sku ?? "");
                setEditStoreId(record.store_id ?? "");
                setEditPhotoEvidence(photoEvidenceCategoryCounts(record.photo_evidence));
                setEditNewPhotos({});
              }}
              className={BTN_FOOTER_GHOST}
            >
              <XCircle className="h-4 w-4" />Cancel
            </button>
            <button type="button" onClick={handleSave} disabled={saving} className={BTN_FOOTER_PRIMARY}>
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}Save
            </button>
          </div>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div className="col-span-2">
              <p className="text-xs text-slate-400">Item Name</p>
              <p className="font-semibold text-foreground">{record.item_name || "Unknown Item"}</p>
            </div>
            {showClaimLinkage ? (
              <div className="col-span-2 rounded-2xl border border-violet-200 bg-violet-50/90 p-4 dark:border-violet-800/50 dark:bg-violet-950/30">
                <p className="mb-2 text-[11px] font-bold uppercase tracking-wide text-violet-700 dark:text-violet-300">Claim filing context</p>
                <p className="mb-3 text-[10px] leading-relaxed text-violet-600/90 dark:text-violet-400/90">
                  Pallet and package hierarchy with copyable product codes for your claim.
                </p>
                <div className="mb-4 grid gap-2 sm:grid-cols-2">
                  <div className="group flex min-w-0 items-center gap-2 rounded-xl border border-violet-200 bg-white/80 px-3 py-2 dark:border-violet-800/60 dark:bg-slate-900/50">
                    <span className="shrink-0 text-[10px] font-bold uppercase tracking-wide text-violet-600 dark:text-violet-400">Pallet #</span>
                    <span className="min-w-0 flex-1 truncate font-mono text-sm font-semibold text-foreground">{linkedPallet?.pallet_number ?? "—"}</span>
                    {linkedPallet?.pallet_number ? (
                      <InlineCopy value={linkedPallet.pallet_number} label="Pallet #" onToast={onToast} />
                    ) : null}
                  </div>
                  <div className="group flex min-w-0 items-center gap-2 rounded-xl border border-violet-200 bg-white/80 px-3 py-2 dark:border-violet-800/60 dark:bg-slate-900/50">
                    <span className="shrink-0 text-[10px] font-bold uppercase tracking-wide text-violet-600 dark:text-violet-400">Package #</span>
                    <span className="min-w-0 flex-1 truncate font-mono text-sm font-semibold text-foreground">{linkedPkg?.package_number ?? "—"}</span>
                    {linkedPkg?.package_number ? (
                      <InlineCopy value={linkedPkg.package_number} label="Package #" onToast={onToast} />
                    ) : null}
                  </div>
                </div>
                <p className="mb-2 text-[10px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">
                  Product title &amp; identifiers
                </p>
                <ReturnIdentifiersColumn
                  itemName={record.item_name}
                  asin={record.asin}
                  fnsku={record.fnsku}
                  sku={record.sku}
                  storePlatform={record.stores?.platform}
                  onToast={onToast}
                />
              </div>
            ) : (
              <div className="col-span-2 rounded-2xl border border-slate-200 bg-slate-50/90 p-4 dark:border-slate-700 dark:bg-slate-900/50">
                <p className="mb-2 text-[11px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">Product codes</p>
                <p className="mb-3 text-[10px] leading-relaxed text-slate-500 dark:text-slate-500">
                  ASIN = Amazon catalog ID · FNSKU = your FBA label on Amazon · SKU = Seller / warehouse SKU (MSKU)
                </p>
                <ReturnIdentifiersColumn
                  hideItemName
                  itemName={record.item_name}
                  asin={record.asin}
                  fnsku={record.fnsku}
                  sku={record.sku}
                  storePlatform={record.stores?.platform}
                  onToast={onToast}
                />
              </div>
            )}
            {record.lpn && (
              <div className="group flex flex-wrap items-center gap-2">
                <div>
                  <p className="text-xs text-slate-400">LPN</p>
                  <p className="font-mono font-bold text-foreground">{record.lpn}</p>
                </div>
                <InlineCopy value={record.lpn} label="LPN" onToast={onToast} />
              </div>
            )}
            {record.expiration_date && (
              <div className="col-span-2 rounded-xl border border-orange-200 bg-orange-50 px-3 py-2 dark:border-orange-700/40 dark:bg-orange-950/30">
                <p className="text-[10px] font-bold uppercase tracking-wide text-orange-500 mb-0.5">Expiry (FEFO)</p>
                <p className="font-mono font-bold text-orange-700 dark:text-orange-300">{record.expiration_date}</p>
              </div>
            )}
            {(() => {
              const peUrls = getReturnPhotoEvidenceUrls(record.photo_evidence);
              return (peUrls.item_url || peUrls.expiry_url) ? (
              <div className="col-span-2">
                <p className="text-[10px] font-bold uppercase tracking-wide text-slate-400 mb-1.5">Item Condition Photos</p>
                <PhotoGallery
                  photos={[
                    ...(peUrls.item_url ? [{ src: peUrls.item_url, label: "Item photo" }] : []),
                    ...(peUrls.expiry_url ? [{ src: peUrls.expiry_url, label: "Expiry label" }] : []),
                  ]}
                />
              </div>
            ) : null;
            })()}
            {(record.inherited_tracking_number || linkedPkg?.tracking_number) && (
              <div className="col-span-2">
                <p className="text-xs text-slate-400">Tracking (from package)</p>
                <div className="group flex flex-wrap items-center gap-2">
                  <p className="font-mono text-sm text-foreground">
                    {record.inherited_tracking_number ?? linkedPkg?.tracking_number ?? "—"}
                  </p>
                  <InlineCopy
                    value={(record.inherited_tracking_number ?? linkedPkg?.tracking_number ?? "").trim()}
                    label="Tracking #"
                    onToast={onToast}
                  />
                </div>
                {(record.inherited_carrier || linkedPkg?.carrier_name) && (
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Carrier (inherited from package):{" "}
                    <span className="font-semibold text-foreground">{record.inherited_carrier ?? linkedPkg?.carrier_name}</span>
                  </p>
                )}
              </div>
            )}
            {(linkedPkg?.order_id?.trim() || (isLooseItem && record.order_id?.trim())) && (
              <div className="col-span-2">
                <p className="text-xs text-slate-400">
                  {linkedPkg?.order_id?.trim() ? "Amazon order ID (from package)" : "Amazon order ID"}
                </p>
                <div className="group flex flex-wrap items-center gap-2">
                  <p className="font-mono text-sm text-foreground">
                    {(linkedPkg?.order_id ?? record.order_id ?? "").trim() || "—"}
                  </p>
                  <InlineCopy
                    value={(linkedPkg?.order_id ?? record.order_id ?? "").trim()}
                    label="Amazon order ID"
                    onToast={onToast}
                  />
                </div>
              </div>
            )}
          </div>
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-2">Conditions</p>
            <div className="flex flex-wrap gap-1.5">{record.conditions.map((c) => <ConditionBadge key={c} value={c} />)}</div>
          </div>
          {(record.expiration_date || record.batch_number) && (
            <div className="rounded-2xl border border-orange-200 bg-orange-50 p-4 dark:border-orange-700/40 dark:bg-orange-950/30 grid grid-cols-2 gap-3 text-sm">
              {record.expiration_date && <div><p className="text-xs text-slate-400">Expiry</p><p className="font-semibold">{fmt(record.expiration_date)}</p></div>}
              {record.batch_number    && <div><p className="text-xs text-slate-400">Batch #</p><p className="font-mono font-semibold">{record.batch_number}</p></div>}
            </div>
          )}
          {photoTotal > 0 && record.photo_evidence && (
            <div>
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-3">
                Inherited Package Evidence ({photoTotal} photo{photoTotal !== 1 ? "s" : ""})
              </p>
              {/* If we have live File objects from this session, show the gallery with zoom */}
              {sessionPhotos && Object.keys(sessionPhotos).length > 0 ? (
                <PhotoGallery
                  photos={Object.entries(sessionPhotos).flatMap(([cat, fileArr]) =>
                    fileArr.map((f, i) => ({
                      src: URL.createObjectURL(f),
                      label: `${ALL_PHOTO_CATEGORIES[cat]?.label ?? cat} ${i + 1}`,
                    }))
                  )}
                />
              ) : (
                /* After page refresh we only have counts — show category badges + note */
                <>
                  <div className="flex flex-wrap gap-2 mb-2">
                    {Object.entries(photoEvidenceCategoryCounts(record.photo_evidence)).filter(([, n]) => n > 0).map(([cat, n]) => (
                      <span key={cat} className="flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
                        <Camera className="h-3 w-3" />{ALL_PHOTO_CATEGORIES[cat]?.label ?? cat} × {n}
                      </span>
                    ))}
                  </div>
                  <p className="text-[11px] italic text-slate-400">Thumbnails visible when cloud storage is connected.</p>
                </>
              )}
            </div>
          )}
          {record.notes && <p className="rounded-xl bg-slate-50 p-3 text-sm text-slate-700 dark:bg-slate-900 dark:text-slate-300">{record.notes}</p>}
          <div className="rounded-2xl border border-slate-100 bg-slate-50/50 p-4 dark:border-slate-800 dark:bg-slate-900/50 grid grid-cols-2 gap-3 text-xs">
            <div><p className="text-slate-400">By</p><p className="font-semibold capitalize text-slate-700 dark:text-slate-300">{operatorDisplayLabel(record)}</p></div>
            <div><p className="text-slate-400">Date</p><p className="font-semibold text-slate-700 dark:text-slate-300">{fmt(record.created_at)}</p></div>
          </div>
          <div className="flex flex-wrap justify-end gap-3 border-t border-slate-200 pt-4 dark:border-slate-800">
            <button onClick={() => setEditing(true)} className={`${BTN_GHOST} h-12 min-w-[7rem]`}><Pencil className="h-4 w-4" />Edit</button>
            {canDelete(role) && !confirmDel && (
              <button onClick={() => setConfirmDel(true)} className="flex h-12 min-w-[7rem] items-center justify-center gap-2 rounded-2xl border border-rose-200 px-4 text-sm font-semibold text-rose-600 hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30">
                <Trash2 className="h-4 w-4" />Delete
              </button>
            )}
            {confirmDel && (
              <div className="flex w-full flex-wrap items-center justify-end gap-2">
                <p className="mr-auto text-xs text-rose-600">Cannot be undone.</p>
                <button onClick={() => setConfirmDel(false)} className={BTN_GHOST}>Cancel</button>
                <button onClick={handleDelete} disabled={deleting} className="inline-flex h-10 items-center gap-1.5 rounded-xl bg-rose-600 px-4 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-60">
                  {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}Delete
                </button>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ─── Assign Existing Item Modal ────────────────────────────────────────────────

function AssignExistingItemModal({ pkg, allReturns, currentItems, actor, onAssigned, onClose }: {
  pkg: PackageRecord; allReturns: ReturnRecord[]; currentItems: ReturnRecord[];
  actor: string; onAssigned: (updated: ReturnRecord) => void; onClose: () => void;
}) {
  const [search,    setSearch]    = useState("");
  const [selected,  setSelected]  = useState<ReturnRecord | null>(null);
  const [confirm,   setConfirm]   = useState(false);
  const [assigning, setAssigning] = useState(false);
  const currentIds = useMemo(() => new Set(currentItems.map((i) => i.id)), [currentItems]);
  const candidates = useMemo(() => {
    const q = search.trim().toLowerCase();
    return allReturns.filter((r) => !currentIds.has(r.id) && (!q || [r.item_name, r.asin ?? "", r.fnsku ?? "", r.sku ?? ""].join(" ").toLowerCase().includes(q)));
  }, [allReturns, currentIds, search]);

  async function handleAssign(item: ReturnRecord) {
    if (item.package_id && item.package_id !== pkg.id) { setSelected(item); setConfirm(true); return; }
    await doAssign(item);
  }
  async function doAssign(item: ReturnRecord) {
    setAssigning(true);
    const res = await import("./actions").then((m) => m.updateReturn(item.id, { package_id: pkg.id }, actor));
    setAssigning(false);
    if (res.ok && res.data) onAssigned(res.data);
    else setConfirm(false);
  }

  return (
    <div className="fixed inset-0 z-[500] flex items-center justify-center bg-black/60 p-2 sm:p-4 backdrop-blur-sm">
      <div className="flex w-[95vw] max-w-lg flex-col overflow-hidden rounded-2xl sm:rounded-3xl border border-border bg-background shadow-2xl">
        <div className="flex items-center justify-between border-b border-border px-5 py-4">
          <h3 className="font-bold text-foreground">Assign Existing Item</h3>
          <button onClick={onClose} className="rounded-lg p-1 hover:bg-accent"><X className="h-4 w-4 text-muted-foreground" /></button>
        </div>
        <div className="p-4">
          <div className="relative mb-3">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <input autoFocus placeholder="Search item name or RMA…" value={search} onChange={(e) => setSearch(e.target.value)} className={`${INPUT} pl-9`} />
          </div>
          <div className="max-h-72 overflow-y-auto space-y-1">
            {candidates.length === 0 && <p className="py-6 text-center text-sm text-slate-400">{search ? "No matches." : "All items are already in this package."}</p>}
            {candidates.map((r) => (
              <button key={r.id} type="button" onClick={() => handleAssign(r)} disabled={assigning}
                className="flex w-full items-start gap-3 rounded-xl border border-border px-3 py-2.5 text-left hover:bg-accent transition">
                <div className="flex-1 min-w-0">
                  <p className="truncate text-sm font-semibold text-foreground">{r.item_name}</p>
                  <p className="font-mono text-xs text-muted-foreground">{r.asin ?? r.fnsku ?? r.sku ?? "—"}</p>
                </div>
                {r.package_id && r.package_id !== pkg.id
                  ? <span className="shrink-0 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">Move from other pkg</span>
                  : <span className="shrink-0 rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-bold text-slate-500 dark:bg-slate-800 dark:text-slate-400">Unassigned</span>}
              </button>
            ))}
          </div>
        </div>
      </div>
      {confirm && selected && (
        <div className="absolute inset-0 z-10 flex items-center justify-center bg-black/70 p-4">
          <div className="w-full max-w-xs overflow-hidden rounded-3xl border border-amber-200 bg-white shadow-2xl dark:border-amber-700/50 dark:bg-slate-950">
            <div className="bg-amber-50 p-5 dark:bg-amber-950/40">
              <div className="flex items-center gap-3 mb-3"><div className="flex h-10 w-10 items-center justify-center rounded-full bg-amber-100 dark:bg-amber-900/60"><AlertTriangle className="h-5 w-5 text-amber-600" /></div><div><p className="text-sm font-bold text-foreground">Move Item?</p><p className="text-xs text-amber-600 dark:text-amber-400">This item belongs to another package</p></div></div>
              <p className="text-xs text-slate-600 dark:text-slate-300">Moving <span className="font-semibold">{selected.item_name}</span> to <span className="font-mono font-bold">{pkg.package_number}</span>. It will be removed from its current package.</p>
            </div>
            <div className="flex gap-3 p-4">
              <button onClick={() => setConfirm(false)} className="flex h-10 flex-1 items-center justify-center rounded-2xl border border-slate-200 text-sm font-semibold text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300">Cancel</button>
              <button onClick={() => doAssign(selected)} disabled={assigning} className="flex h-10 flex-1 items-center justify-center gap-2 rounded-2xl bg-amber-500 text-sm font-semibold text-white hover:bg-amber-600 disabled:opacity-60">{assigning ? <Loader2 className="h-4 w-4 animate-spin" /> : null}Move Here</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Package Drawer Content ────────────────────────────────────────────────────

export function PackageDrawerContent({ pkg: initPkg, role, actor, openPallets = [], allReturns = [], onClose, onPackageUpdated, onItemAdded, onPackageDeleted, onOpenItem, onOpenPallet, showToast }: {
  pkg: PackageRecord; role: UserRole; actor: string;
  openPallets?: PalletRecord[];
  /** Full returns list from page state — used for the "Assign Existing Item" flow. */
  allReturns?: ReturnRecord[];
  onClose: () => void;
  onPackageUpdated: (p: PackageRecord) => void;
  onItemAdded: (r: ReturnRecord) => void;
  onPackageDeleted: (id: string) => void;
  onOpenItem: (r: ReturnRecord) => void;
  /** Open pallet drawer from wizard (PLT link). */
  onOpenPallet?: (pallet: PalletRecord) => void;
  showToast: (msg: string, kind?: ToastKind) => void;
}) {
  const [pkg,        setPkg]        = useState(initPkg);
  const [items,      setItems]      = useState<ReturnRecord[]>([]);
  const [loading,    setLoading]    = useState(true);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [discOpen,   setDiscOpen]   = useState(false);
  const [assignOpen, setAssignOpen] = useState(false);
  const [closing,    setClosing]    = useState(false);
  const [editing,    setEditing]    = useState(false);
  const [editCarrier,   setEditCarrier]   = useState(initPkg.carrier_name ?? "");
  const [editTracking,  setEditTracking]  = useState(initPkg.tracking_number ?? "");
  const [editRmaNumber, setEditRmaNumber] = useState(initPkg.rma_number ?? "");
  const [editExpected,  setEditExpected]  = useState(String(initPkg.expected_item_count));
  const [editPalletId,  setEditPalletId]  = useState(initPkg.pallet_id ?? "");
  const [editOrderId,   setEditOrderId]   = useState(initPkg.order_id ?? "");
  const editSlipCameraRef = useRef<HTMLInputElement>(null);
  const [saveErr,    setSaveErr]    = useState("");
  const [saving,     setSaving]     = useState(false);

  const [editPhotoClosedUrl,        setEditPhotoClosedUrl]        = useState(() => normalizeEntityPhotoEvidenceUrls(initPkg.photo_evidence)[2] ?? "");
  const [editPhotoOpenedUrl,        setEditPhotoOpenedUrl]        = useState(() => normalizeEntityPhotoEvidenceUrls(initPkg.photo_evidence)[0] ?? "");
  const [editPhotoReturnLabelUrl,   setEditPhotoReturnLabelUrl]   = useState(() => normalizeEntityPhotoEvidenceUrls(initPkg.photo_evidence)[1] ?? "");
  const [editPhotoClosedUploading,  setEditPhotoClosedUploading]  = useState(false);
  const [editPhotoOpenedUploading,  setEditPhotoOpenedUploading]  = useState(false);
  const [editPhotoReturnLabelUploading, setEditPhotoReturnLabelUploading] = useState(false);

  /** When a package is on a pallet, carrier is shared across that pallet's shipment — read from sibling packages. */
  const [palletShipmentCarrier, setPalletShipmentCarrier] = useState<string | null>(null);
  useEffect(() => {
    const raw = (editing ? editPalletId : pkg.pallet_id) ?? "";
    const trimmed = raw.trim();
    if (!trimmed || !isUuidString(trimmed)) {
      setPalletShipmentCarrier(null);
      return;
    }
    let cancelled = false;
    supabaseBrowser
      .from("packages")
      .select("carrier_name")
      .eq("pallet_id", trimmed)
      .not("carrier_name", "is", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle()
      .then(({ data }) => {
        if (cancelled) return;
        setPalletShipmentCarrier(data?.carrier_name ? String(data.carrier_name) : null);
      });
    return () => { cancelled = true; };
  }, [editing, editPalletId, pkg.pallet_id, pkg.id]);

  const packageLinkedToPallet = useMemo(() => {
    const raw = (editing ? editPalletId : pkg.pallet_id) ?? "";
    return Boolean(raw.trim() && isUuidString(raw.trim()));
  }, [editing, editPalletId, pkg.pallet_id]);

  const effectiveCarrierDisplay = useMemo(
    () => (pkg.carrier_name?.trim() || palletShipmentCarrier || null),
    [pkg.carrier_name, palletShipmentCarrier],
  );

  useEffect(() => {
    if (editing) return;
    setPkg(initPkg);
    setEditCarrier(initPkg.carrier_name ?? "");
    setEditTracking(initPkg.tracking_number ?? "");
    setEditRmaNumber(initPkg.rma_number ?? "");
    setEditExpected(String(initPkg.expected_item_count));
    setEditPalletId(initPkg.pallet_id ?? "");
    setEditOrderId(initPkg.order_id ?? "");
    const pe = normalizeEntityPhotoEvidenceUrls(initPkg.photo_evidence);
    setEditPhotoClosedUrl(pe[2] ?? "");
    setEditPhotoOpenedUrl(pe[0] ?? "");
    setEditPhotoReturnLabelUrl(pe[1] ?? "");
  }, [initPkg, editing]);

  // ── Physical scanner for the RMA field (edit mode only) ──────────────────
  const { onKeyDown: pkgRmaKeyDown } = usePhysicalScanner({
    enabled: editing,
    onScan: (code) => setEditRmaNumber(code),
  });

  async function handlePkgClaimPhoto(
    e: React.ChangeEvent<HTMLInputElement>,
    field: "closed" | "opened" | "return_label",
  ) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    const setUploading = field === "closed"        ? setEditPhotoClosedUploading
                       : field === "opened"        ? setEditPhotoOpenedUploading
                       : setEditPhotoReturnLabelUploading;
    const setUrl       = field === "closed"        ? setEditPhotoClosedUrl
                       : field === "opened"        ? setEditPhotoOpenedUrl
                       : setEditPhotoReturnLabelUrl;
    const folder =
      field === "closed" ? "packages/claim_closed"
        : field === "opened" ? "packages/claim_opened"
          : "packages/claim_return_label";
    setUploading(true);
    try {
      const publicUrl = await uploadToStorage(f, folder, pkg.organization_id);
      setUrl(publicUrl);
    } catch {
      setSaveErr("Photo upload failed. Try again.");
    } finally {
      setUploading(false);
    }
  }
  /** Edit mode: manifest OCR in progress / errors (saved immediately on success). */
  const [editManifestOcrRunning, setEditManifestOcrRunning] = useState(false);
  const [editManifestErr, setEditManifestErr] = useState("");

  const reconciliationLines = useMemo((): SlipExpectedItem[] | null => {
    const md = pkg.manifest_data;
    const ex = pkg.expected_items;
    const raw =
      md && Array.isArray(md) && md.length > 0
        ? md
        : ex && Array.isArray(ex) && ex.length > 0
          ? ex
          : null;
    if (!raw) return null;
    return raw.map((it) => ({
      barcode: String(it.sku ?? "").trim(),
      name: String(it.description ?? it.sku ?? "").trim(),
      expected_qty: it.expected_qty ?? 1,
    }));
  }, [pkg.manifest_data, pkg.expected_items]);

  const mismatch   = pkg.expected_item_count > 0 && pkg.actual_item_count !== pkg.expected_item_count;
  const pct        = pkg.expected_item_count > 0 ? Math.min(100, (pkg.actual_item_count / pkg.expected_item_count) * 100) : null;
  const remaining  = pkg.expected_item_count > 0 ? pkg.expected_item_count - pkg.actual_item_count : null;
  const atCapacity = remaining !== null && remaining <= 0;

  useEffect(() => { listReturnsByPackage(pkg.id).then((r) => { if (r.ok) setItems(r.data); setLoading(false); }); }, [pkg.id]);

  /** Manifest upload from Edit mode — appends slip image to `photo_evidence`, saves manifest_data + expected_items. */
  async function handleEditManifestUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file || !file.type.startsWith("image/")) {
      setEditManifestErr("Please choose an image file.");
      return;
    }
    setEditManifestOcrRunning(true);
    setEditManifestErr("");
    try {
      const publicUrl = await uploadToStorage(file, "packages/manifest", pkg.organization_id);
      const items = await mockManifestLineItems(file);
      if (items.length === 0) {
        setEditManifestErr("No items detected on the packing slip — try a clearer photo.");
        return;
      }
      const manifest_data: ExpectedItem[] = items.map((it) => ({
        sku: it.barcode,
        expected_qty: it.expected_qty ?? 1,
        description: it.name,
      }));
      const expected_item_count = manifest_data.reduce((a, it) => a + (it.expected_qty ?? 1), 0);
      const res = await updatePackage(
        pkg.id,
        {
          manifest_data,
          expected_item_count,
          photo_evidence: mergeEntityPhotoEvidence(pkg.photo_evidence, [publicUrl]) ?? undefined,
        },
        actor,
      );
      if (res.ok && res.data) {
        setPkg(res.data);
        onPackageUpdated(res.data);
        setEditExpected(String(res.data.expected_item_count));
      } else {
        setEditManifestErr(res.error ?? "Could not save manifest to the package.");
      }
    } catch (err) {
      setEditManifestErr(err instanceof Error ? err.message : "Upload or OCR failed. Try again.");
    } finally {
      setEditManifestOcrRunning(false);
    }
  }

  function handleItemAdded(r: ReturnRecord) { setItems((p) => [r, ...p]); setPkg((p) => ({ ...p, actual_item_count: p.actual_item_count + 1 })); onItemAdded(r); }
  function handleItemDeleted(id: string) { setItems((p) => p.filter((r) => r.id !== id)); setPkg((p) => ({ ...p, actual_item_count: Math.max(0, p.actual_item_count - 1) })); }

  async function handleSaveEdits() {
    setSaving(true); setSaveErr("");
    const rawPid = editPalletId.trim();
    const linkedPlt = Boolean(rawPid && isUuidString(rawPid));
    const carrierFromPalletFlow = (pkg.carrier_name?.trim() || palletShipmentCarrier || "").trim();
    const carrierPayload = linkedPlt
      ? (carrierFromPalletFlow || undefined)
      : (editCarrier.trim() || undefined);
    const o = editPhotoOpenedUrl.trim();
    const l = editPhotoReturnLabelUrl.trim();
    const c = editPhotoClosedUrl.trim();
    const claimUrls: string[] = [];
    if (o) claimUrls.push(o);
    if (l) claimUrls.push(l);
    if (c) claimUrls.push(c);
    const tail = normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence).slice(3);
    const mergedPe = buildEntityPhotoEvidence([...claimUrls, ...tail]);
    const res = await updatePackage(pkg.id, {
      carrier_name:         carrierPayload,
      tracking_number:      editTracking   || undefined,
      rma_number:           editRmaNumber  || null,
      expected_item_count:  parseInt(editExpected, 10) || 0,
      order_id:             editOrderId.trim() || null,
      ...(editPalletId ? { pallet_id: editPalletId } : {}),
      photo_evidence: mergedPe ?? null,
    }, actor);
    setSaving(false);
    if (res.ok && res.data) { setPkg(res.data); onPackageUpdated(res.data); setEditing(false); }
    else setSaveErr(res.error ?? "Save failed.");
  }

  async function handleClose(discNote?: string) {
    setClosing(true);
    const res = await closePackage(pkg.id, { discrepancyNote: discNote, actor });
    setClosing(false);
    if (res.ok) { setPkg((p) => ({ ...p, status: res.status! })); onPackageUpdated({ ...pkg, status: res.status! }); showToast(res.status === "suspicious" ? "Package flagged — discrepancy recorded." : "Package closed.", res.status === "suspicious" ? "warning" : "success"); setDiscOpen(false); }
    else showToast(res.error ?? "Failed.", "error");
  }

  return (
    <div className="p-4 sm:p-6 space-y-5">
      <div className="flex flex-wrap gap-2">
        <PkgStatusBadge status={pkg.status} />
        {effectiveCarrierDisplay && (
          <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300" title={pkg.pallet_id ? "Carrier (pallet shipment — from this or a sibling package on the pallet)" : undefined}>
            <Truck className="h-3 w-3" />
            {effectiveCarrierDisplay}
          </span>
        )}
        {pkg.tracking_number && (
          <span className="group inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 font-mono text-xs text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400">
            <QrCode className="h-3 w-3" />
            {pkg.tracking_number}
            <InlineCopy value={pkg.tracking_number} label="Tracking #" onToast={showToast} />
          </span>
        )}
        {pkg.rma_number && (
          <span className="group inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-2.5 py-0.5 font-mono text-xs text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300">
            <Tag className="h-3 w-3" />
            {pkg.rma_number}
            <InlineCopy value={pkg.rma_number} label="RMA #" onToast={showToast} />
          </span>
        )}
      </div>

      {/* Count KPI */}
      <div className={`rounded-2xl border p-4 ${atCapacity ? "border-emerald-200 bg-emerald-50 dark:border-emerald-700/60 dark:bg-emerald-950/30" : mismatch ? "border-amber-200 bg-amber-50 dark:border-amber-700/60 dark:bg-amber-950/30" : "border-sky-200 bg-sky-50 dark:border-sky-700/60 dark:bg-sky-950/30"}`}>
        <div className="flex items-baseline gap-2">
          <span className={`text-4xl font-extrabold ${atCapacity ? "text-emerald-600 dark:text-emerald-400" : mismatch ? "text-amber-600 dark:text-amber-400" : "text-sky-600 dark:text-sky-400"}`}>{pkg.actual_item_count}</span>
          {pkg.expected_item_count > 0 && <span className="text-xl text-slate-400">/ {pkg.expected_item_count} expected</span>}
        </div>
        <p className="mt-0.5 text-sm text-muted-foreground">
          {remaining === null ? "No expected count set" : atCapacity ? "Count matches ✓" : remaining > 0 ? `${remaining} more needed` : `${Math.abs(remaining)} over expected`}
        </p>
        {pct !== null && <div className="mt-3 h-2 w-full overflow-hidden rounded-full bg-white/60 dark:bg-slate-900/50"><div className={`h-full rounded-full ${atCapacity ? "bg-emerald-500" : mismatch ? "bg-amber-500" : "bg-sky-500"}`} style={{ width: `${pct}%` }} /></div>}
      </div>

      {pkg.status === "suspicious" && pkg.discrepancy_note && (
        <div className="flex items-start gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-4 dark:border-amber-700/50 dark:bg-amber-950/30">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
          <div><p className="text-xs font-bold text-amber-700 dark:text-amber-300 mb-0.5">Discrepancy Note</p><p className="text-sm text-amber-700 dark:text-amber-400">{pkg.discrepancy_note}</p></div>
        </div>
      )}

      {/* Edit mode */}
      {editing ? (
        <div className="space-y-4 rounded-2xl border border-slate-200 p-4 dark:border-slate-700">
          {/* ── Pallet link at the top (Packages → Pallets hierarchy) ── */}
          <div>
            <label className={LABEL}>Link to Pallet <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            <select className={INPUT} value={editPalletId} onChange={(e) => setEditPalletId(e.target.value)}>
              <option value="">— no pallet —</option>
              {openPallets.map((p) => <option key={p.id} value={p.id}>{p.pallet_number} ({p.item_count} items)</option>)}
            </select>
          </div>
          {packageLinkedToPallet ? (
            <div className="rounded-2xl border border-slate-200 bg-slate-50/90 p-4 dark:border-slate-700 dark:bg-slate-900/50">
              <p className={LABEL}>Carrier</p>
              <p className="text-sm font-semibold text-foreground">{effectiveCarrierDisplay ?? "—"}</p>
              <p className="mt-2 text-xs leading-relaxed text-slate-500 dark:text-slate-400">
                This package is linked to a pallet. Carrier is inherited from the pallet shipment (first package on this pallet with a carrier). To set a different carrier, move the package off the pallet first.
              </p>
            </div>
          ) : (
            <div>
              <label className={LABEL}>Carrier</label>
              <select className={INPUT} value={editCarrier} onChange={(e) => setEditCarrier(e.target.value)}>
                <option value="">Select carrier…</option>
                {CARRIERS.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
          )}
          <div><label className={LABEL}>Tracking # <span className="text-xs font-normal text-slate-400">(unique)</span></label>
            <div className="relative"><QrCode className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
              <input className={`${INPUT} pl-11`} value={editTracking} onChange={(e) => setEditTracking(e.target.value)} placeholder="Scan or type tracking number…" />
            </div>
          </div>
          <div>
            <label className={LABEL}>RMA # <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Tag className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
                <input
                  className={`${INPUT} pl-11`} value={editRmaNumber}
                  onChange={(e) => setEditRmaNumber(e.target.value)}
                  placeholder="Scan or type RMA…"
                  onKeyDown={pkgRmaKeyDown}
                />
              </div>
              <ContextualScanButton
                onDetected={(code) => setEditRmaNumber(code)}
                modalTitle="Scan RMA Number"
              />
            </div>
          </div>
          <div><label className={LABEL}>Expected Item Count</label>
            <input type="number" min="0" className={INPUT} value={editExpected} onChange={(e) => setEditExpected(e.target.value)} placeholder="0" />
          </div>
          <div>
            <label className={LABEL}>Amazon order ID <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            <input
              className={`${INPUT} font-mono`}
              value={editOrderId}
              onChange={(e) => setEditOrderId(e.target.value)}
              placeholder="e.g. 111-1234567-8901234"
              autoComplete="off"
            />
          </div>

          {/* ── Packing slip / manifest (upload here — not in read-only drawer) ── */}
          <div className="rounded-2xl border-2 border-violet-200 bg-violet-50/80 p-4 space-y-3 dark:border-violet-800/50 dark:bg-violet-950/25">
            <div className="flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
              <p className="text-sm font-bold text-violet-800 dark:text-violet-200">Packing slip / manifest</p>
            </div>
            <p className="text-xs text-violet-700 dark:text-violet-300">
              Photo is stored on the package; line items drive Expected vs Scanned reconciliation in the drawer.
            </p>
            <input
              ref={editSlipCameraRef}
              type="file"
              accept="image/*"
              capture="environment"
              className="hidden"
              onChange={handleEditManifestUpload}
            />
            {editManifestOcrRunning ? (
              <div className="flex items-center gap-3 rounded-xl bg-violet-100/80 px-4 py-3 dark:bg-violet-950/40">
                <Loader2 className="h-5 w-5 shrink-0 animate-spin text-violet-500" />
                <span className="text-sm font-semibold text-violet-800 dark:text-violet-200">Analyzing manifest…</span>
              </div>
            ) : (
              <div className="space-y-2">
                <button
                  type="button"
                  onClick={() => editSlipCameraRef.current?.click()}
                  className="flex w-full items-center justify-center gap-2 rounded-xl bg-violet-600 py-3.5 text-sm font-semibold text-white shadow-sm transition hover:bg-violet-700"
                >
                  📸 Take photo of packing list
                </button>
                <button
                  type="button"
                  onClick={() => {
                    void (async () => {
                      const mockItems: SlipExpectedItem[] = [
                        { barcode: "111", name: "Item 111", expected_qty: 1 },
                        { barcode: "222", name: "Item 222", expected_qty: 2 },
                      ];
                      const manifest_data: ExpectedItem[] = mockItems.map((it) => ({
                        sku: it.barcode,
                        expected_qty: it.expected_qty ?? 1,
                        description: it.name,
                      }));
                      const expected_item_count = manifest_data.reduce((a, it) => a + (it.expected_qty ?? 1), 0);
                      const res = await updatePackage(
                        pkg.id,
                        { manifest_data, expected_item_count },
                        actor,
                      );
                      if (res.ok && res.data) {
                        setPkg(res.data);
                        onPackageUpdated(res.data);
                        setEditExpected(String(res.data.expected_item_count));
                      } else {
                        setEditManifestErr(res.error ?? "Could not save mock manifest.");
                      }
                    })();
                  }}
                  className="flex w-full items-center justify-center gap-2 rounded-xl border border-violet-300 bg-white py-2.5 text-xs font-semibold text-violet-700 transition hover:bg-violet-100 dark:border-violet-700/50 dark:bg-slate-900 dark:text-violet-300"
                >
                  🧪 Load mock manifest (test reconciliation)
                </button>
              </div>
            )}
            {(reconciliationLines || normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence).length > 0) && (
              <button
                type="button"
                onClick={() => {
                  void (async () => {
                    const res = await updatePackage(
                      pkg.id,
                      {
                        manifest_data: null,
                        expected_item_count: 0,
                      },
                      actor,
                    );
                    if (res.ok && res.data) {
                      setPkg(res.data);
                      onPackageUpdated(res.data);
                      setEditExpected("0");
                    }
                  })();
                }}
                className="text-[10px] font-semibold text-slate-500 underline hover:text-slate-700"
              >
                Clear manifest data
              </button>
            )}
            {editManifestErr && (
              <p className="text-xs text-rose-600 dark:text-rose-400">
                {editManifestErr}{" "}
                <button type="button" className="font-semibold underline" onClick={() => editSlipCameraRef.current?.click()}>
                  Try again
                </button>
              </p>
            )}
          </div>

          {/* ── Claim Evidence Photos ──────────────────────────────────── */}
          <div className="rounded-2xl border-2 border-rose-200 bg-rose-50 p-4 space-y-3 dark:border-rose-700/50 dark:bg-rose-950/20">
            <div className="flex items-center gap-2">
              <Camera className="h-4 w-4 text-rose-600 dark:text-rose-400" />
              <p className="text-sm font-bold text-rose-800 dark:text-rose-200">Claim Evidence Photos</p>
            </div>
            {editPhotoClosedUrl ? (
              <SavedUrlEvidenceCard
                label="Closed box"
                hint="Optional — claim evidence for the sealed package."
                imageUrl={editPhotoClosedUrl}
                onRemove={() => setEditPhotoClosedUrl("")}
                Icon={Camera}
                iconColor="text-rose-600 dark:text-rose-400"
                footerNote="Stored on this package"
              />
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoClosedUploading ? "border-rose-300 text-rose-500" : "border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100 dark:border-rose-700/60 dark:bg-rose-950/30 dark:text-rose-300"}`}>
                {editPhotoClosedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoClosedUploading ? "Uploading…" : "📸 Closed box (optional)"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoClosedUploading} onChange={(e) => handlePkgClaimPhoto(e, "closed")} />
              </label>
            )}
            {editPhotoOpenedUrl ? (
              <SavedUrlEvidenceCard
                label="Opened box"
                hint="Shows contents — required for most claims."
                imageUrl={editPhotoOpenedUrl}
                onRemove={() => setEditPhotoOpenedUrl("")}
                Icon={Camera}
                iconColor="text-amber-600 dark:text-amber-400"
                required
                footerNote="Stored on this package"
              />
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoOpenedUploading ? "border-amber-300 text-amber-500" : "border-amber-300 bg-amber-50 text-amber-700 hover:bg-amber-100 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300"}`}>
                {editPhotoOpenedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoOpenedUploading ? "Uploading…" : "📸 Opened box"}
                <span className="text-rose-500">*</span>
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoOpenedUploading} onChange={(e) => handlePkgClaimPhoto(e, "opened")} />
              </label>
            )}

            {/* Return shipping label */}
            {editPhotoReturnLabelUrl ? (
              <SavedUrlEvidenceCard
                label="Return label"
                hint="Carrier return label — required for most claims."
                imageUrl={editPhotoReturnLabelUrl}
                onRemove={() => setEditPhotoReturnLabelUrl("")}
                Icon={Camera}
                iconColor="text-violet-600 dark:text-violet-400"
                required
                footerNote="Stored on this package"
              />
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoReturnLabelUploading ? "border-violet-300 text-violet-500" : "border-violet-300 bg-violet-50 text-violet-700 hover:bg-violet-100 dark:border-violet-700/60 dark:bg-violet-950/30 dark:text-violet-300"}`}>
                {editPhotoReturnLabelUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoReturnLabelUploading ? "Uploading…" : "📸 Return label"}
                <span className="text-rose-500">*</span>
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoReturnLabelUploading} onChange={(e) => handlePkgClaimPhoto(e, "return_label")} />
              </label>
            )}
          </div>

          {saveErr && <p className="text-sm text-rose-600 dark:text-rose-400">{saveErr}</p>}
          <div className="flex w-full flex-wrap items-center justify-end gap-2 border-t border-slate-200 pt-4 dark:border-slate-800">
            <button type="button" onClick={() => setEditing(false)} className={BTN_FOOTER_GHOST}><XCircle className="h-4 w-4" />Cancel</button>
            <button type="button" onClick={handleSaveEdits} disabled={saving} className={BTN_FOOTER_PRIMARY}>
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}Save
            </button>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3 text-sm">
          {pkg.rma_number && (
            <div className="col-span-2">
              <p className="text-xs text-slate-400">RMA #</p>
              <div className="group flex flex-wrap items-center gap-2">
                <p className="font-mono font-bold text-foreground">{pkg.rma_number}</p>
                <InlineCopy value={pkg.rma_number} label="RMA #" onToast={showToast} />
              </div>
            </div>
          )}
          <div><p className="text-xs text-slate-400">Operator</p><p className="font-semibold capitalize text-foreground">{operatorDisplayLabel(pkg)}</p></div>
          <div><p className="text-xs text-slate-400">Created</p><p className="font-semibold text-foreground">{fmt(pkg.created_at)}</p></div>
          {pkg.order_id?.trim() && (
            <div className="col-span-2">
              <p className="text-xs text-slate-400">Amazon order ID</p>
              <div className="group flex flex-wrap items-center gap-2">
                <p className="font-mono font-semibold text-foreground">{pkg.order_id.trim()}</p>
                <InlineCopy value={pkg.order_id.trim()} label="Amazon order ID" onToast={showToast} />
              </div>
            </div>
          )}
          {(() => {
            const peUrls = normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence);
            if (peUrls.length === 0) return null;
            const labels = ["Opened box", "Return label", "Closed box"];
            return (
              <div className="col-span-2 space-y-2 pt-1">
                <p className="text-xs text-slate-400">Claim evidence</p>
                <PhotoGallery
                  photos={peUrls.map((src, i) => ({
                    src,
                    label: labels[i] ?? (i === 3 ? "Outer / reference" : `Evidence ${i + 1}`),
                  }))}
                />
              </div>
            );
          })()}
        </div>
      )}

      {/* Actions — bottom bar (Item / Package / Pallet) */}
      {!editing && (
        <div className="space-y-3 border-t border-slate-200 pt-4 dark:border-slate-800">
          <div className="flex flex-wrap justify-end gap-3">
            {pkg.status === "open" && (
              <button
                type="button"
                onClick={() => setWizardOpen(true)}
                className="inline-flex h-12 min-w-[11rem] shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-2xl bg-sky-500 px-4 text-sm font-semibold text-white transition hover:bg-sky-600"
              >
                <ScanLine className="h-4 w-4 shrink-0" />
                Scan New Item
              </button>
            )}
            {pkg.status === "open" && allReturns.length > 0 && (
              <button
                type="button"
                onClick={() => setAssignOpen(true)}
                className="inline-flex h-12 min-w-[11rem] shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-2xl border border-sky-300 bg-sky-50 px-4 text-sm font-semibold text-sky-700 transition hover:bg-sky-100 dark:border-sky-700/60 dark:bg-sky-950/30 dark:text-sky-300 dark:hover:bg-sky-950/50"
              >
                <Plus className="h-4 w-4 shrink-0" />
                Assign Existing
              </button>
            )}
          </div>
          <div className="flex flex-wrap justify-end gap-3">
            <button type="button" onClick={() => setEditing(true)} className={`${BTN_GHOST} h-12 min-w-[9rem] justify-center px-4`}>
              <Pencil className="h-4 w-4 shrink-0" />
              Edit
            </button>
            {pkg.status === "open" && (
              <button
                type="button"
                onClick={() => (mismatch ? setDiscOpen(true) : handleClose())}
                disabled={closing}
                className={`${BTN_GHOST} h-12 min-w-[9rem] justify-center px-4`}
              >
                {closing ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckSquare className="h-4 w-4 shrink-0" />}
                Close
              </button>
            )}
            {canDelete(role) && (
              <button
                type="button"
                onClick={async () => {
                  const r = await deletePackage(pkg.id, actor);
                  if (r.ok) {
                    onPackageDeleted(pkg.id);
                    onClose();
                  } else showToast(r.error ?? "Delete failed.", "error");
                }}
                className="inline-flex h-12 min-w-[9rem] items-center justify-center gap-2 whitespace-nowrap rounded-2xl border border-rose-200 px-4 text-sm font-semibold text-rose-600 transition hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30"
              >
                <Trash2 className="h-4 w-4 shrink-0" />
                Delete
              </button>
            )}
          </div>
        </div>
      )}

      {/* Assign Existing Item Modal */}
      {assignOpen && <AssignExistingItemModal pkg={pkg} allReturns={allReturns} currentItems={items} actor={actor} onAssigned={(updated) => { setItems((p) => [...p.filter((i) => i.id !== updated.id), updated]); showToast(`✓ Item assigned to ${pkg.package_number}`); setAssignOpen(false); }} onClose={() => setAssignOpen(false)} />}

      {/* ── Read-only: reconciliation from saved manifest_data (upload lives in Edit) ── */}
      <div className="rounded-2xl border border-slate-200 bg-slate-50/50 p-4 dark:border-slate-700 dark:bg-slate-900/40">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <p className="text-sm font-bold text-foreground">Packing slip / manifest</p>
            <span className="inline-flex items-center gap-1 rounded-full bg-violet-100 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
              <Sparkles className="h-3 w-3" />
              Read-only
            </span>
          </div>
          {reconciliationLines && normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence).length > 0 && (
            <a
              href={normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence).slice(-1)[0] ?? "#"}
              target="_blank"
              rel="noreferrer"
              className="text-[10px] font-semibold text-sky-600 underline hover:text-sky-700 dark:text-sky-400"
            >
              View latest slip image
            </a>
          )}
        </div>
        {!reconciliationLines && (
          <p className="text-center text-[13px] text-muted-foreground">
            No manifest line items on file. Use <strong>Edit</strong> to photograph or load a packing slip — reconciliation appears here once saved.
          </p>
        )}
        {reconciliationLines && !loading && (
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <p className="text-xs font-bold uppercase tracking-widest text-slate-400">Reconciliation</p>
              <span className="inline-flex items-center gap-1 rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-bold text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
                <Sparkles className="h-2.5 w-2.5" />
                Expected vs Scanned
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-3 text-[10px] font-semibold uppercase tracking-wide">
              <span className="flex items-center gap-1 text-emerald-600">🟢 On slip + scanned</span>
              <span className="flex items-center gap-1 text-rose-600">🔴 On slip, not scanned</span>
              <span className="flex items-center gap-1 text-amber-600">🟡 Scanned, not on slip</span>
            </div>
            <div className="overflow-hidden rounded-2xl border border-border">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Barcode / Name</th>
                    <th className="px-3 py-2 text-center font-bold uppercase tracking-wide text-slate-400">On Slip</th>
                    <th className="px-3 py-2 text-center font-bold uppercase tracking-wide text-slate-400">Scanned</th>
                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                  {reconciliationLines.map((exp, slipIdx) => {
                    const need = exp.expected_qty ?? 1;
                    const matched = items.filter((it) => physicalItemMatchesExpectedLine(it, exp));
                    const isMatch = matched.length >= need;
                    return (
                      <tr key={`slip-${slipIdx}-${exp.barcode}`} className={isMatch ? "bg-emerald-50/70 dark:bg-emerald-950/20" : "bg-rose-50/70 dark:bg-rose-950/20"}>
                        <td className="px-3 py-2.5">
                          <p className="font-mono font-semibold text-slate-700 dark:text-slate-300">{exp.barcode}</p>
                          <p className="text-slate-500">{exp.name}</p>
                        </td>
                        <td className="px-3 py-2.5 text-center font-bold text-slate-600 dark:text-slate-300">{need}</td>
                        <td className="px-3 py-2.5 text-center font-bold">
                          {matched.length > 0
                            ? <span className="text-emerald-600">{matched.length}</span>
                            : <span className="text-rose-500">0</span>}
                        </td>
                        <td className="px-3 py-2.5">
                          {isMatch
                            ? <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300">🟢 Match</span>
                            : <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 px-2 py-0.5 text-[10px] font-bold text-rose-700 dark:bg-rose-900/40 dark:text-rose-300">🔴 Missing</span>}
                        </td>
                      </tr>
                    );
                  })}
                  {items
                    .filter((it) => !reconciliationLines.some((exp) => physicalItemMatchesExpectedLine(it, exp)))
                    .map((it) => (
                      <tr key={it.id} className="bg-amber-50/70 dark:bg-amber-950/20">
                        <td className="px-3 py-2.5">
                          <p className="font-semibold text-slate-700 dark:text-slate-300">{it.item_name}</p>
                          <p className="font-mono text-slate-400">
                            {(it as { product_identifier?: string | null }).product_identifier?.trim()
                              || (it.asin ?? it.fnsku ?? it.sku ?? it.lpn ?? "—")}
                          </p>
                        </td>
                        <td className="px-3 py-2.5 text-center text-slate-400">—</td>
                        <td className="px-3 py-2.5 text-center font-bold text-amber-600">1</td>
                        <td className="px-3 py-2.5">
                          <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">🟡 Scanned, not on slip</span>
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
              {items.length === 0 && reconciliationLines.length > 0 && (
                <p className="py-4 text-center text-xs text-slate-400">
                  No items scanned yet — all {reconciliationLines.length} expected line-items are missing.
                </p>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Items sub-table */}
      <div>
        <p className="mb-2 text-xs font-bold uppercase tracking-widest text-slate-400">Items ({loading ? "…" : items.length})</p>
        {loading ? (
          <div className="flex justify-center py-6"><Loader2 className="h-5 w-5 animate-spin text-slate-400" /></div>
        ) : (
          <ItemsSubTable items={items} role={role} actor={actor} onItemClick={onOpenItem} onItemDeleted={handleItemDeleted} showToast={showToast} />
        )}
      </div>

      {wizardOpen && (() => {
        // Merge saved manifest lines into the package so the wizard's
        // itemMatchesPackageExpectation can check against the real manifest.
        const rawManifest =
          (pkg.manifest_data && pkg.manifest_data.length > 0
            ? pkg.manifest_data
            : pkg.expected_items) ?? [];
        const pkgWithExpected: PackageRecord = {
          ...pkg,
          expected_items: rawManifest.length
            ? rawManifest
            : (reconciliationLines ?? []).map((e: SlipExpectedItem) => ({
                sku: e.barcode,
                expected_qty: e.expected_qty ?? 1,
                description: e.name,
              })),
        };
        return (
          <SingleItemWizardModal
            onClose={() => setWizardOpen(false)}
            onSuccess={(r) => { handleItemAdded(r); }}
            actor={actor}
            organizationId={pkg.organization_id}
            openPackages={[pkgWithExpected]}
            openPallets={openPallets}
            onCreatePackage={() => {}}
            onCreatePallet={() => {}}
            inheritedContext={{ packageId: pkg.id, packageLabel: pkg.package_number, palletId: pkg.pallet_id ?? undefined, palletLabel: openPallets.find((p) => p.id === pkg.pallet_id)?.pallet_number }}
            onSoftPackageWarning={() => showToast("⚠ This item is not on the scanned packing slip.", "warning")}
            onToast={showToast}
            onLinkedPackageUpdated={onPackageUpdated}
            onNavigateToPackage={(id) => { if (id === pkg.id) setWizardOpen(false); }}
            onNavigateToPallet={(palletId) => {
              const plt = openPallets.find((p) => p.id === palletId);
              if (plt) {
                setWizardOpen(false);
                onOpenPallet?.(plt);
              }
            }}
          />
        );
      })()}
      {discOpen && <DiscrepancyModal pkg={pkg} onConfirm={(note) => handleClose(note)} onCancel={() => setDiscOpen(false)} />}
    </div>
  );
}

// ─── Pallet Drawer Content ─────────────────────────────────────────────────────

export function PalletDrawerContent({ pallet, role, actor, organizationId = MVP_ORGANIZATION_ID, packages, onClose, onPalletUpdated, onPalletDeleted, onOpenPackage, showToast }: {
  pallet: PalletRecord; role: UserRole; actor: string;
  /** Workspace org for storage paths and `updatePallet` scoping. */
  organizationId?: string;
  packages: PackageRecord[];
  onClose: () => void;
  onPalletUpdated: (p: PalletRecord) => void;
  onPalletDeleted: (id: string) => void;
  onOpenPackage: (p: PackageRecord) => void;
  showToast: (msg: string, kind?: ToastKind) => void;
}) {
  const orgId = isUuidString((organizationId ?? "").trim()) ? (organizationId ?? "").trim() : MVP_ORGANIZATION_ID;
  const [plt, setPlt] = useState(pallet);
  const [editOpen, setEditOpen] = useState(false);
  const [editStatus, setEditStatus] = useState<PalletStatus>(pallet.status);
  const [editTracking, setEditTracking] = useState(pallet.tracking_number ?? "");
  const [editNotes, setEditNotes] = useState(pallet.notes ?? "");
  const [editSaving, setEditSaving] = useState(false);
  const [bolUploading, setBolUploading] = useState(false);
  const [generalPhotoUploading, setGeneralPhotoUploading] = useState(false);

  useEffect(() => {
    setPlt(pallet);
    setEditStatus(pallet.status);
    setEditTracking(pallet.tracking_number ?? "");
    setEditNotes(pallet.notes ?? "");
  }, [pallet]);

  async function handleClose() {
    const res = await updatePalletStatus(plt.id, "closed", actor);
    if (res.ok) { setPlt((p) => ({ ...p, status: "closed" })); onPalletUpdated({ ...plt, status: "closed" }); showToast("Pallet closed."); }
    else showToast(res.error ?? "Failed.", "error");
  }

  async function handleSaveEdit() {
    setEditSaving(true);
    const res = await updatePallet(
      plt.id,
      {
        status: editStatus,
        tracking_number: editTracking.trim() || null,
        notes: editNotes.trim() || null,
      },
      actor,
      orgId,
    );
    setEditSaving(false);
    if (res.ok && res.data) {
      setPlt(res.data);
      onPalletUpdated(res.data);
      setEditOpen(false);
      showToast("Pallet updated.");
    } else {
      showToast(res.error ?? "Update failed.", "error");
    }
  }

  async function handleBolUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    setBolUploading(true);
    try {
      const url = await uploadToStorage(f, "pallets/bol", orgId);
      const res = await updatePallet(
        plt.id,
        { photo_evidence: mergeEntityPhotoEvidence(plt.photo_evidence, [url]) ?? undefined },
        actor,
        orgId,
      );
      if (res.ok && res.data) {
        setPlt(res.data);
        onPalletUpdated(res.data);
        showToast("Bill of Lading saved.", "success");
      } else {
        showToast(res.error ?? "Could not save BOL.", "error");
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Upload failed.", "error");
    } finally {
      setBolUploading(false);
    }
  }

  async function handleGeneralPalletPhotoUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    setGeneralPhotoUploading(true);
    try {
      const url = await uploadToStorage(f, "pallets", orgId);
      const res = await updatePallet(
        plt.id,
        { photo_evidence: mergeEntityPhotoEvidence(plt.photo_evidence, [url]) ?? undefined },
        actor,
        orgId,
      );
      if (res.ok && res.data) {
        setPlt(res.data);
        onPalletUpdated(res.data);
        showToast("Pallet photo saved.", "success");
      } else {
        showToast(res.error ?? "Could not save pallet photo.", "error");
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Upload failed.", "error");
    } finally {
      setGeneralPhotoUploading(false);
    }
  }

  return (
    <div className="p-4 sm:p-6 space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        <PalletStatusBadge status={plt.status} />
      </div>
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div><p className="text-xs text-slate-400">Total Items</p><p className="text-3xl font-extrabold text-foreground">{plt.item_count}</p></div>
        <div><p className="text-xs text-slate-400">Operator</p><p className="font-semibold capitalize text-foreground">{operatorDisplayLabel(plt)}</p></div>
        <div><p className="text-xs text-slate-400">Created</p><p className="font-semibold">{fmt(plt.created_at)}</p></div>
        <div><p className="text-xs text-slate-400">Updated</p><p className="font-semibold">{fmt(plt.updated_at)}</p></div>
      </div>
      {plt.tracking_number?.trim() && (
        <p className="text-sm">
          <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Tracking </span>
          <span className="font-mono font-semibold text-foreground">{plt.tracking_number}</span>
        </p>
      )}
      {plt.notes && <p className="rounded-xl bg-slate-50 p-3 text-sm text-slate-700 dark:bg-slate-900 dark:text-slate-300">{plt.notes}</p>}
      {normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-2">Pallet photos</p>
          <PhotoGallery
            photos={normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).map((src, i) => ({
              src,
              label: `Photo ${i + 1}`,
            }))}
          />
        </div>
      )}

      {/* Packages sub-table */}
      <div>
        <p className="mb-2 text-xs font-bold uppercase tracking-widest text-slate-400">Packages in this Pallet</p>
        <PackagesSubTable palletId={plt.id} packages={packages} onPackageClick={onOpenPackage} showToast={showToast} />
      </div>

      {/* Pallet info note: drill into packages above to see items */}
      <p className="rounded-xl bg-sky-50 px-4 py-2 text-xs text-sky-700 dark:bg-sky-950/40 dark:text-sky-300">
        💡 Tap a package above to open it and view or scan its items.
      </p>

      <div className="flex flex-wrap items-center justify-end gap-3 border-t border-slate-200 pt-4 dark:border-slate-800">
        {canEdit(role) && (
          <button
            type="button"
            onClick={() => setEditOpen(true)}
            className={`${BTN_GHOST} h-12 min-w-[9rem] justify-center px-4`}
          >
            <Pencil className="h-4 w-4 shrink-0" />
            Edit Pallet
          </button>
        )}
        {canEdit(role) && plt.status === "open" && (
          <button type="button" onClick={handleClose} className={`${BTN_GHOST} h-12 min-w-[9rem] justify-center px-4`}>
            <CheckSquare className="h-4 w-4 shrink-0" />
            Close Pallet
          </button>
        )}
        {canDelete(role) && (
          <button
            type="button"
            onClick={async () => {
              const r = await deletePallet(plt.id, actor);
              if (r.ok) {
                onPalletDeleted(plt.id);
                onClose();
              } else showToast(r.error ?? "Failed.", "error");
            }}
            className="inline-flex h-12 min-w-[9rem] items-center justify-center gap-2 whitespace-nowrap rounded-2xl border border-rose-200 px-4 text-sm font-semibold text-rose-600 transition hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30"
          >
            <Trash2 className="h-4 w-4 shrink-0" />
            Delete
          </button>
        )}
      </div>

      {editOpen && (
        <div className="fixed inset-0 z-[400] flex items-center justify-center bg-black/50 p-3 backdrop-blur-sm" role="dialog" aria-modal="true">
          <div className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-slate-200 bg-white p-5 shadow-xl dark:border-slate-700 dark:bg-slate-950">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-bold text-foreground">Edit Pallet</h3>
              <button type="button" onClick={() => setEditOpen(false)} className="rounded-full p-2 text-slate-400 hover:bg-accent"><X className="h-5 w-5" /></button>
            </div>
            <div className="space-y-4">
              <div>
                <label className={LABEL}>Status</label>
                <select className={INPUT} value={editStatus} onChange={(e) => setEditStatus(e.target.value as PalletStatus)}>
                  {(Object.keys(PALLET_STATUS_CFG) as PalletStatus[]).map((s) => (
                    <option key={s} value={s}>{PALLET_STATUS_CFG[s].label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className={LABEL}>Tracking # <span className="text-xs font-normal text-slate-400">(optional)</span></label>
                <input className={INPUT} value={editTracking} onChange={(e) => setEditTracking(e.target.value)} placeholder="Carrier / inbound tracking…" />
              </div>
              <div>
                <label className={LABEL}>Notes</label>
                <textarea className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" rows={3} value={editNotes} onChange={(e) => setEditNotes(e.target.value)} />
              </div>
              <div>
                <label className={LABEL}>Pallet photo <span className="text-xs font-normal text-slate-400">(optional)</span></label>
                <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-2xl border-2 border-dashed border-amber-300 bg-amber-50 py-3 text-sm font-semibold text-amber-800 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-200 ${generalPhotoUploading ? "opacity-60" : ""}`}>
                  {generalPhotoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                  {generalPhotoUploading ? "Uploading…" : normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).length > 2 ? "Add pallet photo" : "Upload pallet photo"}
                  <input type="file" className="hidden" accept="image/*" capture="environment" onChange={handleGeneralPalletPhotoUpload} disabled={generalPhotoUploading} />
                </label>
                {normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).length > 0 ? (
                  <div className="mt-3 overflow-hidden rounded-xl border border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-800/50">
                    <p className="border-b border-slate-200 px-3 py-2 text-xs font-semibold text-foreground dark:border-slate-700">Latest uploads (see gallery above)</p>
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img
                      src={normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).slice(-1)[0] ?? ""}
                      alt="Pallet"
                      className="max-h-48 w-full object-contain"
                    />
                  </div>
                ) : null}
              </div>
              <div>
                <label className={LABEL}>Bill of Lading (BoL) <span className="text-xs font-normal text-slate-400">(optional)</span></label>
                <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-2xl border-2 border-dashed border-border py-3 text-sm font-semibold ${bolUploading ? "opacity-60" : ""}`}>
                  {bolUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileText className="h-4 w-4" />}
                  {bolUploading ? "Uploading…" : "Upload or replace BoL"}
                  <input type="file" className="hidden" accept="image/*,application/pdf" onChange={handleBolUpload} disabled={bolUploading} />
                </label>
                {(() => {
                  const bol = normalizeEntityPhotoEvidenceUrls(plt.photo_evidence).find((u) => u.toLowerCase().includes(".pdf"))
                    ?? normalizeEntityPhotoEvidenceUrls(plt.photo_evidence)[1];
                  return bol ? (
                  <div className="mt-3 overflow-hidden rounded-xl border border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-800/50">
                    <p className="border-b border-slate-200 px-3 py-2 text-xs font-semibold text-foreground dark:border-slate-700">BoL preview</p>
                    {bol.toLowerCase().includes(".pdf") ? (
                      <a
                        href={bol}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-center gap-2 px-3 py-4 text-sm font-semibold text-sky-600 underline hover:text-sky-700 dark:text-sky-400"
                      >
                        <FileText className="h-5 w-5 shrink-0" /> Open PDF in new tab
                      </a>
                    ) : (
                      // eslint-disable-next-line @next/next/no-img-element
                      <img src={bol} alt="Bill of lading" className="max-h-48 w-full object-contain" />
                    )}
                  </div>
                  ) : null;
                })()}
                <p className="mt-1 text-[10px] text-muted-foreground">Images or PDF — stored per organization.</p>
              </div>
            </div>
            <div className="mt-6 flex w-full flex-row-reverse flex-wrap items-center justify-start gap-2 border-t border-slate-200 pt-4 sm:justify-end dark:border-slate-800">
              <button
                type="button"
                onClick={() => void handleSaveEdit()}
                disabled={editSaving}
                className="inline-flex h-10 min-w-[7rem] shrink-0 items-center justify-center gap-2 rounded-md bg-sky-500 px-4 text-sm font-semibold text-white shadow transition hover:bg-sky-600 disabled:opacity-50 dark:bg-sky-600 dark:hover:bg-sky-500"
              >
                {editSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                {editSaving ? "Saving…" : "Save"}
              </button>
              <button type="button" onClick={() => setEditOpen(false)} className={`${BTN_GHOST} h-10 min-w-[7rem] justify-center px-4`}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── DiscrepancyModal ──────────────────────────────────────────────────────────

export function DiscrepancyModal({ pkg, onConfirm, onCancel }: {
  pkg: PackageRecord; onConfirm: (note: string) => void; onCancel: () => void;
}) {
  const [note, setNote] = useState("");
  const diff = pkg.actual_item_count - pkg.expected_item_count;
  return (
    <div className="fixed inset-0 z-[400] flex items-center justify-center bg-black/60 p-2 sm:p-4 backdrop-blur-sm">
      <div className="w-[95vw] max-w-lg overflow-hidden rounded-2xl sm:rounded-3xl border border-amber-200 bg-white shadow-2xl dark:border-amber-700/50 dark:bg-slate-950">
        <div className="bg-amber-50 p-6 dark:bg-amber-950/40">
          <div className="flex items-center gap-3 mb-4"><div className="flex h-12 w-12 items-center justify-center rounded-full bg-amber-100 dark:bg-amber-900/60"><AlertTriangle className="h-6 w-6 text-amber-600 dark:text-amber-400" /></div><div><h3 className="text-lg font-bold text-foreground">Count Discrepancy</h3><p className="text-sm text-amber-700 dark:text-amber-300">Package will be flagged</p></div></div>
          <div className="grid grid-cols-3 gap-2">
            {[["Expected", pkg.expected_item_count], ["Scanned", pkg.actual_item_count], ["Diff", diff > 0 ? `+${diff}` : diff]].map(([l, v]) => (
              <div key={String(l)} className="rounded-xl bg-white/80 p-3 text-center dark:bg-slate-900/60"><p className="text-2xl font-bold text-foreground">{v}</p><p className="text-xs text-slate-400">{l}</p></div>
            ))}
          </div>
        </div>
        <div className="p-6 space-y-4">
          <div><label className={LABEL}>Note <span className="text-rose-500">*</span></label><textarea value={note} onChange={(e) => setNote(e.target.value)} rows={3} placeholder="Describe the discrepancy…" className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm focus:border-sky-400 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500" /></div>
          <div className="flex gap-3">
            <button onClick={onCancel} className="flex h-12 flex-1 items-center justify-center rounded-2xl border border-slate-200 text-sm font-semibold text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800">Keep Open</button>
            <button onClick={() => onConfirm(note)} disabled={!note.trim()} className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl bg-amber-500 text-sm font-semibold text-white hover:bg-amber-600 disabled:opacity-50"><AlertTriangle className="h-4 w-4" />Flag & Close</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Wizard Steps ──────────────────────────────────────────────────────────────

export function WizardStep1({ state, setState, openPackages, openPallets, onCreatePackage, onCreatePallet, inherited, aiLabelEnabled = false, onAdvance, onNavigateToPackage, onNavigateToPallet }: {
  state: WizardState; setState: React.Dispatch<React.SetStateAction<WizardState>>;
  openPackages: PackageRecord[]; openPallets: PalletRecord[];
  onCreatePackage: () => void; onCreatePallet: () => void;
  inherited?: WizardInheritedContext;
  aiLabelEnabled?: boolean;
  /** Scanner UX: called when Enter is pressed on the last wizard field and all step-1 fields are valid. */
  onAdvance?: () => void;
  onNavigateToPackage?: (packageId: string) => void;
  onNavigateToPallet?: (palletId: string) => void;
}) {
  const up = (k: keyof WizardState, v: unknown) => setState((p) => ({ ...p, [k]: v }));
  const pkgOpts = openPackages.map((p) => ({
    id: p.id,
    label: p.package_number,
    sublabel: `${p.actual_item_count}/${p.expected_item_count > 0 ? p.expected_item_count : "?"} items`,
    tracking: p.tracking_number ?? undefined,
    rma: p.rma_number ?? undefined,
  }));
  const isLooseFlow = !!state.loose_item && !inherited?.packageId;
  const hasPackageLink = !isLooseFlow && !!(inherited?.packageId ?? state.package_link_id);
  const [ocrLoad, setOcrLoad] = useState(false);
  const [ocrBanner, setOcrBanner] = useState<{ ok: boolean; msg: string } | null>(null);
  const ocrFileRef = useRef<HTMLInputElement>(null);
  // Scanner keyboard-nav refs
  const rmaRef     = useRef<HTMLInputElement>(null);
  const itemNameRef = useRef<HTMLInputElement>(null);

  // ── Connected stores for the Store ID dropdown (fetched from stores table) ─
  const [connectedStores, setConnectedStores] = useState<
    { id: string; name: string; platform: string }[]
  >([]);
  const [storeInherited, setStoreInherited] = useState(false);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const res = await listStores();
      if (cancelled || !res.ok || !res.data) return;
      setConnectedStores(
        res.data
          .filter((s) => s.is_active !== false)
          .map((s) => ({ id: s.id, name: s.name, platform: s.platform })),
      );
    })();
    return () => { cancelled = true; };
  }, []);

  // ── Auto-fill store_id from linked package (Package → Item inheritance) ───
  useEffect(() => {
    const pkgId = state.package_link_id || inherited?.packageId;
    if (!pkgId?.trim() || !isUuidString(pkgId.trim())) {
      setStoreInherited(false);
      return;
    }
    let cancelled = false;
    (async () => {
      const local = openPackages.find((p) => p.id === pkgId);
      if (local?.store_id) {
        if (!cancelled) {
          up("store_id", local.store_id);
          setStoreInherited(true);
        }
        return;
      }
      const { data } = await supabaseBrowser
        .from("packages")
        .select("store_id")
        .eq("id", pkgId)
        .eq("organization_id", MVP_ORGANIZATION_ID)
        .maybeSingle();
      if (cancelled) return;
      if (data?.store_id) {
        up("store_id", data.store_id as string);
        setStoreInherited(true);
      } else {
        setStoreInherited(false);
      }
    })();
    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.package_link_id, inherited?.packageId, openPackages]);

  // Keep `returns.marketplace` aligned with the selected Store (fixes Next disabled when only store is set).
  useEffect(() => {
    if (!state.store_id || connectedStores.length === 0) return;
    const store = connectedStores.find((s) => s.id === state.store_id);
    if (!store) return;
    const m = platformToMarketplace(store.platform);
    setState((p) => (p.marketplace === m ? p : { ...p, marketplace: m }));
  }, [state.store_id, connectedStores, setState]);

  const setCatalogResolution = (s: WizardState["catalog_resolution"]) =>
    setState((p) => ({ ...p, catalog_resolution: s }));

  // ── Catalog lookup preview (display only; resolution lives on WizardState) ─
  const [catalogPreview, setCatalogPreview] = useState<{ name: string; price?: number; image_url?: string } | null>(null);
  // ── SP-API mock auto-fill state ───────────────────────────────────────────
  const [spApiStatus, setSpApiStatus] = useState<"idle" | "loading" | "done">("idle");

  async function handleBarcodeLookup(barcode: string) {
    if (!barcode.trim()) { setCatalogResolution("idle"); return; }
    setCatalogResolution("loading");
    setCatalogPreview(null);

    const classified = classifyProductBarcode(barcode.trim());
    if (classified.kind === "fnsku") {
      up("fnsku", classified.normalized);
      up("product_identifier", classified.normalized);
    } else if (classified.kind === "asin") {
      up("asin", classified.normalized);
      up("product_identifier", classified.normalized);
    } else if (classified.kind === "upc_ean") {
      up("product_identifier", classified.normalized);
    } else {
      up("product_identifier", classified.normalized);
    }

    // Auto-detect marketplace from barcode prefix (Amazon FNSKU: X00… / B00…)
    const detectedSource = parseBarcodeSource(barcode.trim(), state.marketplace);
    if (detectedSource && detectedSource !== state.marketplace && (detectedSource === "amazon" || detectedSource === "walmart" || detectedSource === "ebay")) {
      up("marketplace", detectedSource);
    }

    // Smart store_id fallback — only for standalone items (no parent package)
    const isStandalone = !state.package_link_id && !inherited?.packageId;
    if (isStandalone) {
      const upper = barcode.trim().toUpperCase();
      if (upper.startsWith("X00") || upper.startsWith("B00")) {
        // Amazon FNSKU → find first active Amazon store, else use default
        const amazonStore = connectedStores.find(
          (s) => s.platform.toLowerCase() === "amazon",
        );
        const defRaw = getDefaultStoreIdFromStorage().trim();
        const defStore = isUuidString(defRaw) ? defRaw : "";
        up("store_id", amazonStore?.id ?? defStore);
      } else if (!state.store_id) {
        // No known prefix → fallback to operator's saved default store
        const fallbackRaw = getDefaultStoreIdFromStorage().trim();
        const fallback = isUuidString(fallbackRaw) ? fallbackRaw : "";
        if (fallback) up("store_id", fallback);
      }
    }

    // Step A: check local products cache first
    const { data: local } = await supabaseBrowser
      .from("products")
      .select("*")
      .eq("barcode", barcode.trim())
      .maybeSingle();

    if (local) {
      up("item_name", local.name);
      setCatalogPreview({ name: local.name, price: local.price, image_url: local.image_url });
      setCatalogResolution("local");
      return;
    }

    // Step B: call the Amazon adapter
    const amazon = await fetchProductFromAmazon(barcode.trim());
    if (amazon) {
      up("item_name", amazon.name);
      setCatalogPreview({ name: amazon.name, price: amazon.price, image_url: amazon.image_url });
      setCatalogResolution("amazon");
      // Cache the result locally so the next scan is instant
      try {
        await supabaseBrowser
          .from("products")
          .insert({ barcode: barcode.trim(), name: amazon.name, price: amazon.price, image_url: amazon.image_url, source: "Amazon" });
      } catch {
        // ignore cache errors
      }
      return;
    }

    setCatalogResolution("unknown");
  }

  // ── SP-API Phase B mock: auto-fill ASIN / FNSKU / Item Name ──────────────
  async function handleSpApiLookup() {
    if (!state.product_identifier.trim()) return;
    setSpApiStatus("loading");
    await new Promise((r) => setTimeout(r, 1500));
    const clean = state.product_identifier.trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
    const mockAsin  = `B0${clean.slice(0, 6).padEnd(6, "0")}`;
    const mockFnsku = `X00${clean.slice(0, 5).padEnd(5, "0")}`;
    setState((p) => ({
      ...p,
      asin:      mockAsin,
      fnsku:     mockFnsku,
      item_name: p.item_name.trim() || `Amazon Return Product (${state.product_identifier.trim()})`,
    }));
    setSpApiStatus("done");
    setTimeout(() => setSpApiStatus("idle"), 3500);
  }

  // ── Physical hardware scanner — attached directly to the barcode input only ─
  const { onKeyDown: barcodeKeyDown } = usePhysicalScanner({
    onScan: (code) => { up("product_identifier", code); void handleBarcodeLookup(code); },
  });

  async function handleLabelOcr(file: File) {
    setOcrLoad(true); setOcrBanner(null);
    const res = await mockLabelOcr(file);
    setOcrLoad(false);
    if (res.ok && res.data) {
      setState((p) => ({
        ...p,
        lpn: res.data!.lpn,
        product_identifier: res.data!.scan_code ?? p.product_identifier,
        marketplace: res.data!.marketplace as typeof p.marketplace,
      }));
      setOcrBanner({ ok: true, msg: `AI Scan — ${Math.round(res.data.confidence * 100)}% confidence. Verify fields below.` });
    } else {
      setOcrBanner({ ok: false, msg: res.error ?? "Scan failed. Enter manually." });
    }
  }

  const wizardIdIconBtn =
    "inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border border-slate-200 bg-white text-slate-500 transition hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800";

  async function copyWizardCode(v: string) {
    const t = v.trim();
    if (!t) return;
    try {
      await navigator.clipboard.writeText(t);
    } catch {
      /* ignore */
    }
  }

  const wizardStorePlatform = connectedStores.find((s) => s.id === state.store_id)?.platform;

  return (
    <div className="space-y-5">
      {/* ── HIERARCHY: Package link (optional) vs explicit loose item ── */}
      {!inherited?.packageId && (
        <div className="space-y-3">
          <label className="flex cursor-pointer items-start gap-3 rounded-2xl border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-900">
            <input
              type="checkbox"
              className="mt-1 h-4 w-4 shrink-0 rounded border-slate-300"
              checked={state.loose_item}
              onChange={(e) => {
                const v = e.target.checked;
                setState((p) => ({
                  ...p,
                  loose_item: v,
                  package_link_id: v ? "" : p.package_link_id,
                  photo_return_label_url: v ? p.photo_return_label_url : "",
                }));
              }}
            />
            <div>
              <p className="text-sm font-semibold text-foreground">Loose Item (No Box)</p>
              <p className="mt-1 text-xs text-muted-foreground">
                Skip package assignment. In Step 2 you can add an optional return-label photo for this item only.
              </p>
            </div>
          </label>
          {!state.loose_item && (
            <div className="rounded-2xl border-2 border-sky-200 bg-sky-50 p-3 dark:border-sky-700/50 dark:bg-sky-950/30">
              <p className="mb-2 flex items-center gap-2 text-xs font-bold text-sky-700 dark:text-sky-300">
                <Package2 className="h-3.5 w-3.5" />Step 1 — Assign to Package <span className="font-normal text-sky-500">(recommended)</span>
              </p>
              <ComboboxField label="" hint="" icon={Tag} options={pkgOpts} value={state.package_link_id} onChange={(id) => up("package_link_id", id)} onClear={() => up("package_link_id", "")} placeholder="Scan tracking # or search package…" onCreateNew={onCreatePackage} createLabel="Create new package…" />
              {!state.package_link_id && <p className="mt-1.5 text-[10px] text-sky-500">⚠ Without a package this item will be marked <strong>Orphaned / Loose</strong></p>}
            </div>
          )}
          {state.loose_item && (
            <div className="rounded-xl border border-amber-200 bg-amber-50/90 px-3 py-2 text-xs font-medium text-amber-900 dark:border-amber-800/50 dark:bg-amber-950/35 dark:text-amber-200">
              Loose item — no package link. Tracking and carrier are captured at the item / notes level.
            </div>
          )}
        </div>
      )}
      {inherited && (
        <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-700/50 dark:bg-emerald-950/30">
          <CheckCircle2 className="h-5 w-5 shrink-0 text-emerald-600 dark:text-emerald-400" />
          <div className="min-w-0 flex-1">
            <p className="text-xs font-bold text-emerald-700 dark:text-emerald-300">Context inherited — fields pre-filled</p>
            <p className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-emerald-600 dark:text-emerald-400">
              {inherited.packageId && inherited.packageLabel && (
                <>
                  <Tag className="h-3 w-3 shrink-0" />
                  <span>PKG</span>
                  {onNavigateToPackage ? (
                    <button
                      type="button"
                      onClick={() => onNavigateToPackage(inherited.packageId!)}
                      className="font-mono font-bold text-sky-700 underline decoration-sky-400/80 underline-offset-2 hover:text-sky-800 dark:text-sky-300 dark:hover:text-sky-200"
                    >
                      {inherited.packageLabel}
                    </button>
                  ) : (
                    <span className="font-mono font-bold">{inherited.packageLabel}</span>
                  )}
                </>
              )}
              {inherited.palletId && inherited.palletLabel && (
                <>
                  <span className="text-emerald-500/80">·</span>
                  <Boxes className="h-3 w-3 shrink-0" />
                  <span>PLT</span>
                  {onNavigateToPallet ? (
                    <button
                      type="button"
                      onClick={() => onNavigateToPallet(inherited.palletId!)}
                      className="font-mono font-bold text-slate-700 underline decoration-slate-400/80 underline-offset-2 hover:text-slate-900 dark:text-slate-200 dark:hover:text-white"
                    >
                      {inherited.palletLabel}
                    </button>
                  ) : (
                    <span className="font-mono font-bold">{inherited.palletLabel}</span>
                  )}
                </>
              )}
            </p>
          </div>
        </div>
      )}
      {!inherited && state.package_link_id && (
        <div className="rounded-xl border border-sky-200 bg-sky-50/80 px-3 py-2 text-xs dark:border-sky-800/50 dark:bg-sky-950/20">
          <span className="font-semibold text-sky-800 dark:text-sky-200">Linked package: </span>
          {onNavigateToPackage ? (
            <button
              type="button"
              onClick={() => onNavigateToPackage(state.package_link_id)}
              className="font-mono font-bold text-sky-700 underline underline-offset-2 hover:text-sky-900 dark:text-sky-300"
            >
              {openPackages.find((p) => p.id === state.package_link_id)?.package_number ?? state.package_link_id.slice(0, 8) + "…"}
            </button>
          ) : (
            <span className="font-mono font-bold">{openPackages.find((p) => p.id === state.package_link_id)?.package_number ?? "—"}</span>
          )}
        </div>
      )}
      {/* AI Label OCR — premium feature, only shown when org has the flag enabled */}
      {aiLabelEnabled && (
        <div>
          <div className="mb-2 flex items-center gap-2">
            <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-300"><Sparkles className="h-3 w-3" />AI Feature</span>
            <span className="text-xs text-slate-400">Scan the return shipping label to auto-fill fields</span>
          </div>
          {ocrBanner && (
            <div className={`mb-2 flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-medium ${ocrBanner.ok ? "border border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-400" : "border border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"}`}>
              {ocrBanner.ok ? <Sparkles className="h-4 w-4 shrink-0" /> : <XCircle className="h-4 w-4 shrink-0" />}{ocrBanner.msg}
            </div>
          )}
          <button type="button" onClick={() => ocrFileRef.current?.click()} disabled={ocrLoad}
            className="flex w-full items-center justify-center gap-2 rounded-2xl border-2 border-dashed border-violet-300 bg-violet-50 py-3 text-sm font-semibold text-violet-700 transition hover:border-violet-400 hover:bg-violet-100 disabled:opacity-60 dark:border-violet-700/60 dark:bg-violet-950/30 dark:text-violet-300 dark:hover:bg-violet-950/50">
            {ocrLoad ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
            {ocrLoad ? "Scanning label…" : "Photo Return Label (AI Scan)"}
          </button>
          <input ref={ocrFileRef} type="file" accept="image/*" capture="environment" className="hidden"
            onChange={(e) => { const f = e.target.files?.[0]; if (f) handleLabelOcr(f); e.target.value = ""; }} />
        </div>
      )}

      <div>
        <label className={LABEL}>Product barcode (ASIN / UPC / FNSKU) <span className="text-rose-500">*</span></label>
        <div className="flex gap-2">
          <div className="relative flex-1">
            <QrCode className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              className={`${INPUT} pl-11 transition-all ${state.catalog_resolution === "unknown" ? "border-yellow-400 ring-2 ring-yellow-300 focus:border-yellow-400 focus:ring-yellow-300" : ""}`}
              placeholder="Scan or type product identifier…"
              value={state.product_identifier}
              onChange={(e) => {
                setState((p) => ({ ...p, product_identifier: e.target.value, catalog_resolution: "idle" }));
              }}
              autoFocus
              onKeyDown={(e) => {
                barcodeKeyDown(e);
                if (!e.defaultPrevented && e.key === "Enter") { e.preventDefault(); rmaRef.current?.focus(); }
              }}
              onBlur={(e) => { if (e.target.value.trim()) void handleBarcodeLookup(e.target.value); }}
            />
          </div>
          <ContextualScanButton
            onDetected={(code) => { up("product_identifier", code); void handleBarcodeLookup(code); }}
            modalTitle="Scan Product Barcode"
          />
          {/* SP-API auto-fill trigger */}
          <button
            type="button"
            onClick={() => void handleSpApiLookup()}
            disabled={!state.product_identifier.trim() || spApiStatus === "loading"}
            title="Auto-fill ASIN, FNSKU & Item Name from SP-API (mock)"
            aria-label="Fetch product details from SP-API"
            className="inline-flex h-12 items-center gap-1.5 rounded-xl border border-amber-300 bg-amber-50 px-3 text-xs font-semibold text-amber-700 transition hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-300 dark:hover:bg-amber-950/50"
          >
            {spApiStatus === "loading"
              ? <Loader2 className="h-4 w-4 animate-spin shrink-0" />
              : <Zap className="h-4 w-4 shrink-0" />}
            <span className="hidden sm:inline">{spApiStatus === "loading" ? "Fetching…" : "SP-API"}</span>
          </button>
        </div>
        {/* SP-API fetch feedback */}
        {spApiStatus === "loading" && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-300">
            <Loader2 className="h-3.5 w-3.5 animate-spin shrink-0" />
            Fetching product details from SP-API…
          </div>
        )}
        {spApiStatus === "done" && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-300">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" />
            SP-API — ASIN, FNSKU &amp; Item Name auto-filled (mock data)
          </div>
        )}
        {/* Catalog lookup feedback */}
        {state.catalog_resolution === "loading" && spApiStatus === "idle" && (
          <div className="mt-1.5 flex items-center gap-2 text-xs text-slate-400">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />Looking up barcode…
          </div>
        )}
        {state.catalog_resolution === "local" && catalogPreview && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-300">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
            Found in Catalog — {catalogPreview.name}
          </div>
        )}
        {state.catalog_resolution === "amazon" && catalogPreview && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-3 py-1.5 text-xs font-semibold text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/30 dark:text-sky-300">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
            Found on Amazon — {catalogPreview.name}{catalogPreview.price != null ? ` · $${catalogPreview.price.toFixed(2)}` : ""}
          </div>
        )}
        {state.catalog_resolution === "unknown" && (
          <div className="mt-1.5 rounded-xl border-2 border-yellow-400 bg-yellow-50 px-3 py-2 text-xs font-bold text-yellow-800 dark:border-yellow-500/60 dark:bg-yellow-950/20 dark:text-yellow-300">
            ⚠️ Unknown Item — Not found locally or on Amazon. Enter the item name below — you can continue without a catalog match.
          </div>
        )}
        <p className="mt-2 text-[11px] text-slate-500 dark:text-slate-400">
          Smart scan: <span className="font-mono">X00…</span> → FNSKU · <span className="font-mono">B…</span> (10 chars) → ASIN · 8–13 digits → UPC/EAN.
        </p>
      </div>
      {!hasPackageLink && (
        <div>
          <label className={LABEL}>Return label / LPN <span className="ml-1 text-xs font-normal text-slate-400">(optional — orphaned items)</span></label>
          <div className="relative"><Barcode className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" /><input ref={rmaRef} type="text" className={`${INPUT} pl-11`} placeholder="Only if not assigned to a package…" value={state.lpn} onChange={(e) => up("lpn", e.target.value)} /></div>
        </div>
      )}
      {hasPackageLink && (
        <p className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-xs text-sky-800 dark:border-sky-800/60 dark:bg-sky-950/30 dark:text-sky-200">
          Tracking and carrier apply at the <span className="font-semibold">package</span> level — this item inherits them when saved.
        </p>
      )}
      <div>
        <label className={LABEL}>
          Store <span className="text-rose-500">*</span>
          {storeInherited && (
            <span className="ml-2 text-xs font-normal text-emerald-600 dark:text-emerald-400">
              · Inherited from Package
            </span>
          )}
        </label>
        <select
          className={INPUT}
          value={state.store_id}
          onChange={(e) => {
            const id = e.target.value;
            const store = connectedStores.find((s) => s.id === id);
            setState((p) => ({
              ...p,
              store_id: id,
              marketplace: store ? platformToMarketplace(store.platform) : "",
            }));
            setStoreInherited(false);
          }}
          disabled={storeInherited}
        >
          <option value="">— Select Store —</option>
          {connectedStores.map((s) => (
            <option key={s.id} value={s.id}>
              {s.name} ({s.platform})
            </option>
          ))}
        </select>
        {storeInherited && (
          <p className="mt-1 text-[11px] text-emerald-600 dark:text-emerald-400">
            🔒 Locked — store is inherited from the linked package.
          </p>
        )}
        {connectedStores.length === 0 && (
          <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">
            No active stores found. Add a store in Settings → Stores.
          </p>
        )}
      </div>
      <div>
        <label className={LABEL}>Item Name <span className="text-rose-500">*</span></label>
        <input
          ref={itemNameRef} type="text" className={INPUT} placeholder="Product name…"
          value={state.item_name}
          onChange={(e) => {
            const v = e.target.value;
            setState((p) => ({
              ...p,
              item_name: v,
              ...(p.catalog_resolution === "unknown" ? { catalog_resolution: "idle" as const } : {}),
            }));
          }}
          onKeyDown={(e) => { if (e.key === "Enter" && onAdvance) { e.preventDefault(); onAdvance(); } }}
        />
      </div>
      <div className="space-y-4">
        <p className="text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
          ASIN = catalog ID · FNSKU = your FBA label · SKU = Seller / warehouse (MSKU). Each is copyable; search uses the selected store (Amazon / Walmart).
        </p>
        <div>
          <label className={LABEL}>ASIN <span className="text-xs font-normal text-slate-400">(optional)</span></label>
          <div className="flex gap-2">
            <input
              type="text" className={INPUT} placeholder="e.g. B08N5WRWNW"
              value={state.asin} onChange={(e) => up("asin", e.target.value)}
            />
            <button type="button" title="Copy ASIN" className={wizardIdIconBtn} onClick={() => void copyWizardCode(state.asin)} disabled={!state.asin.trim()}><Copy className="h-4 w-4" /></button>
            <button
              type="button"
              title="Open marketplace search"
              className={wizardIdIconBtn}
              disabled={!marketplaceSearchUrl(wizardStorePlatform, state.asin)}
              onClick={() => {
                const url = marketplaceSearchUrl(wizardStorePlatform, state.asin);
                if (url) window.open(url, "_blank", "noopener,noreferrer");
              }}
            >
              <Store className="h-4 w-4" aria-hidden />
            </button>
          </div>
        </div>
        <div>
          <label className={LABEL}>FNSKU <span className="text-xs font-normal text-slate-400">(optional — your label on Amazon)</span></label>
          <div className="flex gap-2">
            <input
              type="text" className={INPUT} placeholder="e.g. X001ABC123"
              value={state.fnsku} onChange={(e) => up("fnsku", e.target.value)}
            />
            <button type="button" title="Copy FNSKU" className={wizardIdIconBtn} onClick={() => void copyWizardCode(state.fnsku)} disabled={!state.fnsku.trim()}><Copy className="h-4 w-4" /></button>
            <button
              type="button"
              title="Open marketplace search"
              className={wizardIdIconBtn}
              disabled={!marketplaceSearchUrl(wizardStorePlatform, state.fnsku)}
              onClick={() => {
                const url = marketplaceSearchUrl(wizardStorePlatform, state.fnsku);
                if (url) window.open(url, "_blank", "noopener,noreferrer");
              }}
            >
              <Store className="h-4 w-4" aria-hidden />
            </button>
          </div>
        </div>
        <div>
          <label className={LABEL}>SKU <span className="text-xs font-normal text-slate-400">(optional — warehouse / Seller Central)</span></label>
          <div className="flex gap-2">
            <input
              type="text" className={INPUT} placeholder="Seller SKU…"
              value={state.sku} onChange={(e) => up("sku", e.target.value)}
            />
            <button type="button" title="Copy SKU" className={wizardIdIconBtn} onClick={() => void copyWizardCode(state.sku)} disabled={!state.sku.trim()}><Copy className="h-4 w-4" /></button>
            <button
              type="button"
              title="Open marketplace search"
              className={wizardIdIconBtn}
              disabled={!marketplaceSearchUrl(wizardStorePlatform, state.sku)}
              onClick={() => {
                const url = marketplaceSearchUrl(wizardStorePlatform, state.sku);
                if (url) window.open(url, "_blank", "noopener,noreferrer");
              }}
            >
              <Store className="h-4 w-4" aria-hidden />
            </button>
          </div>
        </div>
      </div>
      <div>
        <label className={LABEL}>What is wrong with this item? <span className="text-rose-500">*</span></label>
        <p className="mb-3 text-xs text-muted-foreground">Select all that apply. Conflicting options are disabled automatically.</p>
        <div className="flex flex-wrap gap-2">
          {CONDITION_CHIP_DEFS.map((c) => {
            const Icon = c.icon;
            const active = state.condition_keys.includes(c.key);
            const dis = chipDisabledForConditions(c.key, state.condition_keys);
            return (
              <button
                key={c.key}
                type="button"
                disabled={dis}
                onClick={() => setState((p) => ({ ...p, condition_keys: toggleConditionKey(p.condition_keys, c.key) }))}
                className={`inline-flex min-h-[40px] items-center gap-2 rounded-full border-2 px-3 py-2 text-left text-sm font-semibold transition active:scale-[0.99] disabled:cursor-not-allowed disabled:opacity-40 ${active ? `${c.border} ${c.bg} ring-2 ring-primary/30` : "border-border bg-card hover:bg-accent/50"}`}
              >
                <Icon className={`h-5 w-5 shrink-0 ${active ? c.iconColor : "text-muted-foreground"}`} />
                <span className="text-foreground">{c.label}</span>
              </button>
            );
          })}
        </div>
      </div>
      <div className="rounded-2xl border border-orange-200 bg-orange-50 p-4 dark:border-orange-700/40 dark:bg-orange-950/30 space-y-3">
        <p className="flex items-center gap-2 text-sm font-bold text-orange-700 dark:text-orange-400">
          <CalendarX2 className="h-4 w-4" />Expiry &amp; Batch
        </p>
        <p className="text-xs text-orange-600 dark:text-orange-400">
          Enter the expiry date for all perishable items to enable First-Expired, First-Out (FEFO) compliance. Required if the item has an expiry label.
        </p>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={LABEL}>
              Expiration Date
              {state.condition_keys.includes("expired") && <span className="ml-1 text-rose-500">*</span>}
            </label>
            <input type="date" className={INPUT} value={state.expiration_date} onChange={(e) => up("expiration_date", e.target.value)} />
          </div>
          <div>
            <label className={LABEL}>Batch / Lot #</label>
            <input type="text" className={INPUT} placeholder="LOT-2024A…" value={state.batch_number} onChange={(e) => up("batch_number", e.target.value)} />
          </div>
        </div>
      </div>

      <div>
        <label className={LABEL}>Additional Comments / Discrepancy Notes <span className="text-xs font-normal text-slate-400">(optional)</span></label>
        <textarea
          rows={3}
          placeholder="Describe any discrepancy, cosmetic issues, or special observations…"
          value={state.notes}
          onChange={(e) => up("notes", e.target.value)}
          className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm placeholder:text-slate-400 focus:border-sky-400 focus:outline-none focus:ring-2 focus:ring-sky-400/20 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
        />
      </div>
    </div>
  );
}

/** Saved URL preview — same chrome as SmartCameraUpload “complete” (header + 1/1 + thumb + remove). */
function SavedUrlEvidenceCard({
  label,
  hint,
  imageUrl,
  onRemove,
  Icon,
  iconColor,
  required,
  footerNote = "Stored on this return item",
}: {
  label: string;
  hint: string;
  imageUrl: string;
  onRemove: () => void;
  Icon: React.ElementType;
  iconColor: string;
  required?: boolean;
  footerNote?: string;
}) {
  return (
    <div className={["overflow-hidden rounded-2xl border-2 border-emerald-400 bg-white transition dark:border-emerald-600/60 dark:bg-slate-900"].join(" ")}>
      <div className="flex items-start gap-3 border-b border-slate-200 px-4 py-3 dark:border-slate-800">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-emerald-100 dark:bg-emerald-950/40">
          <CheckCircle2 className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">
            {label}
            {required ? <span className="text-rose-500"> *</span> : null}
          </p>
          <p className="mt-0.5 text-[11px] leading-snug text-slate-500 dark:text-slate-400">{hint}</p>
        </div>
        <span className="shrink-0 text-xs font-bold text-emerald-600 dark:text-emerald-400">1/1</span>
      </div>
      <div className="grid grid-cols-4 gap-2 px-4 pt-3">
        <div className="relative col-span-2 aspect-square overflow-hidden rounded-xl border border-slate-200 bg-slate-100 sm:col-span-1 dark:border-slate-700 dark:bg-slate-800">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={imageUrl} alt={label} className="h-full w-full object-contain" />
          <button
            type="button"
            onClick={onRemove}
            className="absolute right-1 top-1 flex h-6 w-6 items-center justify-center rounded-full bg-black/70 text-white shadow-md transition hover:bg-rose-600"
            aria-label="Remove photo"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      <div className="px-4 pb-3 pt-2">
        <p className="text-[10px] text-slate-400 dark:text-slate-500">
          <Icon className={`mr-1 inline h-3 w-3 align-text-bottom ${iconColor}`} />
          {footerNote}
        </p>
      </div>
    </div>
  );
}

export function WizardStep2({
  state,
  setState,
  conditions,
  photoCtx,
  inheritedPackagePhotos,
  packageInheritsBoxPhotos,
  isLooseItem = false,
  actor,
  linkedPackageId,
  linkedPackage,
  onPackageUpdated,
  onToast,
  organizationId = MVP_ORGANIZATION_ID,
}: {
  state: WizardState;
  setState: React.Dispatch<React.SetStateAction<WizardState>>;
  conditions: string[];
  photoCtx?: { hasPackageLink?: boolean; orphanLpn?: boolean; packageInheritsBoxPhotos?: boolean };
  inheritedPackagePhotos: {
    photo_opened_url: string | null;
    photo_closed_url: string | null;
    photo_return_label_url: string | null;
    photo_url?: string | null;
  } | null;
  packageInheritsBoxPhotos: boolean;
  /** No parent box — hide package photo backfill; optional item-level return label only. */
  isLooseItem?: boolean;
  actor: string;
  linkedPackageId?: string;
  linkedPackage?: PackageRecord | null;
  onPackageUpdated?: (p: PackageRecord) => void;
  onToast?: (msg: string, kind?: ToastKind) => void;
  organizationId?: string;
}) {
  const orgId = isUuidString((organizationId ?? "").trim()) ? (organizationId ?? "").trim() : MVP_ORGANIZATION_ID;
  const categories = getCategoriesForConditions(conditions, { ...photoCtx, packageInheritsBoxPhotos, looseItem: isLooseItem });
  const outerBoxUrl = inheritedPackagePhotos?.photo_url?.trim() || null;
  const openedBoxUrl = inheritedPackagePhotos?.photo_opened_url?.trim() || null;
  const closedBoxUrl = inheritedPackagePhotos?.photo_closed_url?.trim() || null;
  const labelUrl = inheritedPackagePhotos?.photo_return_label_url ?? null;
  const showExpiryPhotoSlot = shouldShowExpiryLabelPhoto(state);

  const [pkgOpenedFiles, setPkgOpenedFiles] = useState<File[]>([]);
  const [pkgOuterBoxFiles, setPkgOuterBoxFiles] = useState<File[]>([]);
  const [pkgLabelFiles, setPkgLabelFiles] = useState<File[]>([]);

  const pkgClaimResolved = linkedPackage ? resolvePackageClaimPhotoUrls(linkedPackage) : { opened: null as string | null, label: null as string | null };
  const hasPkgOpenedOnly = !!pkgClaimResolved.opened?.trim();
  const pkgPe = normalizeEntityPhotoEvidenceUrls(linkedPackage?.photo_evidence);
  const hasPkgOuterPhoto = !!pkgPe[3]?.trim();
  const hasPkgReturnLabel = !!pkgClaimResolved.label?.trim();
  const packageMissingMandatoryPhotos = !hasPkgOpenedOnly || !hasPkgReturnLabel;
  const showPackageBackfill =
    !!photoCtx?.hasPackageLink && !!linkedPackageId && packageMissingMandatoryPhotos;
  const showOptionalOuterUpload =
    !!photoCtx?.hasPackageLink && !!linkedPackageId && !hasPkgOuterPhoto;

  async function onPkgOuterBoxFilesChange(files: File[]) {
    setPkgOuterBoxFiles(files);
    if (files.length === 0 || !linkedPackageId) return;
    try {
      const url = await uploadToStorage(files[files.length - 1], "packages", orgId);
      const res = await updatePackage(linkedPackageId, {
        photo_evidence: mergeEntityPhotoEvidence(linkedPackage?.photo_evidence, [url]) ?? undefined,
      }, actor);
      if (res.ok && res.data) {
        onPackageUpdated?.(res.data);
        setPkgOuterBoxFiles([]);
        onToast?.("Outer box photo saved on package.", "success");
      } else {
        onToast?.(res.error ?? "Could not update package", "error");
        setPkgOuterBoxFiles([]);
      }
    } catch (e) {
      onToast?.(e instanceof Error ? e.message : "Upload failed", "error");
      setPkgOuterBoxFiles([]);
    }
  }

  async function onPkgOpenedFilesChange(files: File[]) {
    setPkgOpenedFiles(files);
    if (files.length === 0 || !linkedPackageId) return;
    try {
      const url = await uploadToStorage(files[files.length - 1], "packages/claim_opened", orgId);
      const res = await updatePackage(linkedPackageId, {
        photo_evidence: setPackageClaimEvidenceSlot(linkedPackage?.photo_evidence, 0, url) ?? undefined,
      }, actor);
      if (res.ok && res.data) {
        onPackageUpdated?.(res.data);
        setPkgOpenedFiles([]);
        onToast?.("Opened box photo saved on package.", "success");
      } else {
        onToast?.(res.error ?? "Could not update package", "error");
        setPkgOpenedFiles([]);
      }
    } catch (e) {
      onToast?.(e instanceof Error ? e.message : "Upload failed", "error");
      setPkgOpenedFiles([]);
    }
  }

  async function onPkgLabelFilesChange(files: File[]) {
    setPkgLabelFiles(files);
    if (files.length === 0 || !linkedPackageId) return;
    try {
      const url = await uploadToStorage(files[files.length - 1], "packages/claim_return_label", orgId);
      const res = await updatePackage(linkedPackageId, {
        photo_evidence: setPackageClaimEvidenceSlot(linkedPackage?.photo_evidence, 1, url) ?? undefined,
      }, actor);
      if (res.ok && res.data) {
        onPackageUpdated?.(res.data);
        setPkgLabelFiles([]);
        onToast?.("Return label photo saved on package.", "success");
      } else {
        onToast?.(res.error ?? "Could not update package", "error");
        setPkgLabelFiles([]);
      }
    } catch (e) {
      onToast?.(e instanceof Error ? e.message : "Upload failed", "error");
      setPkgLabelFiles([]);
    }
  }

  return (
    <div className="space-y-5">
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900/80">
        <div className="mb-3 flex items-center gap-2">
          <Camera className="h-4 w-4 text-slate-600 dark:text-slate-300" />
          <p className="text-sm font-bold uppercase tracking-wide text-slate-600 dark:text-slate-300">Evidence</p>
        </div>
        <p className="mb-4 text-xs text-slate-500 dark:text-slate-400">
          {isLooseItem ? (
            <>
              <span className="font-semibold">Loose item</span> — no linked package. Add optional return-label and item photos below; condition photos follow.
            </>
          ) : (
            <>
              Carton and return-label reference shots on the <span className="font-semibold">Package</span> are inherited here. Item-level shots and condition photos are grouped below.
            </>
          )}
        </p>

        <div className="space-y-4 border-t border-slate-200 pt-4 dark:border-slate-600">
          {photoCtx?.hasPackageLink && (outerBoxUrl || openedBoxUrl || closedBoxUrl || labelUrl) && (
            <div>
              <p className="mb-2 text-[10px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">Inherited from linked package</p>
              <div className="grid gap-3 sm:grid-cols-2">
                {outerBoxUrl && (
                  <a href={outerBoxUrl} target="_blank" rel="noreferrer" className="overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-600 dark:bg-slate-950">
                    <div className="flex h-28 w-full items-center justify-center bg-slate-100 dark:bg-slate-900">
                      <img src={outerBoxUrl} alt="Outer box" className="max-h-28 w-full object-contain" />
                    </div>
                    <p className="border-t border-slate-100 px-2 py-1.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:text-slate-300">
                      Package — outer box (optional)
                    </p>
                  </a>
                )}
                {openedBoxUrl && (
                  <a href={openedBoxUrl} target="_blank" rel="noreferrer" className="overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-600 dark:bg-slate-950">
                    <div className="flex h-28 w-full items-center justify-center bg-slate-100 dark:bg-slate-900">
                      <img src={openedBoxUrl} alt="Opened box" className="max-h-28 w-full object-contain" />
                    </div>
                    <p className="border-t border-slate-100 px-2 py-1.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:text-slate-300">
                      Package — opened box
                    </p>
                  </a>
                )}
                {closedBoxUrl && (
                  <a href={closedBoxUrl} target="_blank" rel="noreferrer" className="overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-600 dark:bg-slate-950">
                    <div className="flex h-28 w-full items-center justify-center bg-slate-100 dark:bg-slate-900">
                      <img src={closedBoxUrl} alt="Closed box" className="max-h-28 w-full object-contain" />
                    </div>
                    <p className="border-t border-slate-100 px-2 py-1.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:text-slate-300">
                      Package — closed box
                    </p>
                  </a>
                )}
                {labelUrl && (
                  <a href={labelUrl} target="_blank" rel="noreferrer" className="overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-600 dark:bg-slate-950">
                    <div className="flex h-28 w-full items-center justify-center bg-slate-100 dark:bg-slate-900">
                      <img src={labelUrl} alt="Package return label" className="max-h-28 w-full object-contain" />
                    </div>
                    <p className="border-t border-slate-100 px-2 py-1.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:text-slate-300">
                      Package — return label
                    </p>
                  </a>
                )}
              </div>
            </div>
          )}

          {showOptionalOuterUpload && (
            <div className="rounded-2xl border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950">
              <p className="mb-2 text-[10px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">Optional — outer box</p>
              <SmartCameraUpload
                label="Outer box (optional)"
                hint="Exterior carton — appended to the linked package photo_evidence.urls."
                required={false}
                maxPhotos={1}
                files={pkgOuterBoxFiles}
                onChange={onPkgOuterBoxFilesChange}
                accentClass="border-slate-300 dark:border-slate-700"
                icon={Package2}
                iconColor="text-slate-600 dark:text-slate-400"
              />
            </div>
          )}

          {showPackageBackfill && (
            <div className="rounded-2xl border-2 border-dashed border-amber-300/90 bg-amber-50/90 p-4 ring-1 ring-amber-200/60 dark:border-amber-700/70 dark:bg-amber-950/35 dark:ring-amber-900/40">
              <p className="mb-1 text-xs font-bold uppercase tracking-wide text-amber-900 dark:text-amber-200">
                Quick add for package
              </p>
              <p className="mb-3 text-[11px] leading-snug text-amber-950/90 dark:text-amber-100/90">
                This linked package is missing mandatory claim photos (opened box and/or return label). Capture them here — they are saved on the <span className="font-semibold">package record</span> only (not on this return item).
              </p>
              <div className="space-y-4">
                {!hasPkgOpenedOnly && (
                  <SmartCameraUpload
                    label="Opened box"
                    hint="Interior / opened carton — stored in packages.photo_evidence (opened slot)."
                    required
                    maxPhotos={1}
                    files={pkgOpenedFiles}
                    onChange={onPkgOpenedFilesChange}
                    accentClass="border-amber-200 dark:border-amber-800/50"
                    icon={Package2}
                    iconColor="text-amber-700 dark:text-amber-400"
                  />
                )}
                {!hasPkgReturnLabel && (
                  <SmartCameraUpload
                    label="Return label"
                    hint="Return / RMA label on the carton — stored on the linked package."
                    required
                    maxPhotos={1}
                    files={pkgLabelFiles}
                    onChange={onPkgLabelFilesChange}
                    accentClass="border-sky-200 dark:border-sky-800/50"
                    icon={Barcode}
                    iconColor="text-sky-600 dark:text-sky-400"
                  />
                )}
              </div>
            </div>
          )}

          <div className="space-y-4">
            <p className="text-[10px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">Item-specific photos</p>

            {!isLooseItem && (
              <MasterUploader
                label="Item photo"
                hint="Overall shot of the product (optional). Stored in photo_evidence (item_url)."
                value={state.photo_item_url.trim() ? [state.photo_item_url.trim()] : []}
                onChange={(urls) => setState((p) => ({ ...p, photo_item_url: urls[0] ?? "" }))}
                organizationId={orgId}
                maxFiles={1}
              />
            )}

            {showExpiryPhotoSlot && (
              <MasterUploader
                label="Expiry label photo"
                hint={ALL_PHOTO_CATEGORIES.expiry_label.hint}
                value={state.photo_expiry_url.trim() ? [state.photo_expiry_url.trim()] : []}
                onChange={(urls) => setState((p) => ({ ...p, photo_expiry_url: urls[0] ?? "" }))}
                organizationId={orgId}
                maxFiles={1}
              />
            )}

            {isLooseItem && (
              <MasterUploader
                label="Return label (optional)"
                hint="RMA / return label for this item — stored on this return only (photo_evidence)."
                value={state.photo_return_label_url.trim() ? [state.photo_return_label_url.trim()] : []}
                onChange={(urls) => setState((p) => ({ ...p, photo_return_label_url: urls[0] ?? "" }))}
                organizationId={orgId}
                maxFiles={1}
              />
            )}

            <MasterUploader
              label="Additional evidence (gallery)"
              hint="Optional — multiple images merged into photo_evidence.urls."
              value={state.evidence_gallery_urls}
              onChange={(urls) => setState((p) => ({ ...p, evidence_gallery_urls: urls }))}
              organizationId={orgId}
              maxFiles={24}
            />
          </div>

          {categories.length > 0 &&
            categories.map((cat) => (
              <div key={cat.id} className={`rounded-2xl border p-4 ${cat.accentClass}`}>
                <div className="mb-3 flex items-center gap-2">
                  <cat.icon className={`h-5 w-5 ${cat.iconColor}`} />
                  <div>
                    <p className="text-sm font-bold text-foreground">
                      {cat.label}
                      {!cat.optional ? <span className="text-rose-500"> *</span> : null}
                    </p>
                    <p className="text-xs text-slate-400">{cat.hint}</p>
                  </div>
                </div>
                <SmartCameraUpload
                  label={cat.label}
                  hint={cat.hint}
                  required={!cat.optional}
                  icon={cat.icon}
                  iconColor={cat.iconColor}
                  accentClass={cat.accentClass}
                  files={state.photos[cat.id] ?? []}
                  onChange={(files) => setState((p) => ({ ...p, photos: { ...p.photos, [cat.id]: files } }))}
                />
              </div>
            ))}
        </div>
      </div>

      {categories.length === 0 && (
        <div className="flex flex-col items-center gap-2 py-6 text-center">
          <CheckCircle2 className="h-8 w-8 text-emerald-500" />
          <p className="text-sm font-semibold text-foreground">No extra condition photos required</p>
          <p className="text-xs text-slate-400">Use the item photo above if the issue is visible on the unit.</p>
        </div>
      )}

      <p className="text-center text-[11px] leading-relaxed text-muted-foreground">
        Packing slip / manifest scans belong to <span className="font-semibold text-foreground">Package</span> setup only — not here.
      </p>
    </div>
  );
}

export function WizardStep3({ state, conditions, packages, pallets, inherited, onNotesChange, onAmazonOrderIdChange, packageExpectationMismatch, onToast, onNavigateToPackage, onNavigateToPallet, palletEvidenceFromDb, packageEvidenceFromDb }: {
  state: WizardState; conditions: string[]; packages: PackageRecord[]; pallets: PalletRecord[];
  inherited?: WizardInheritedContext; onNotesChange: (v: string) => void;
  onAmazonOrderIdChange: (v: string) => void;
  /** Soft warning — does not block submit */
  packageExpectationMismatch?: boolean;
  onToast?: (msg: string, kind?: ToastKind) => void;
  onNavigateToPackage?: (packageId: string) => void;
  onNavigateToPallet?: (palletId: string) => void;
  /** Pallet `photo_evidence` when the linked pallet is not fully loaded in `pallets` (DB fetch in wizard). */
  palletEvidenceFromDb?: { photo_evidence?: unknown | null } | null;
  /** Fresh package `photo_evidence` + ids from DB when `openPackages` is stale or incomplete. */
  packageEvidenceFromDb?: {
    photo_evidence?: unknown | null;
    pallet_id?: string | null;
    order_id?: string | null;
  } | null;
}) {
  const photoTotal = Object.values(state.photos).reduce((a, files) => a + files.length, 0);
  const pkgKey = ((inherited?.packageId) ?? state.package_link_id)?.trim() ?? "";
  const pkgFromList = pkgKey ? packages.find((p) => p.id === pkgKey) : undefined;
  const linkedPkg = useMemo((): PackageRecord | undefined => {
    if (!pkgKey) return undefined;
    const fromDb = packageEvidenceFromDb;
    if (pkgFromList && fromDb) {
      return {
        ...pkgFromList,
        photo_evidence:
          fromDb.photo_evidence !== undefined && fromDb.photo_evidence !== null
            ? fromDb.photo_evidence
            : pkgFromList.photo_evidence,
        pallet_id: fromDb.pallet_id ?? pkgFromList.pallet_id,
        order_id: fromDb.order_id ?? pkgFromList.order_id,
      };
    }
    if (pkgFromList) return pkgFromList;
    if (fromDb) {
      return {
        id: pkgKey,
        organization_id: "",
        package_number: "",
        tracking_number: null,
        carrier_name: null,
        expected_item_count: 0,
        actual_item_count: 0,
        pallet_id: fromDb.pallet_id ?? null,
        order_id: fromDb.order_id ?? null,
        status: "open",
        discrepancy_note: null,
        photo_evidence: fromDb.photo_evidence ?? null,
        created_at: "",
        updated_at: "",
      } as PackageRecord;
    }
    return undefined;
  }, [pkgKey, pkgFromList, packageEvidenceFromDb]);
  const palletIdForGallery = (linkedPkg?.pallet_id ?? "").trim();
  const linkedPlt = palletIdForGallery ? pallets.find((p) => p.id === palletIdForGallery) : undefined;

  const [blobMap, setBlobMap] = useState<Record<string, string>>({});
  useEffect(() => {
    const next: Record<string, string> = {};
    for (const [cat, files] of Object.entries(state.photos)) {
      files.forEach((f, i) => {
        const id = `cat:${cat}:${i}`;
        next[id] = URL.createObjectURL(f);
      });
    }
    setBlobMap(next);
    return () => {
      Object.values(next).forEach((u) => URL.revokeObjectURL(u));
    };
  }, [state.photos]);

  /** Read-only thumbnails for scan confirmation — warehouse does not filter claim evidence here. */
  const summaryPhotos = useMemo((): PhotoItem[] => {
    const lines: PhotoItem[] = [];
    const pltUrls = normalizeEntityPhotoEvidenceUrls(
      linkedPlt?.photo_evidence ?? palletEvidenceFromDb?.photo_evidence,
    );
    const pltLabels = ["Pallet — manifest", "Pallet — BOL", "Pallet — overview"];
    pltUrls.forEach((src, i) => {
      lines.push({
        label: pltLabels[i] ?? `Pallet — evidence ${i + 1}`,
        src,
      });
    });
    const pkgUrls = normalizeEntityPhotoEvidenceUrls(linkedPkg?.photo_evidence);
    const pkgLabels = ["Package — opened box", "Package — return label", "Package — closed box"];
    pkgUrls.forEach((src, i) => {
      if (i <= 2) {
        lines.push({ label: pkgLabels[i], src });
      } else if (i === 3) {
        lines.push({ label: "Package — reference", src });
      } else {
        lines.push({ label: `Package — evidence ${i + 1}`, src });
      }
    });
    if (state.photo_item_url?.trim()) {
      lines.push({
        label: "Item photo",
        src: state.photo_item_url.trim(),
      });
    }
    if (state.photo_expiry_url?.trim()) {
      lines.push({
        label: "Expiry label",
        src: state.photo_expiry_url.trim(),
      });
    }
    if (state.loose_item && state.photo_return_label_url?.trim()) {
      lines.push({
        label: "Return label (item)",
        src: state.photo_return_label_url.trim(),
      });
    }
    for (const [cat, files] of Object.entries(state.photos)) {
      files.forEach((f, i) => {
        const id = `cat:${cat}:${i}`;
        const src = blobMap[id];
        if (!src) return;
        lines.push({
          label: `${ALL_PHOTO_CATEGORIES[cat]?.label ?? cat} ${i + 1}`,
          src,
        });
      });
    }
    return lines;
  }, [linkedPlt, linkedPkg, palletEvidenceFromDb, packageEvidenceFromDb, state.loose_item, state.photo_item_url, state.photo_expiry_url, state.photo_return_label_url, state.photos, blobMap]);

  const summaryPhotoCount = summaryPhotos.length;
  const pkgOrder = linkedPkg?.order_id?.trim() ?? "";

  return (
    <div className="space-y-5">
      {packageExpectationMismatch && (
        <div className="flex items-start gap-3 rounded-2xl border border-amber-300 bg-amber-50 p-4 dark:border-amber-600/50 dark:bg-amber-950/40">
          <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-amber-600" />
          <div>
            <p className="text-sm font-bold text-amber-900 dark:text-amber-200">Warning: This item is not on the package&apos;s expected list.</p>
            <p className="mt-1 text-xs text-amber-800/90 dark:text-amber-300/90">You can still submit — this is informational only.</p>
          </div>
        </div>
      )}
      {conditions.length > 0 && (
        <div className="rounded-2xl border border-violet-200 bg-violet-50/95 p-4 dark:border-violet-800/50 dark:bg-violet-950/35">
          <p className="text-xs font-bold uppercase tracking-wide text-violet-700 dark:text-violet-300">Defect reasons (issues)</p>
          <ul className="mt-2 space-y-1.5">
            {conditions.map((c) => (
              <li key={c} className="flex items-start gap-2 text-sm font-semibold text-foreground">
                <span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-violet-500" aria-hidden />
                {CONDITION_CHIP_DEFS.find((d) => d.key === c)?.label ?? c}
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-5 dark:border-slate-700 dark:bg-slate-900 space-y-3">
        <p className="text-xs font-bold uppercase tracking-widest text-slate-400">Review Summary</p>
        <div className="grid grid-cols-2 gap-y-3 gap-x-6 text-sm">
          <div><p className="text-xs text-slate-400">Product ID</p><p className="font-mono font-bold">{state.product_identifier || "—"}</p></div>
          {state.lpn && (
            <div className="group flex flex-wrap items-center gap-2">
              <div>
                <p className="text-xs text-slate-400">LPN (optional)</p>
                <p className="font-mono font-bold">{state.lpn}</p>
              </div>
              <InlineCopy value={state.lpn} label="LPN" onToast={onToast} />
            </div>
          )}
          <div>
            <p className="text-xs text-slate-400">Store</p>
            <p className="font-bold">
              {linkedPkg?.stores?.name
                ? linkedPkg.stores.name
                : state.store_id
                  ? <span className="text-emerald-600 dark:text-emerald-400">Store Assigned ✓</span>
                  : <span className="text-amber-600 dark:text-amber-400">Not assigned</span>}
            </p>
          </div>
          <div className="col-span-2"><p className="text-xs text-slate-400">Item</p><p className="font-bold">{state.item_name}</p></div>
        </div>
        <div>
          <p className="mb-1.5 text-xs text-slate-400">Defect tags</p>
          <div className="flex flex-wrap gap-1.5">{conditions.map((c) => <ConditionBadge key={c} value={c} />)}</div>
        </div>
        {(linkedPkg || linkedPlt) && (
          <div className="flex flex-col gap-2">
            <div className="flex flex-wrap gap-2">
              {linkedPkg && (
                <span className="group inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-2.5 py-0.5 text-xs font-semibold text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/40 dark:text-sky-300">
                  <Tag className="h-3 w-3" />
                  {onNavigateToPackage ? (
                    <button
                      type="button"
                      onClick={() => onNavigateToPackage(linkedPkg.id)}
                      className="font-mono underline decoration-sky-400/80 underline-offset-2 hover:text-sky-900 dark:hover:text-sky-100"
                    >
                      {linkedPkg.package_number}
                    </button>
                  ) : (
                    <span className="font-mono">{linkedPkg.package_number}</span>
                  )}
                  <InlineCopy value={linkedPkg.package_number} label="Package #" onToast={onToast} />
                </span>
              )}
              {linkedPlt && (
                <span className="group inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                  <Boxes className="h-3 w-3" />
                  {onNavigateToPallet ? (
                    <button
                      type="button"
                      onClick={() => onNavigateToPallet(linkedPlt.id)}
                      className="font-mono underline decoration-slate-400/80 underline-offset-2 hover:text-slate-900 dark:hover:text-slate-100"
                    >
                      {linkedPlt.pallet_number}
                    </button>
                  ) : (
                    <span className="font-mono">{linkedPlt.pallet_number}</span>
                  )}
                  <InlineCopy value={linkedPlt.pallet_number} label="Pallet #" onToast={onToast} />
                </span>
              )}
            </div>
            {linkedPkg?.tracking_number && (
              <p className="group text-xs text-muted-foreground">
                <span className="font-semibold text-foreground">Inherits tracking:</span>{" "}
                <span className="font-mono">{linkedPkg.tracking_number}</span>
                <InlineCopy value={linkedPkg.tracking_number} label="Tracking #" className="ml-1 align-middle" onToast={onToast} />
                {linkedPkg.carrier_name ? <span> · {linkedPkg.carrier_name}</span> : null}
              </p>
            )}
          </div>
        )}
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Camera className="h-4 w-4" />
          {summaryPhotoCount > 0
            ? `${summaryPhotoCount} photo${summaryPhotoCount > 1 ? "s" : ""} in summary below`
            : photoTotal > 0
              ? `${photoTotal} condition photo${photoTotal > 1 ? "s" : ""} attached`
              : <span className="text-amber-600 dark:text-amber-400">No photos attached</span>}
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950">
        <p className="mb-1 text-xs font-bold uppercase tracking-widest text-slate-400">Photo summary</p>
        <p className="mb-3 text-xs text-slate-500 dark:text-slate-400">
          Inherited pallet/package shots, item evidence, and condition photos captured in this session — read-only for scan confirmation.
        </p>
        <PhotoGallery photos={summaryPhotos} emptyText="No photos to show." />
      </div>

      <div>
        <label className={LABEL}>
          Amazon order ID
          {pkgOrder ? (
            <span className="text-xs font-normal text-slate-400"> (from package)</span>
          ) : (
            <span className="text-xs font-normal text-slate-400"> (optional)</span>
          )}
        </label>
        <input
          type="text"
          inputMode="text"
          autoComplete="off"
          placeholder="e.g. 111-1234567-8901234"
          value={pkgOrder ? pkgOrder : state.amazon_order_id}
          readOnly={!!pkgOrder}
          onChange={(e) => onAmazonOrderIdChange(e.target.value)}
          className={`w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm font-mono placeholder:text-slate-400 focus:border-sky-400 focus:outline-none dark:border-slate-700 dark:text-slate-100 ${pkgOrder ? "bg-slate-100 dark:bg-slate-800" : "bg-white dark:bg-slate-900"}`}
        />
      </div>
      <div>
        <label className={LABEL}>Operator Notes <span className="text-xs font-normal text-slate-400">(optional)</span></label>
        <textarea rows={3} placeholder="Any additional observations…" value={state.notes} onChange={(e) => onNotesChange(e.target.value)}
          className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm placeholder:text-slate-400 focus:border-sky-400 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
        />
      </div>
    </div>
  );
}

// ─── Single Item Wizard Modal ──────────────────────────────────────────────────

export function SingleItemWizardModal({ onClose, onSuccess, actor, openPackages, openPallets, onCreatePackage, onCreatePallet, inheritedContext, aiLabelEnabled = false, onSoftPackageWarning, onToast, onNavigateToPackage, onNavigateToPallet, onLinkedPackageUpdated, organizationId = MVP_ORGANIZATION_ID }: {
  onClose: () => void;
  /** Called with the saved record AND the in-session photo files for gallery display. */
  onSuccess: (r: ReturnRecord, photos: Record<string, File[]>) => void;
  actor: string; openPackages: PackageRecord[]; openPallets: PalletRecord[];
  onCreatePackage: () => void; onCreatePallet: () => void;
  inheritedContext?: WizardInheritedContext;
  aiLabelEnabled?: boolean;
  /** Non-blocking: called when item may not match package manifest (simulated) — save still proceeds. */
  onSoftPackageWarning?: () => void;
  onToast?: (msg: string, kind?: ToastKind) => void;
  onNavigateToPackage?: (packageId: string) => void;
  onNavigateToPallet?: (palletId: string) => void;
  /** After Step 2 uploads box/label photos to the linked package row. */
  onLinkedPackageUpdated?: (p: PackageRecord) => void;
  /** Workspace org for `returns.organization_id` / claim_submissions (defaults to MVP seed). */
  organizationId?: string;
}) {
  const workspaceOrgId = useMemo(() => {
    const raw = (organizationId ?? "").trim();
    return isUuidString(raw) ? raw : MVP_ORGANIZATION_ID;
  }, [organizationId]);

  const [step, setStep] = useState(1);
  const [state, setState] = useState<WizardState>({ ...EMPTY_WIZARD, package_link_id: inheritedContext?.packageId ?? "" });
  const [submitting, setSubmitting] = useState(false);
  const [submitErr, setSubmitErr] = useState("");
  const [flash, setFlash] = useState(false);
  const [fetchedPkgPhotos, setFetchedPkgPhotos] = useState<{
    photo_evidence: unknown | null;
    /** From DB — may be missing on stale `openPackages` rows; used for pallet gallery + inheritance. */
    pallet_id?: string | null;
    order_id?: string | null;
  } | null>(null);
  /** Pallet photos when package → pallet chain is resolved (matches claim payload family tree). */
  const [fetchedPalletEvidence, setFetchedPalletEvidence] = useState<{
    photo_evidence: unknown | null;
  } | null>(null);
  /** Resolves `marketplace` on submit when Step 1 synced only `store_id` (same source as listStores in WizardStep1). */
  const [wizardStoresForSubmit, setWizardStoresForSubmit] = useState<{ id: string; platform: string }[]>([]);
  useEffect(() => {
    let cancelled = false;
    void listStores().then((res) => {
      if (cancelled || !res.ok || !res.data) return;
      setWizardStoresForSubmit(
        res.data.filter((s) => s.is_active !== false).map((s) => ({ id: s.id, platform: s.platform })),
      );
    });
    return () => { cancelled = true; };
  }, []);

  const isLooseItem = !!state.loose_item && !inheritedContext?.packageId;
  const resolvedPkgId = isLooseItem ? "" : (inheritedContext?.packageId ?? state.package_link_id ?? "");

  useEffect(() => {
    if (!resolvedPkgId?.trim() || !isUuidString(resolvedPkgId.trim())) {
      setFetchedPkgPhotos(null);
      setFetchedPalletEvidence(null);
      return;
    }
    const pkgKey = resolvedPkgId.trim();
    let cancelled = false;

    function applyPalletEvidence(palletId: string | null | undefined) {
      const pid = palletId?.trim() ?? "";
      if (!pid || !isUuidString(pid)) {
        if (!cancelled) setFetchedPalletEvidence(null);
        return;
      }
      const localPlt = openPallets.find((p) => p.id === pid);
      if (localPlt) {
        if (!cancelled) {
          setFetchedPalletEvidence({
            photo_evidence: localPlt.photo_evidence ?? null,
          });
        }
        return;
      }
      void supabaseBrowser
        .from("pallets")
        .select("photo_evidence")
        .eq("id", pid)
        .eq("organization_id", workspaceOrgId)
        .maybeSingle()
        .then(({ data }) => {
          if (cancelled) return;
          if (!data) {
            setFetchedPalletEvidence(null);
            return;
          }
          setFetchedPalletEvidence({
            photo_evidence: (data as { photo_evidence?: unknown }).photo_evidence ?? null,
          });
        });
    }

    /** Always load from DB so pallet_id + photos are not stale vs. cached `openPackages`. */
    void supabaseBrowser
      .from("packages")
      .select("photo_evidence, order_id, pallet_id")
      .eq("id", pkgKey)
      .eq("organization_id", workspaceOrgId)
      .maybeSingle()
      .then(({ data }) => {
        if (cancelled) return;
        if (!data) {
          setFetchedPkgPhotos(null);
          setFetchedPalletEvidence(null);
          return;
        }
        setFetchedPkgPhotos({
          photo_evidence: (data as { photo_evidence?: unknown }).photo_evidence ?? null,
          pallet_id: (data as { pallet_id?: string | null }).pallet_id ?? null,
          order_id: (data as { order_id?: string | null }).order_id ?? null,
        });
        const oid = (data.order_id as string | null | undefined)?.trim();
        if (oid) setState((p) => (p.amazon_order_id.trim() ? p : { ...p, amazon_order_id: oid }));
        applyPalletEvidence((data as { pallet_id?: string | null }).pallet_id ?? null);
      });
    return () => { cancelled = true; };
  }, [resolvedPkgId, openPallets, workspaceOrgId]);

  const inheritedPackagePhotos = useMemo(() => {
    if (!fetchedPkgPhotos) return null;
    const pe = normalizeEntityPhotoEvidenceUrls(fetchedPkgPhotos.photo_evidence);
    return {
      photo_opened_url: pe[0] ?? null,
      photo_closed_url: pe[2] ?? null,
      photo_return_label_url: pe[1] ?? null,
      photo_url: pe[3] ?? null,
    };
  }, [fetchedPkgPhotos]);

  const packageInheritsBoxPhotos = !!(
    inheritedPackagePhotos &&
    inheritedPackagePhotos.photo_opened_url &&
    inheritedPackagePhotos.photo_return_label_url
  );

  const linkedPackageForWizard = useMemo((): PackageRecord | undefined => {
    const id = resolvedPkgId?.trim();
    if (!id) return undefined;
    const base = openPackages.find((p) => p.id === id);
    const pe = fetchedPkgPhotos?.photo_evidence;
    if (base) {
      if (pe !== undefined && pe !== null) return { ...base, photo_evidence: pe };
      return base;
    }
    if (fetchedPkgPhotos) {
      return {
        id,
        organization_id: workspaceOrgId,
        package_number: "",
        tracking_number: null,
        carrier_name: null,
        expected_item_count: 0,
        actual_item_count: 0,
        pallet_id: fetchedPkgPhotos.pallet_id ?? null,
        order_id: fetchedPkgPhotos.order_id ?? null,
        status: "open",
        discrepancy_note: null,
        photo_evidence: fetchedPkgPhotos.photo_evidence ?? null,
        created_at: "",
        updated_at: "",
      } as PackageRecord;
    }
    return undefined;
  }, [resolvedPkgId, openPackages, fetchedPkgPhotos, workspaceOrgId]);

  const conditions = conditionsFromKeys(state.condition_keys);
  const pkgIdForWarn = isLooseItem ? undefined : (inheritedContext?.packageId ?? state.package_link_id) || undefined;
  const pkgForWarn = pkgIdForWarn ? openPackages.find((p) => p.id === pkgIdForWarn) : undefined;
  const packageExpectationMismatch = !!(pkgForWarn && !itemMatchesPackageExpectation(state.item_name, pkgForWarn));

  const hasManualProductIds =
    !!state.asin.trim() ||
    !!state.fnsku.trim() ||
    !!state.sku.trim() ||
    !!state.product_identifier.trim();
  const productIdOk =
    hasManualProductIds ||
    state.catalog_resolution === "unknown" ||
    state.catalog_resolution === "local" ||
    state.catalog_resolution === "amazon";

  const rawPkgLink = (inheritedContext?.packageId ?? state.package_link_id)?.trim() ?? "";
  const packageLinkUuidOk =
    isLooseItem ||
    !rawPkgLink ||
    isUuidString(rawPkgLink);
  const step1Valid =
    packageLinkUuidOk &&
    !!state.store_id &&
    isUuidString(state.store_id.trim()) &&
    !!state.item_name.trim() &&
    productIdOk &&
    state.condition_keys.length > 0 &&
    (!state.condition_keys.includes("expired") || !!state.expiration_date.trim());

  const photoCtxForStep2 = useMemo(
    () => ({
      hasPackageLink: !isLooseItem && !!(inheritedContext?.packageId ?? state.package_link_id),
      orphanLpn: !!state.lpn.trim(),
      packageInheritsBoxPhotos,
    }),
    [isLooseItem, inheritedContext?.packageId, state.package_link_id, state.lpn, packageInheritsBoxPhotos],
  );

  const step2Valid = useMemo(() => {
    const expiryBlocking =
      isExpiryLabelPhotoMandatory(state) && !state.photo_expiry_url.trim();
    if (isLooseItem) {
      return !expiryBlocking;
    }
    const cats = getCategoriesForConditions(conditions, photoCtxForStep2);
    const catsOk = cats.every((c) => c.optional || (state.photos[c.id]?.length ?? 0) > 0);
    return !expiryBlocking && catsOk;
  }, [state.condition_keys, state.expiration_date, state.photo_expiry_url, state.photos, conditions, photoCtxForStep2, isLooseItem]);

  async function handleSubmit() {
    setSubmitting(true); setSubmitErr("");
    const rawLink = (inheritedContext?.packageId ?? state.package_link_id)?.trim() ?? "";
    if (!isLooseItem && rawLink && !isUuidString(rawLink)) {
      setSubmitErr(
        "Package link is invalid. Open Step 1 and pick the package again (search by tracking # — the saved value must be the package record id, not a tracking code).",
      );
      setSubmitting(false);
      return;
    }
    const storeFkMsg = uuidFkInvalidMessage(state.store_id, "Store");
    if (storeFkMsg) {
      setSubmitErr(storeFkMsg);
      onToast?.(storeFkMsg, "error");
      setSubmitting(false);
      return;
    }
    const pkgId = isLooseItem ? undefined : (inheritedContext?.packageId ?? state.package_link_id) || undefined;
    const linkedPkg = pkgId ? openPackages.find((p) => p.id === pkgId) : undefined;
    if (pkgId && onSoftPackageWarning) {
      if (linkedPkg && !itemMatchesPackageExpectation(state.item_name, linkedPkg)) onSoftPackageWarning();
    }
    const categoryCounts = Object.fromEntries(Object.entries(state.photos).map(([k, v]) => [k, v.length])) as Record<string, number>;
    const orderId =
      linkedPkg?.order_id?.trim() ||
      state.amazon_order_id.trim() ||
      undefined;

    try {
      /** Condition-category files (local) — uploaded here so URLs exist for storage; claim payload merges package/pallet from DB. */
      const conditionCategoryUrls: string[] = [];
      for (const [, files] of Object.entries(state.photos)) {
        for (let i = 0; i < files.length; i++) {
          const url = await uploadToIncidentPhotos(files[i], "incident", workspaceOrgId);
          conditionCategoryUrls.push(url);
        }
      }

      const photoEvidence = mergeReturnPhotoEvidence(
        categoryCounts,
        {
          item_url: state.photo_item_url,
          expiry_url: state.photo_expiry_url,
          return_label_url: state.photo_return_label_url,
        },
        { galleryUrls: [...state.evidence_gallery_urls, ...conditionCategoryUrls] },
      );

      const storeRow = wizardStoresForSubmit.find((s) => s.id === state.store_id);
      const marketplaceResolved =
        String(state.marketplace ?? "").trim() ||
        (storeRow ? platformToMarketplace(storeRow.platform) : "amazon");

      const res = await insertReturn({
        organization_id: workspaceOrgId,
        lpn: state.lpn || undefined,
        marketplace: marketplaceResolved,
        item_name: state.item_name,
        conditions,
        asin: state.asin.trim() || state.product_identifier.trim() || undefined,
        fnsku: state.fnsku.trim() || undefined,
        sku: state.sku.trim() || undefined,
        amazon_order_id: orderId,
        notes: state.notes, photo_evidence: photoEvidence ?? undefined,
        expiration_date: state.expiration_date || undefined,
        batch_number: state.batch_number || undefined,
        package_id: pkgId,
        store_id:   state.store_id || undefined,
        created_by: actor,
        claim_evidence_selected_urls: conditionCategoryUrls.length > 0 ? conditionCategoryUrls : undefined,
      });
      if (res.ok && res.data) {
        onSuccess(res.data, state.photos);
        if (res.data.status === "ready_for_claim" && res.data.marketplace?.toLowerCase() === "amazon") {
          onToast?.("Success: return saved and claim queued (ready_to_send) for the agent.", "success");
        } else if (res.data.status === "ready_for_claim") {
          onToast?.("Success: return saved — ready for claim.", "success");
        } else {
          onToast?.("Success: return saved.", "success");
        }
        setFlash(true);
        setTimeout(() => {
          setFlash(false);
          setStep(1);
          setState({
            ...EMPTY_WIZARD,
            package_link_id: inheritedContext?.packageId ?? "",
            loose_item: false,
            catalog_resolution: "idle",
          });
          setSubmitErr("");
        }, 700);
      } else {
        const msg = res.error ?? "Submission failed.";
        setSubmitErr(msg);
        onToast?.(msg, "error");
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Submission failed.";
      setSubmitErr(msg);
      onToast?.(msg, "error");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="fixed inset-0 z-[300] flex items-end justify-center bg-black/50 backdrop-blur-sm sm:items-center sm:p-4">
      <div className={`relative flex h-full w-full max-w-2xl flex-col overflow-hidden rounded-t-3xl bg-white shadow-2xl dark:bg-slate-950 sm:h-auto sm:max-h-[92vh] sm:rounded-3xl sm:border sm:border-slate-200 sm:dark:border-slate-700 ${flash ? "animate-[grow_0.4s_ease]" : ""}`}>
        <div className="flex shrink-0 items-center justify-between border-b border-slate-200 px-4 py-3 sm:px-6 sm:py-4 dark:border-slate-700">
          <div className="flex items-center gap-4">
            <button onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><ArrowLeft className="h-5 w-5" /></button>
            <div>
              <div className="flex items-center gap-2">
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Single Item</p>
                {step === 1 && <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"><ScanLine className="h-3 w-3" />Scanner-Ready</span>}
              </div>
              <p className="font-bold text-foreground">{step === 1 ? "Identification" : step === 2 ? "Photo Evidence" : "Review & Submit"}</p>
            </div>
          </div>
          <StepIndicator step={step} total={3} />
        </div>
        <div className="flex-1 overflow-y-auto p-4 sm:p-6">
          {step === 1 && (
            <WizardStep1
              state={state}
              setState={setState}
              openPackages={openPackages}
              openPallets={openPallets}
              onCreatePackage={onCreatePackage}
              onCreatePallet={onCreatePallet}
              inherited={inheritedContext}
              aiLabelEnabled={aiLabelEnabled}
              onAdvance={step1Valid ? () => setStep(2) : undefined}
              onNavigateToPackage={onNavigateToPackage}
              onNavigateToPallet={onNavigateToPallet}
            />
          )}
          {step === 2 && (
            <WizardStep2
              state={state}
              setState={setState}
              conditions={conditions}
              inheritedPackagePhotos={inheritedPackagePhotos}
              packageInheritsBoxPhotos={packageInheritsBoxPhotos}
              isLooseItem={isLooseItem}
              photoCtx={{
                hasPackageLink: !isLooseItem && !!(inheritedContext?.packageId ?? state.package_link_id),
                orphanLpn: !!state.lpn.trim(),
                packageInheritsBoxPhotos,
              }}
              actor={actor}
              organizationId={workspaceOrgId}
              linkedPackageId={resolvedPkgId || undefined}
              linkedPackage={linkedPackageForWizard}
              onPackageUpdated={onLinkedPackageUpdated}
              onToast={onToast}
            />
          )}
          {step === 3 && (
            <WizardStep3
              state={state}
              conditions={conditions}
              packages={openPackages}
              pallets={openPallets}
              inherited={inheritedContext}
              onNotesChange={(v) => setState((p) => ({ ...p, notes: v }))}
              onAmazonOrderIdChange={(v) => setState((p) => ({ ...p, amazon_order_id: v }))}
              packageExpectationMismatch={packageExpectationMismatch}
              onToast={onToast}
              onNavigateToPackage={onNavigateToPackage}
              onNavigateToPallet={onNavigateToPallet}
              palletEvidenceFromDb={fetchedPalletEvidence}
              packageEvidenceFromDb={fetchedPkgPhotos}
            />
          )}
        </div>
        {submitErr && <p className="shrink-0 px-4 sm:px-6 pb-2 text-sm font-semibold text-rose-600 dark:text-rose-400">{submitErr}</p>}
        <div className="flex shrink-0 items-center justify-between gap-2 border-t border-slate-200 px-4 py-3 sm:px-6 sm:py-4 dark:border-slate-700">
          <div className="flex min-w-0 items-center gap-2">
            {step > 1 && <button type="button" onClick={() => setStep((s) => s - 1)} className={`${BTN_FOOTER_GHOST} px-3`}><ArrowLeft className="h-4 w-4" /></button>}
          </div>
          <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">
            {step < 3 ? (
              <button type="button" onClick={() => setStep((s) => s + 1)} disabled={(step === 1 && !step1Valid) || (step === 2 && !step2Valid)} className={BTN_FOOTER_PRIMARY}>
                Next <ArrowRight className="h-4 w-4" />
              </button>
            ) : (
              <button type="button" onClick={handleSubmit} disabled={submitting} className={BTN_FOOTER_PRIMARY}>
                {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
                {submitting ? "Submitting…" : "Submit return"}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Create Package Modal ──────────────────────────────────────────────────────

export function CreatePackageModal({ onClose, onCreated, actor, openPallets, aiPackingSlipEnabled = false, organizationId = MVP_ORGANIZATION_ID }: {
  onClose: () => void; onCreated: (p: PackageRecord) => void; actor: string; openPallets: PalletRecord[];
  aiPackingSlipEnabled?: boolean;
  organizationId?: string;
}) {
  const [pkgNum, setPkgNum] = useState(generatePackageNumber());
  const [tracking, setTracking] = useState(""); const [carrier, setCarrier] = useState(""); const [expected, setExpected] = useState(""); const [palletId, setPalletId] = useState("");
  const [rmaNumber, setRmaNumber] = useState("");
  const [amazonOrderId, setAmazonOrderId] = useState("");
  const [ocrFile, setOcrFile] = useState<File | null>(null); const [ocrLoad, setOcrLoad] = useState(false); const [ocrDone, setOcrDone] = useState(false);
  const [saving, setSaving] = useState(false); const [error, setError] = useState("");
  /** Box-level claim evidence — stored in `packages.photo_evidence` JSONB (`urls` array). */
  const [boxEvidenceUrls, setBoxEvidenceUrls] = useState<string[]>([]);
  /** Persisted packing-slip image URL (saved with the package row). */
  const [manifestPhotoUrl, setManifestPhotoUrl] = useState<string | null>(null);
  /** Parsed manifest lines — saved as manifest_data JSONB; count rolls up to expected_item_count. */
  const [manifestParsedLines, setManifestParsedLines] = useState<ExpectedItem[] | null>(null);
  const [manifestOcrLoad, setManifestOcrLoad] = useState(false);
  /** Poly / loose shipment — skip mandatory opened-box + return-label photos. */
  const [noBoxMode, setNoBoxMode] = useState(false);
  const manifestFileRef = useRef<HTMLInputElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const palletOptions = openPallets.map((p) => ({
    id: p.id,
    label: p.pallet_number,
    sublabel: `${p.item_count} items`,
    tracking: p.tracking_number ?? undefined,
  }));

  // ── Auto-fill carrier from the pallet's existing packages ─────────────────
  useEffect(() => {
    if (!palletId?.trim() || !isUuidString(palletId.trim())) return;
    supabaseBrowser
      .from("packages")
      .select("carrier_name")
      .eq("pallet_id", palletId)
      .not("carrier_name", "is", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle()
      .then(({ data }) => {
        if (data?.carrier_name) setCarrier(data.carrier_name as string);
      });
  }, [palletId]);

  // ── Store state (Package form) ─────────────────────────────────────────────
  const [pkgStoreId,        setPkgStoreId]        = useState("");
  const [pkgStoreInherited, setPkgStoreInherited] = useState(false);
  const [pkgStoresList,     setPkgStoresList]     = useState<{ id: string; name: string; platform: string }[]>([]);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const res = await listStores();
      if (cancelled || !res.ok || !res.data) return;
      setPkgStoresList(
        res.data
          .filter((s) => s.is_active !== false)
          .map((s) => ({ id: s.id, name: s.name, platform: s.platform })),
      );
    })();
    return () => { cancelled = true; };
  }, []);

  // ── Inherit store_id from pallet (Pallet → Package) — local list or DB fetch ─
  useEffect(() => {
    if (!palletId?.trim() || !isUuidString(palletId.trim())) {
      setPkgStoreInherited(false);
      return;
    }
    let cancelled = false;
    (async () => {
      const local = openPallets.find((p) => p.id === palletId);
      if (local?.store_id) {
        if (!cancelled) {
          setPkgStoreId(local.store_id);
          setPkgStoreInherited(true);
        }
        return;
      }
      const { data } = await supabaseBrowser
        .from("pallets")
        .select("store_id")
        .eq("id", palletId)
        .eq("organization_id", MVP_ORGANIZATION_ID)
        .maybeSingle();
      if (cancelled) return;
      if (data?.store_id) {
        setPkgStoreId(data.store_id as string);
        setPkgStoreInherited(true);
      } else {
        setPkgStoreInherited(false);
      }
    })();
    return () => { cancelled = true; };
  }, [palletId, openPallets]);

  // ── Physical hardware scanner — attached only to the tracking # input ───────
  const { onKeyDown: trackingKeyDown } = usePhysicalScanner({
    onScan: (code) => setTracking(code),
  });

  // ── Physical hardware scanner — attached only to the RMA # input ─────────
  const { onKeyDown: rmaKeyDown } = usePhysicalScanner({
    onScan: (code) => setRmaNumber(code),
  });

  async function handleOcr(file: File) {
    setOcrFile(file);
    setOcrLoad(true);
    setOcrDone(false);
    setError("");
    let manifestLines: ExpectedItem[] = [];
    try {
      const url = await uploadToStorage(file, "packages/manifest", organizationId);
      setManifestPhotoUrl(url);
      const slipItems = await mockManifestLineItems(file);
      manifestLines = slipItems.map((it) => ({
        sku: it.barcode,
        expected_qty: it.expected_qty ?? 1,
        description: it.name,
      }));
      setManifestParsedLines(manifestLines);
      const fromLines = manifestLines.reduce((a, it) => a + (it.expected_qty ?? 1), 0);
      if (fromLines > 0) setExpected(String(fromLines));
    } catch (e) {
      setOcrLoad(false);
      setError(e instanceof Error ? e.message : "Manifest image upload failed.");
      return;
    }
    const res = await mockPackageOcr(file);
    setOcrLoad(false);
    if (res.ok && res.data) {
      const lineCount = manifestLines.reduce((a, it) => a + (it.expected_qty ?? 1), 0);
      setExpected(String(lineCount > 0 ? lineCount : res.data.expected_item_count));
      setCarrier(res.data.carrier_name);
      setTracking(res.data.tracking_number);
      setOcrDone(true);
    } else {
      setError(res.error ?? "OCR failed.");
    }
  }
  async function handleManifestUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    if (f) handleOcr(f);
    e.target.value = "";
  }

  /** Standalone manifest capture (when AI packing slip block is off) — still saves photo + manifest_data. */
  async function handleStandaloneManifestUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file || !file.type.startsWith("image/")) return;
    setManifestOcrLoad(true);
    setError("");
    try {
      const url = await uploadToStorage(file, "packages/manifest", organizationId);
      setManifestPhotoUrl(url);
      const slipItems = await mockManifestLineItems(file);
      const lines: ExpectedItem[] = slipItems.map((it) => ({
        sku: it.barcode,
        expected_qty: it.expected_qty ?? 1,
        description: it.name,
      }));
      setManifestParsedLines(lines);
      const fromLines = lines.reduce((a, it) => a + (it.expected_qty ?? 1), 0);
      if (fromLines > 0) setExpected(String(fromLines));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Manifest upload failed.");
    } finally {
      setManifestOcrLoad(false);
    }
  }
  async function handleCreate() {
    if (!pkgNum.trim()) return;
    if (!pkgStoreId.trim()) {
      setError("Select a store.");
      return;
    }
    const storeMsg = uuidFkInvalidMessage(pkgStoreId, "Store");
    if (storeMsg) {
      setError(storeMsg);
      return;
    }
    const pltMsg = uuidFkInvalidMessage(palletId, "Pallet");
    if (pltMsg) {
      setError(pltMsg);
      return;
    }
    if (!noBoxMode && boxEvidenceUrls.length < 2) {
      setError("Upload at least two box photos (e.g. opened carton + return label), or turn on “No box / poly mailer”.");
      return;
    }
    setSaving(true); setError("");
    const boxUrls = [...boxEvidenceUrls];
    if (manifestPhotoUrl) boxUrls.push(manifestPhotoUrl);
    const res = await createPackage({
      organization_id: organizationId,
      package_number: pkgNum.trim(),
      tracking_number: tracking.trim() || undefined,
      carrier_name: carrier || undefined,
      rma_number: rmaNumber.trim() || undefined,
      order_id: amazonOrderId.trim() || undefined,
      expected_item_count:
        manifestParsedLines && manifestParsedLines.length > 0
          ? manifestParsedLines.reduce((a, it) => a + (it.expected_qty ?? 1), 0)
          : expected
            ? parseInt(expected, 10) || 0
            : 0,
      pallet_id: palletId || undefined,
      store_id: pkgStoreId || undefined,
      created_by: actor,
      ...(manifestParsedLines && manifestParsedLines.length > 0 ? { manifest_data: manifestParsedLines } : {}),
      ...(boxUrls.length > 0 ? { photo_evidence: buildEntityPhotoEvidence(boxUrls) ?? undefined } : {}),
    });
    setSaving(false);
    if (res.ok && res.data) onCreated(res.data); else setError(res.error ?? "Failed.");
  }

  return (
    <div
      className="fixed inset-0 z-[300] flex items-end justify-center bg-black/50 backdrop-blur-sm sm:items-center sm:p-4"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="flex max-h-[92dvh] w-full max-w-lg flex-col overflow-hidden rounded-t-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950 sm:max-h-[88vh] sm:rounded-3xl">
        <div className="shrink-0 border-b border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950 sm:p-6">
          <div className="flex items-center justify-between gap-2">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Batch Flow</p>
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"><ScanLine className="h-3 w-3" />Scanner-Ready</span>
              </div>
              <h2 className="mt-0.5 text-xl font-bold text-foreground">Create Package</h2>
            </div>
            <button type="button" onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
          </div>
        </div>
        <div className="min-h-0 flex-1 max-h-[80vh] overflow-y-auto p-4 sm:p-6 space-y-5">
          {aiPackingSlipEnabled && (
            <>
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-300"><Sparkles className="h-3 w-3" />AI Feature</span>
                <span className="text-xs text-slate-400">Packing Slip OCR</span>
              </div>
              {!ocrFile ? <button type="button" onClick={() => fileRef.current?.click()} className="flex w-full flex-col items-center gap-3 rounded-2xl border-2 border-dashed border-violet-300 bg-violet-50 p-5 transition hover:border-violet-400 hover:bg-violet-100 dark:border-violet-700/60 dark:bg-violet-950/30"><FileImage className="h-10 w-10 text-violet-400" /><p className="text-sm font-semibold text-violet-700 dark:text-violet-300">Photo Packing Slip for AI Scan</p><p className="text-xs text-slate-400">Extracts carrier, tracking, expected count</p></button>
                : ocrLoad ? <div className="flex items-center gap-3 rounded-2xl border border-sky-200 bg-sky-50 p-4 dark:border-sky-700/60 dark:bg-sky-950/30"><Loader2 className="h-5 w-5 animate-spin text-sky-500" /><p className="text-sm font-semibold text-sky-700 dark:text-sky-300">Scanning with AI…</p></div>
                : ocrDone ? <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-700/60 dark:bg-emerald-950/30"><Sparkles className="h-5 w-5 text-emerald-500" /><div className="flex-1"><p className="text-sm font-bold text-emerald-700 dark:text-emerald-300">AI Scan Complete</p><p className="text-xs text-slate-400">Fields pre-filled — override if needed.</p></div><button onClick={() => { setOcrFile(null); setOcrDone(false); setManifestPhotoUrl(null); setManifestParsedLines(null); }} className="text-xs text-slate-400 underline">Rescan</button></div>
                : null}
              <input ref={fileRef} type="file" className="hidden" accept="image/*" capture="environment" onChange={(e) => { const f = e.target.files?.[0]; if (f) handleOcr(f); e.target.value = ""; }} />
            </>
          )}
          <div><label className={LABEL}>Package # <span className="text-rose-500">*</span></label><input type="text" className={INPUT} value={pkgNum} onChange={(e) => setPkgNum(e.target.value)} /></div>
          <div><label className={LABEL}>Carrier</label><select className={INPUT} value={carrier} onChange={(e) => setCarrier(e.target.value)}><option value="">Select…</option>{CARRIERS.map((c) => <option key={c} value={c}>{c}</option>)}</select></div>
          <div>
            <label className={LABEL}>Tracking # <span className="text-xs font-normal text-slate-400">(unique)</span></label>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <QrCode className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
                <input
                  type="text" className={`${INPUT} pl-11`} placeholder="Scan or type…"
                  value={tracking} onChange={(e) => setTracking(e.target.value)}
                  autoFocus
                  onKeyDown={(e) => {
                    trackingKeyDown(e);
                    if (!e.defaultPrevented && e.key === "Enter" && pkgNum.trim() && !saving) { e.preventDefault(); handleCreate(); }
                  }}
                />
              </div>
              <ContextualScanButton
                onDetected={(code) => setTracking(code)}
                modalTitle="Scan Tracking Number"
              />
            </div>
          </div>
          <div>
            <label className={LABEL}>RMA # <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Tag className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
                <input
                  type="text" className={`${INPUT} pl-11`} placeholder="Scan or type RMA…"
                  value={rmaNumber} onChange={(e) => setRmaNumber(e.target.value)}
                  onKeyDown={rmaKeyDown}
                />
              </div>
              <ContextualScanButton
                onDetected={(code) => setRmaNumber(code)}
                modalTitle="Scan RMA Number"
              />
            </div>
          </div>
          <div>
            <label className={LABEL}>Amazon order ID <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            <input
              type="text"
              inputMode="text"
              autoComplete="off"
              className={`${INPUT} font-mono`}
              placeholder="e.g. 111-1234567-8901234"
              value={amazonOrderId}
              onChange={(e) => setAmazonOrderId(e.target.value)}
            />
            <p className="mt-1 text-[11px] text-slate-400">Linked return items inherit this for claims.</p>
          </div>
          <div><label className={LABEL}>Expected Items</label><input type="number" min="0" className={INPUT} placeholder="0" value={expected} onChange={(e) => setExpected(e.target.value)} /></div>
          <ComboboxField label="Link to Pallet" hint="(optional)" icon={Boxes} options={palletOptions} value={palletId} onChange={setPalletId} onClear={() => setPalletId("")} placeholder="Search pallets…" />

          <div>
            <label className={LABEL}>
              Store <span className="text-rose-500">*</span>
              {pkgStoreInherited && (
                <span className="ml-2 text-xs font-normal text-emerald-600 dark:text-emerald-400">
                  · Inherited from Pallet
                </span>
              )}
            </label>
            <select
              className={INPUT}
              value={pkgStoreId}
              onChange={(e) => { setPkgStoreId(e.target.value); setPkgStoreInherited(false); }}
              disabled={pkgStoreInherited}
            >
              <option value="">— Select Store —</option>
              {pkgStoresList.map((s) => (
                <option key={s.id} value={s.id}>{s.name} ({s.platform})</option>
              ))}
            </select>
            {pkgStoreInherited && (
              <p className="mt-1 text-[11px] text-emerald-600 dark:text-emerald-400">
                🔒 Locked — store is inherited from the selected pallet.
              </p>
            )}
            {pkgStoresList.length === 0 && (
              <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">
                No active stores found. Add a store in Settings → Stores.
              </p>
            )}
          </div>

          {/* ── Packing slip / manifest (when AI packing-slip block above is disabled) ── */}
          {!aiPackingSlipEnabled && (
            <div className="rounded-2xl border-2 border-violet-200 bg-violet-50/80 p-4 space-y-3 dark:border-violet-800/50 dark:bg-violet-950/25">
              <div className="flex items-center gap-2">
                <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
                <p className="text-sm font-bold text-violet-800 dark:text-violet-200">Packing slip / manifest</p>
              </div>
              <p className="text-xs text-violet-700 dark:text-violet-300">
                Optional — photo and parsed lines are saved on the package for reconciliation.
              </p>
              <input
                ref={manifestFileRef}
                type="file"
                accept="image/*"
                capture="environment"
                className="hidden"
                onChange={handleStandaloneManifestUpload}
              />
              {manifestOcrLoad ? (
                <div className="flex items-center gap-2 rounded-xl bg-violet-100/80 px-3 py-2 dark:bg-violet-950/40">
                  <Loader2 className="h-4 w-4 animate-spin text-violet-500" />
                  <span className="text-sm font-semibold text-violet-800 dark:text-violet-200">Processing manifest…</span>
                </div>
              ) : (
                <div className="space-y-2">
                  <button
                    type="button"
                    onClick={() => manifestFileRef.current?.click()}
                    className="flex w-full items-center justify-center gap-2 rounded-xl bg-violet-600 py-3 text-sm font-semibold text-white hover:bg-violet-700"
                  >
                    📸 Scan packing slip / manifest
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      const lines: ExpectedItem[] = [
                        { sku: "111", expected_qty: 1, description: "Item 111" },
                        { sku: "222", expected_qty: 2, description: "Item 222" },
                      ];
                      setManifestParsedLines(lines);
                      setExpected(String(lines.reduce((a, it) => a + (it.expected_qty ?? 1), 0)));
                    }}
                    className="flex w-full items-center justify-center gap-2 rounded-xl border border-violet-300 bg-white py-2.5 text-xs font-semibold text-violet-700 dark:border-violet-700/50 dark:bg-slate-900 dark:text-violet-300"
                  >
                    🧪 Load mock manifest (test)
                  </button>
                </div>
              )}
              {manifestParsedLines && manifestParsedLines.length > 0 && (
                <p className="text-[11px] font-semibold text-emerald-700 dark:text-emerald-400">
                  ✓ {manifestParsedLines.length} manifest line(s) — will save with the package.
                </p>
              )}
            </div>
          )}

          {/* ── Claim Evidence Photos (JSONB `photo_evidence.urls` only) ───────────── */}
          <div className="rounded-2xl border-2 border-rose-200 bg-rose-50 p-4 space-y-3 dark:border-rose-700/50 dark:bg-rose-950/20">
            <div className="flex flex-wrap items-center gap-2">
              <Camera className="h-4 w-4 text-rose-600 dark:text-rose-400" />
              <p className="text-sm font-bold text-rose-800 dark:text-rose-200">Claim Evidence Photos</p>
              <span className="ml-auto rounded-full bg-rose-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-rose-600 dark:bg-rose-900/40 dark:text-rose-300">Box Level</span>
            </div>
            <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-rose-200 bg-white/80 p-3 dark:border-rose-800/50 dark:bg-slate-900/40">
              <input
                type="checkbox"
                className="mt-1 h-4 w-4 shrink-0 rounded border-slate-300"
                checked={noBoxMode}
                onChange={(e) => setNoBoxMode(e.target.checked)}
              />
              <div>
                <p className="text-sm font-semibold text-rose-900 dark:text-rose-100">No box / poly mailer</p>
                <p className="mt-0.5 text-[11px] text-rose-700/90 dark:text-rose-300/90">
                  Skip required box photos (e.g. loose or non-carton shipments).
                </p>
              </div>
            </label>
            <p className="text-[11px] text-rose-800/90 dark:text-rose-200/90">
              {!noBoxMode
                ? "Upload at least two photos (e.g. opened carton, then return label). Stored in packages.photo_evidence."
                : "Optional — add any reference shots for this shipment."}
            </p>
            <MasterUploader
              label="Box photos"
              hint="Drag-drop, choose files, or camera — incident-photos bucket."
              value={boxEvidenceUrls}
              onChange={setBoxEvidenceUrls}
              organizationId={organizationId}
              maxFiles={24}
            />
          </div>

          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="flex shrink-0 flex-col gap-3 border-t border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950">
          <div className="flex w-full flex-wrap items-center justify-end gap-2">
            <button type="button" onClick={onClose} className="rounded-full border border-slate-200 px-4 py-2.5 text-sm font-semibold text-foreground hover:bg-accent dark:border-slate-600">
              Cancel
            </button>
            <button
              type="button"
              onClick={handleCreate}
              disabled={
                saving ||
                !pkgNum.trim() ||
                !pkgStoreId.trim() ||
                !isUuidString(pkgStoreId.trim()) ||
                (!noBoxMode && boxEvidenceUrls.length < 2)
              }
              className={BTN_PRIMARY}
            >
              {saving ? <Loader2 className="h-5 w-5 animate-spin" /> : <Plus className="h-5 w-5" />}
              {saving ? "Creating…" : "Create Package"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Create Pallet Modal ───────────────────────────────────────────────────────

export function CreatePalletModal({ onClose, onCreated, actor, aiManifestEnabled = false, organizationId = MVP_ORGANIZATION_ID }: {
  onClose: () => void; onCreated: (p: PalletRecord) => void; actor: string;
  aiManifestEnabled?: boolean;
  organizationId?: string;
}) {
  const [palletNum, setPalletNum] = useState(generatePalletNumber()); const [notes, setNotes] = useState(""); const [file, setFile] = useState<File | null>(null);
  const [bolFile, setBolFile] = useState<File | null>(null);
  const [ocrLoad, setOcrLoad] = useState(false); const [ocrResult, setOcrResult] = useState<{ pallet_number: string; total_items: number; confidence: number } | null>(null);
  const [saving, setSaving] = useState(false); const [error, setError] = useState("");
  const [palletEvidenceUrls, setPalletEvidenceUrls] = useState<string[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);
  const bolRef = useRef<HTMLInputElement>(null);

  // ── Store state (Pallet form — top of hierarchy) ──────────────────────────
  const [palletStoreId,  setPalletStoreId]  = useState("");
  const [palletStoresList, setPalletStoresList] = useState<{ id: string; name: string; platform: string }[]>([]);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const res = await listStores();
      if (cancelled || !res.ok || !res.data) return;
      setPalletStoresList(
        res.data
          .filter((s) => s.is_active !== false)
          .map((s) => ({ id: s.id, name: s.name, platform: s.platform })),
      );
    })();
    return () => { cancelled = true; };
  }, []);

  async function handleOcr(f: File) { setFile(f); setOcrLoad(true); const res = await mockPalletOcr(f); setOcrLoad(false); if (res.ok && res.data) { setOcrResult(res.data); setPalletNum(res.data.pallet_number); } else setError(res.error ?? "OCR failed."); }
  async function handleCreate() {
    if (!palletNum.trim()) return;
    if (!palletStoreId.trim() || !isUuidString(palletStoreId.trim())) {
      setError("Select a valid Store from the dropdown.");
      return;
    }
    setSaving(true); setError("");
    try {
      let manifestUrl: string | undefined;
      let bolUrl: string | undefined;
      if (file) manifestUrl = await uploadToStorage(file, "pallets/manifest", organizationId);
      if (bolFile) bolUrl = await uploadToStorage(bolFile, "pallets/bol", organizationId);
      const peUrls: string[] = [];
      if (manifestUrl) peUrls.push(manifestUrl);
      if (bolUrl) peUrls.push(bolUrl);
      peUrls.push(...palletEvidenceUrls);
      const res = await createPallet({
        organization_id: organizationId,
        pallet_number: palletNum.trim(),
        ...(peUrls.length > 0 ? { photo_evidence: buildEntityPhotoEvidence(peUrls) ?? undefined } : {}),
        store_id: palletStoreId,
        notes,
        created_by: actor,
      });
      setSaving(false);
      if (res.ok && res.data) onCreated(res.data); else setError(res.error ?? "Failed.");
    } catch (e) {
      setSaving(false);
      setError(e instanceof Error ? e.message : "Upload failed.");
    }
  }

  return (
    <div
      className="fixed inset-0 z-[300] flex items-end justify-center bg-black/50 backdrop-blur-sm sm:items-center sm:p-4"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="flex max-h-[92dvh] w-full max-w-lg flex-col overflow-hidden rounded-t-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950 sm:max-h-[88vh] sm:rounded-3xl">
        <div className="shrink-0 border-b border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950 sm:p-6">
          <div className="flex items-center justify-between gap-2">
            <div>
              <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Pallet Flow</p>
              <h2 className="mt-0.5 text-xl font-bold text-foreground">Create Pallet</h2>
            </div>
            <button type="button" onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
          </div>
        </div>
        <div className="min-h-0 flex-1 max-h-[80vh] overflow-y-auto p-4 sm:p-6 space-y-4">
          {aiManifestEnabled && (
            <>
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-300"><Sparkles className="h-3 w-3" />AI Feature</span>
                <span className="text-xs text-slate-400">Manifest OCR</span>
              </div>
              {!file && <button type="button" onClick={() => fileRef.current?.click()} className="flex w-full flex-col items-center gap-3 rounded-2xl border-2 border-dashed border-violet-300 bg-violet-50 p-5 transition hover:border-violet-400 dark:border-violet-700/60 dark:bg-violet-950/30"><FileImage className="h-10 w-10 text-violet-400" /><p className="text-sm font-semibold text-violet-700 dark:text-violet-300">Photo Manifest for AI Scan</p></button>}
              {ocrLoad && <div className="flex items-center gap-3 rounded-2xl border border-sky-200 bg-sky-50 p-4 dark:border-sky-700/60 dark:bg-sky-950/30"><Loader2 className="h-5 w-5 animate-spin text-sky-500" /><p className="text-sm font-semibold text-sky-700 dark:text-sky-300">Scanning…</p></div>}
              {ocrResult && <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-700/60 dark:bg-emerald-950/30"><Sparkles className="h-5 w-5 text-emerald-500" /><p className="text-sm font-bold text-emerald-700 dark:text-emerald-300">AI Scan — {Math.round(ocrResult.confidence*100)}% confidence</p></div>}
              <input ref={fileRef} type="file" className="hidden" accept="image/*" capture="environment" onChange={(e) => { const f = e.target.files?.[0]; if (f) handleOcr(f); e.target.value = ""; }} />
            </>
          )}
          <div>
            <label className={LABEL}>Pallet Number <span className="text-rose-500">*</span></label>
            <div className="flex gap-2">
              <input
                type="text" className={`${INPUT} flex-1`} value={palletNum} onChange={(e) => setPalletNum(e.target.value)}
                autoFocus
                onKeyDown={(e) => { if (e.key === "Enter" && palletNum.trim() && !saving) { e.preventDefault(); handleCreate(); } }}
              />
              <ContextualScanButton
                onDetected={(code) => setPalletNum(code)}
                modalTitle="Scan Pallet Number"
              />
            </div>
          </div>
          <div>
            <label className={LABEL}>Bill of Lading (BoL) <span className="text-xs font-normal text-slate-400">(optional)</span></label>
            {!bolFile ? (
              <button type="button" onClick={() => bolRef.current?.click()} className="flex w-full items-center justify-center gap-2 rounded-2xl border-2 border-dashed border-border py-3 text-sm font-medium text-muted-foreground transition hover:bg-accent">
                <FileText className="h-4 w-4" />Upload BoL scan
              </button>
            ) : (
              <div className="flex items-center justify-between rounded-2xl border border-border bg-muted/40 px-4 py-3 text-sm">
                <span className="truncate font-medium text-foreground">{bolFile.name}</span>
                <button type="button" className="text-xs text-sky-600 underline dark:text-sky-400" onClick={() => setBolFile(null)}>Remove</button>
              </div>
            )}
            <input ref={bolRef} type="file" className="hidden" accept="image/*,application/pdf" capture="environment" onChange={(e) => { const f = e.target.files?.[0]; if (f) setBolFile(f); e.target.value = ""; }} />
          </div>
          <div>
            <label className={LABEL}>Store <span className="text-rose-500">*</span></label>
            <select
              className={INPUT}
              value={palletStoreId}
              onChange={(e) => setPalletStoreId(e.target.value)}
            >
              <option value="">— Select Store —</option>
              {palletStoresList.map((s) => (
                <option key={s.id} value={s.id}>{s.name} ({s.platform})</option>
              ))}
            </select>
            {palletStoresList.length === 0 ? (
              <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">
                No active stores found. Add a store in Settings → Stores.
              </p>
            ) : (
              <p className="mt-1 text-[11px] text-muted-foreground">
                Packages created inside this pallet will inherit this store automatically.
              </p>
            )}
          </div>
          <div><label className={LABEL}>Notes (optional)</label><textarea value={notes} onChange={(e) => setNotes(e.target.value)} rows={2} className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" /></div>
          <MasterUploader
            label="Pallet photos"
            hint="Optional — stored in pallets.photo_evidence (incident-photos bucket)."
            value={palletEvidenceUrls}
            onChange={setPalletEvidenceUrls}
            organizationId={organizationId}
            maxFiles={24}
          />
          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="flex shrink-0 flex-col gap-3 border-t border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-950">
          <div className="flex w-full flex-wrap items-center justify-end gap-2">
            <button type="button" onClick={onClose} className="rounded-full border border-slate-200 px-4 py-2.5 text-sm font-semibold text-foreground hover:bg-accent dark:border-slate-600">
              Cancel
            </button>
            <button
              type="button"
              onClick={handleCreate}
              disabled={saving || !palletNum.trim() || !palletStoreId.trim() || !isUuidString(palletStoreId.trim())}
              className={BTN_PRIMARY}
            >
              {saving ? <Loader2 className="h-5 w-5 animate-spin" /> : <Plus className="h-5 w-5" />}
              {saving ? "Creating…" : "Create Pallet"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Items Data Table ──────────────────────────────────────────────────────────

export function ItemsDataTable({ items, packages, pallets, role, actor, fefoSettings, onRowClick, onRowEdit, onBulkDeleted, onBulkMoved, onNewItem, externalSearch = "", onToast }: {
  items: ReturnRecord[]; packages: PackageRecord[]; pallets: PalletRecord[];
  role: UserRole; actor: string;
  fefoSettings?: { fefo_critical_days: number; fefo_warning_days: number };
  onRowClick: (r: ReturnRecord) => void; onRowEdit: (r: ReturnRecord) => void;
  onBulkDeleted: (ids: string[]) => void;
  onBulkMoved: (updated: ReturnRecord[]) => void;
  onNewItem: () => void;
  /** Merged with local search — set from TopHeader global search on Returns. */
  externalSearch?: string;
  /** Copy-to-clipboard feedback (page-level toast). */
  onToast?: (msg: string, kind?: ToastKind) => void;
}) {
  const fefo_critical = fefoSettings?.fefo_critical_days ?? 30;
  const fefo_warning  = fefoSettings?.fefo_warning_days  ?? 90;
  const [search, setSearch] = useState(""); const [statusF, setStatusF] = useState(""); const [marketF, setMarketF] = useState("");
  const [dateFrom, setDateFrom] = useState(""); const [dateTo, setDateTo] = useState("");
  const [sortField, setSortField] = useState("created_at"); const [sortAsc, setSortAsc] = useState(false);
  const [page, setPage] = useState(1);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [showBulkMove, setShowBulkMove] = useState(false);
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const PER = 25;

  const pkgMap = useMemo(() => new Map(packages.map((p) => [p.id, p])), [packages]);
  const pltMap = useMemo(() => new Map(pallets.map((p) => [p.id, p])), [pallets]);

  function handleSort(f: string) { if (sortField === f) setSortAsc((a) => !a); else { setSortField(f); setSortAsc(false); } setPage(1); }

  const filtered = useMemo(() => {
    let d = [...items];
    const q = (externalSearch.trim() || search).trim().toLowerCase();
    if (q) {
      d = d.filter((r) => {
        const pkg = r.package_id ? pkgMap.get(r.package_id) : null;
        const blob = [
          r.id, r.lpn, r.item_name, r.marketplace,
          formatMarketplaceSource(r.marketplace),
          r.inherited_tracking_number ?? "",
          r.asin ?? "", r.fnsku ?? "", r.sku ?? "",
          pkg?.tracking_number ?? "", pkg?.package_number ?? "",
        ].join(" ").toLowerCase();
        return blob.includes(q);
      });
    }
    if (statusF)  d = d.filter((r) => r.status === statusF);
    if (marketF)  d = d.filter((r) => r.marketplace === marketF);
    if (dateFrom) d = d.filter((r) => r.created_at >= dateFrom);
    if (dateTo)   d = d.filter((r) => r.created_at <= dateTo + "T23:59:59.999Z");
    d.sort((a, b) =>
      compareSortKeys(
        sortKeyItem(a, sortField, pkgMap, pltMap),
        sortKeyItem(b, sortField, pkgMap, pltMap),
        sortAsc,
      ),
    );
    return d;
  }, [items, search, externalSearch, statusF, marketF, dateFrom, dateTo, sortField, sortAsc, pkgMap, pltMap]);

  const hasActiveFilters = !!(externalSearch.trim() || search || statusF || marketF || dateFrom || dateTo);

  const total = Math.max(1, Math.ceil(filtered.length / PER));
  const rows  = filtered.slice((page-1)*PER, page*PER);
  const allSelected = filtered.length > 0 && filtered.every((r) => selectedIds.has(r.id));

  async function handleBulkDelete() {
    if (!window.confirm(`Delete ${selectedIds.size} item(s)? This cannot be undone.`)) return;
    setBulkDeleting(true);
    await Promise.all([...selectedIds].map((id) => deleteReturn(id, actor)));
    onBulkDeleted([...selectedIds]);
    setSelectedIds(new Set()); setBulkDeleting(false);
  }

  const INPUT_SM_DARK = `${INPUT_SM} dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500`;

  return (
    <div className="space-y-3">
      {selectedIds.size > 0 && (
        <BulkActionsBar count={selectedIds.size} onDelete={canDelete(role) ? handleBulkDelete : undefined} onMove={() => setShowBulkMove(true)} onClear={() => setSelectedIds(new Set())} deleting={bulkDeleting} />
      )}
      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[180px] flex-1"><Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" /><input placeholder="Filter: ID, ASIN, tracking, RMA…" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} pl-9`} /></div>
        <select value={statusF} onChange={(e) => { setStatusF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`}><option value="">All Statuses</option>{Object.entries(STATUS_CFG).map(([k,v]) => <option key={k} value={k}>{v.label}</option>)}</select>
        <select value={marketF} onChange={(e) => { setMarketF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`} title="Filter by store"><option value="">All Stores</option>{MARKETPLACES.map((m) => <option key={m} value={m}>{MP_LABELS[m]}</option>)}</select>
        <div className="flex items-center gap-1.5">
          <Calendar className="h-4 w-4 shrink-0 text-slate-400" />
          <input type="date" value={dateFrom} onChange={(e) => { setDateFrom(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} title="From date" />
          <span className="text-xs text-slate-400">–</span>
          <input type="date" value={dateTo} onChange={(e) => { setDateTo(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} title="To date" />
        </div>
        {(search || statusF || marketF || dateFrom || dateTo) && <button onClick={() => { setSearch(""); setStatusF(""); setMarketF(""); setDateFrom(""); setDateTo(""); setPage(1); }} className="flex h-10 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400"><X className="h-3.5 w-3.5" />Clear</button>}
        <button onClick={onNewItem} className="ml-auto flex h-10 items-center gap-2 rounded-xl bg-sky-500 px-4 text-sm font-semibold text-white hover:bg-sky-600"><Plus className="h-4 w-4" />Scan Item</button>
      </div>

      <div className="w-full overflow-x-auto rounded-2xl border border-border">
        <div className="w-full min-w-0">
          <table className="w-full min-w-[1400px] text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                <th className={TH_CHK} onClick={(e) => e.stopPropagation()}>
                  <div className={CHK_FLEX}>
                    <input type="checkbox" checked={allSelected} onChange={(e) => setSelectedIds(e.target.checked ? new Set(filtered.map((r) => r.id)) : new Set())} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                  </div>
                </th>
                <th className="px-4 py-3 text-left"><SortButton field="item_name" label="Identifiers" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="tracking_effective" label="Tracking" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left sm:table-cell"><SortButton field="lpn" label="LPN" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left sm:table-cell"><SortButton field="store_name" label="Store" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left lg:table-cell"><SortButton field="item_conditions" label="Conditions" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left"><SortButton field="status" label="Status" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                {/* ── NEW: Expiry Date column ── */}
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="expiration_date" label="Expiry" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                {/* ── NEW: Evidence Photo column ── */}
                <th className="hidden px-4 py-3 text-left md:table-cell text-xs font-semibold text-slate-500 uppercase tracking-wide">Photo</th>
                <th className="hidden px-4 py-3 text-left lg:table-cell"><SortButton field="hierarchy_key" label="Hierarchy" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left xl:table-cell"><SortButton field="created_by" label="Operator" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left lg:table-cell"><SortButton field="created_at" label="Date" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-3 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {rows.map((r) => {
                const linkedPkg = r.package_id ? pkgMap.get(r.package_id) : null;
                const linkedPlt = r.pallet_id  ? pltMap.get(r.pallet_id)  : null;
                const track = r.inherited_tracking_number ?? linkedPkg?.tracking_number ?? "";
                const expiryStatus = getExpiryStatus(r.expiration_date, fefo_critical, fefo_warning);
                const peUrls = getReturnPhotoEvidenceUrls(r.photo_evidence);
                return (
                  <tr key={r.id} onClick={() => onRowClick(r)} className="group cursor-pointer transition hover:bg-sky-50/50 dark:hover:bg-sky-950/20">
                    <td className={TD_CHK} onClick={(e) => e.stopPropagation()}>
                      <div className={CHK_FLEX}>
                        <input type="checkbox" checked={selectedIds.has(r.id)} onChange={(e) => { const s = new Set(selectedIds); e.target.checked ? s.add(r.id) : s.delete(r.id); setSelectedIds(s); }} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                      </div>
                    </td>
                    <td className="px-4 py-3 min-w-[200px]">
                      <ReturnIdentifiersColumn
                        itemName={r.item_name}
                        asin={r.asin}
                        fnsku={r.fnsku}
                        sku={r.sku}
                        storePlatform={r.stores?.platform}
                        onToast={onToast}
                      />
                    </td>
                    <td className="hidden px-4 py-3 md:table-cell" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-center gap-1 font-mono text-[11px] text-muted-foreground">
                        <span className="min-w-0 truncate">{track || "—"}</span>
                        {track ? <InlineCopy value={track} label="Tracking #" onToast={onToast} stopPropagation /> : null}
                      </div>
                    </td>
                    <td className="hidden px-4 py-3 font-mono text-xs text-muted-foreground sm:table-cell" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-center gap-1">
                        <span>{r.lpn ?? "—"}</span>
                        {r.lpn ? <InlineCopy value={r.lpn} label="LPN" onToast={onToast} stopPropagation /> : null}
                      </div>
                    </td>
                    <td className="hidden px-4 py-3 sm:table-cell">
                      {r.stores ? (
                        <span className="max-w-[140px] truncate text-xs font-medium text-slate-700 dark:text-slate-300" title={r.stores.name}>{r.stores.name}</span>
                      ) : (
                        <span className="text-xs font-medium text-slate-500 dark:text-slate-400">{formatMarketplaceSource(r.marketplace)}</span>
                      )}
                    </td>
                    <td className="hidden px-4 py-3 lg:table-cell"><div className="flex flex-wrap gap-1">{r.conditions.slice(0,2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                    <td className="px-4 py-3"><StatusBadge status={r.status} /></td>
                    {/* ── Expiry Date cell (FEFO) ── */}
                    <td className="hidden px-4 py-3 md:table-cell">
                      {expiryStatus ? (
                        <div className="flex flex-col gap-0.5">
                          <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-bold ${expiryStatus.cls}`}>
                            <span className={`h-1.5 w-1.5 rounded-full ${expiryStatus.dotCls}`} />
                            {expiryStatus.label}
                          </span>
                          <span className="pl-0.5 text-[10px] text-slate-400">{expiryStatus.daysLabel}</span>
                        </div>
                      ) : (
                        <span className="text-xs text-slate-400">—</span>
                      )}
                    </td>
                    {/* ── Evidence Photo cell ── */}
                    <td className="hidden px-4 py-3 md:table-cell" onClick={(e) => e.stopPropagation()}>
                      {peUrls.item_url ? (
                        <PhotoThumb url={peUrls.item_url} alt={`Evidence: ${r.item_name}`} />
                      ) : (
                        <span className="text-xs text-slate-400">—</span>
                      )}
                    </td>
                    <td className="hidden px-4 py-3 lg:table-cell">
                      {linkedPkg
                        ? <span className="inline-flex items-center gap-1 rounded-full bg-sky-100 px-2 py-0.5 font-mono text-[10px] font-bold text-sky-700 dark:bg-sky-900/40 dark:text-sky-300">📦 {linkedPkg.package_number}{linkedPlt ? ` › ${linkedPlt.pallet_number}` : ""}</span>
                        : <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">⚠ Orphaned / Loose</span>}
                    </td>
                    <td className="hidden px-4 py-3 xl:table-cell text-xs text-slate-400">{operatorDisplayLabel(r)}</td>
                    <td className="hidden px-4 py-3 text-xs text-slate-400 lg:table-cell">{fmt(r.created_at)}</td>
                    <td className="px-3 py-3">
                      <RowActionMenu
                        onView={() => onRowClick(r)}
                        onEdit={() => onRowEdit(r)}
                        onDelete={canDelete(role) ? async () => {
                          if (!window.confirm("Delete this return? This cannot be undone.")) return;
                          const res = await deleteReturn(r.id, actor);
                          if (res.ok) onBulkDeleted([r.id]);
                        } : undefined}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {rows.length === 0 && (
          <p className="py-10 text-center text-sm text-slate-400">
            {items.length === 0 && !hasActiveFilters
              ? "No items yet. Scan a return to get started."
              : "No records match your filters."}
          </p>
        )}
      </div>

      {total > 1 && <div className="flex items-center justify-between text-sm text-slate-500"><p>Page {page} of {total} · {filtered.length} items</p><div className="flex gap-2"><button disabled={page<=1} onClick={() => setPage((p)=>p-1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-sm font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:bg-slate-800">← Prev</button><button disabled={page>=total} onClick={() => setPage((p)=>p+1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-sm font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:bg-slate-800">Next →</button></div></div>}

      {showBulkMove && (
        <BulkMoveModal selectedIds={[...selectedIds]} packages={packages} pallets={pallets} actor={actor} onClose={() => setShowBulkMove(false)}
          onMoved={(updated, failed) => { onBulkMoved(updated); setSelectedIds(new Set()); setShowBulkMove(false); if (failed > 0) window.alert(`${failed} item(s) failed to move.`); }}
        />
      )}
    </div>
  );
}

// ─── Packages Data Table ───────────────────────────────────────────────────────

export function PackagesDataTable({ packages, returns: allReturns = [], pallets = [], role, actor, onRowClick, onRowEdit, onBulkDeleted, onBulkPackagesUpdated, onNewPackage, externalSearch = "", onToast }: {
  packages: PackageRecord[]; returns?: ReturnRecord[]; pallets?: PalletRecord[];
  role: UserRole; actor: string;
  onRowClick: (p: PackageRecord) => void; onRowEdit: (p: PackageRecord) => void;
  onBulkDeleted: (ids: string[]) => void;
  /** Called after bulk assign to pallet so parent state stays in sync with DB */
  onBulkPackagesUpdated?: (updated: PackageRecord[]) => void;
  onNewPackage: () => void;
  externalSearch?: string;
  onToast?: (msg: string, kind?: ToastKind) => void;
}) {
  const [search, setSearch] = useState(""); const [statusF, setStatusF] = useState(""); const [carrierF, setCarrierF] = useState("");
  const [dateFrom, setDateFrom] = useState(""); const [dateTo, setDateTo] = useState("");
  const [sortField, setSortField] = useState("created_at"); const [sortAsc, setSortAsc] = useState(false);
  const [page, setPage] = useState(1);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [showBulkPallet, setShowBulkPallet] = useState(false);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const PER = 25;

  function toggleExpand(id: string, e: React.MouseEvent) { e.stopPropagation(); setExpandedIds((s) => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n; }); }

  function handleSort(f: string) { if (sortField === f) setSortAsc((a) => !a); else { setSortField(f); setSortAsc(false); } setPage(1); }

  const filtered = useMemo(() => {
    let d = [...packages];
    const q = (externalSearch.trim() || search).trim().toLowerCase();
    if (q) {
      d = d.filter((p) => [p.id, p.package_number, p.tracking_number ?? "", p.carrier_name ?? ""].join(" ").toLowerCase().includes(q));
    }
    if (statusF)  d = d.filter((p) => p.status === statusF);
    if (carrierF) d = d.filter((p) => p.carrier_name === carrierF);
    if (dateFrom) d = d.filter((p) => p.created_at >= dateFrom);
    if (dateTo)   d = d.filter((p) => p.created_at <= dateTo + "T23:59:59.999Z");
    d.sort((a, b) =>
      compareSortKeys(sortKeyPackage(a, sortField), sortKeyPackage(b, sortField), sortAsc),
    );
    return d;
  }, [packages, search, externalSearch, statusF, carrierF, dateFrom, dateTo, sortField, sortAsc]);

  const hasActiveFilters = !!(externalSearch.trim() || search || statusF || carrierF || dateFrom || dateTo);

  const total = Math.max(1, Math.ceil(filtered.length / PER));
  const rows  = filtered.slice((page-1)*PER, page*PER);
  const allSelected = filtered.length > 0 && filtered.every((p) => selectedIds.has(p.id));
  const usedCarriers = useMemo(() => [...new Set(packages.map((p) => p.carrier_name).filter(Boolean))], [packages]);
  const INPUT_SM_DARK = `${INPUT_SM} dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500`;

  async function handleBulkDelete() {
    if (!window.confirm(`Delete ${selectedIds.size} package(s)?`)) return;
    setBulkDeleting(true);
    await Promise.all([...selectedIds].map((id) => deletePackage(id, actor)));
    onBulkDeleted([...selectedIds]); setSelectedIds(new Set()); setBulkDeleting(false);
  }

  return (
    <div className="space-y-3">
      {selectedIds.size > 0 && (
        <BulkActionsBar
          count={selectedIds.size}
          onAssignPallet={pallets.length > 0 ? () => setShowBulkPallet(true) : undefined}
          onDelete={canDelete(role) ? handleBulkDelete : undefined}
          onClear={() => setSelectedIds(new Set())}
          deleting={bulkDeleting}
        />
      )}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[180px] flex-1"><Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" /><input placeholder="Search package # or tracking…" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} pl-9`} /></div>
        <select value={statusF} onChange={(e) => { setStatusF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`}><option value="">All Statuses</option>{Object.entries(PKG_STATUS_CFG).map(([k,v]) => <option key={k} value={k}>{v.label}</option>)}</select>
        <select value={carrierF} onChange={(e) => { setCarrierF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`}><option value="">All Carriers</option>{usedCarriers.map((c) => <option key={c!} value={c!}>{c}</option>)}</select>
        <div className="flex items-center gap-1.5"><Calendar className="h-4 w-4 shrink-0 text-slate-400" /><input type="date" value={dateFrom} onChange={(e) => { setDateFrom(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} /><span className="text-xs text-slate-400">–</span><input type="date" value={dateTo} onChange={(e) => { setDateTo(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} /></div>
        <button onClick={onNewPackage} className="ml-auto flex h-10 items-center gap-2 rounded-xl bg-violet-500 px-4 text-sm font-semibold text-white hover:bg-violet-600"><Plus className="h-4 w-4" />New Package</button>
      </div>
      <div className="w-full overflow-hidden rounded-2xl border border-border">
        <div className="w-full min-w-0 overflow-x-auto">
          <table className="w-full min-w-[720px] text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                <th className={TH_EXP} aria-hidden />
                <th className={TH_CHK} onClick={(e) => e.stopPropagation()}>
                  <div className={CHK_FLEX}>
                    <input type="checkbox" checked={allSelected} onChange={(e) => setSelectedIds(e.target.checked ? new Set(filtered.map((p) => p.id)) : new Set())} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                  </div>
                </th>
                <th className="px-4 py-3 text-left"><SortButton field="package_number" label="Package #" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="store_name" label="Store" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left sm:table-cell"><SortButton field="carrier_tracking" label="Carrier / Tracking" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left"><SortButton field="pkg_items_sort" label="Items" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left"><SortButton field="status" label="Status" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="created_by" label="Operator" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="created_at" label="Date" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-3 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {rows.map((p) => {
                const pct = p.expected_item_count > 0 ? Math.min(100, (p.actual_item_count / p.expected_item_count) * 100) : null;
                const isExpanded = expandedIds.has(p.id);
                const pkgItems = allReturns.filter((r) => r.package_id === p.id);
                return (
                  <React.Fragment key={p.id}>
                    <tr onClick={() => onRowClick(p)} className="group cursor-pointer transition hover:bg-violet-50/50 dark:hover:bg-violet-950/20">
                      <td className={TD_EXP} onClick={(e) => toggleExpand(p.id, e)}>
                        <button type="button" className="flex h-6 w-6 items-center justify-center rounded-lg text-slate-400 hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800 dark:hover:text-slate-300" title={isExpanded ? "Collapse items" : `Show ${pkgItems.length} item(s)`}>
                          {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                        </button>
                      </td>
                      <td className={TD_CHK} onClick={(e) => e.stopPropagation()}>
                        <div className={CHK_FLEX}>
                          <input type="checkbox" checked={selectedIds.has(p.id)} onChange={(e) => { const s = new Set(selectedIds); e.target.checked ? s.add(p.id) : s.delete(p.id); setSelectedIds(s); }} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                        </div>
                      </td>
                      <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-center gap-1.5 font-mono text-xs font-bold text-foreground">
                          <span>{p.package_number}</span>
                          <InlineCopy value={p.package_number} label="Package #" onToast={onToast} stopPropagation />
                        </div>
                      </td>
                      <td className="hidden px-4 py-3 md:table-cell">
                        {p.stores ? (
                          <span className="max-w-[140px] truncate text-xs font-medium text-slate-700 dark:text-slate-300" title={p.stores.name}>{p.stores.name}</span>
                        ) : (
                          <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-medium text-slate-400 dark:border-slate-700 dark:bg-slate-900">Mixed / Unassigned</span>
                        )}
                      </td>
                      <td className="hidden px-4 py-3 sm:table-cell" onClick={(e) => e.stopPropagation()}>
                        <div className="flex flex-col gap-0.5">
                          {p.carrier_name && (
                            <span className="flex items-center gap-1 text-xs text-slate-600 dark:text-slate-300">
                              <Truck className="h-3 w-3" />
                              {p.carrier_name}
                            </span>
                          )}
                          {p.tracking_number && (
                            <span className="flex items-center gap-1 font-mono text-[10px] text-slate-400">
                              <span className="min-w-0 truncate">{p.tracking_number}</span>
                              <InlineCopy value={p.tracking_number} label="Tracking #" onToast={onToast} stopPropagation />
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-3"><div className="flex items-center gap-2"><span className="text-sm font-bold text-slate-700 dark:text-slate-300">{p.actual_item_count}/{p.expected_item_count > 0 ? p.expected_item_count : "?"}</span>{pct !== null && <div className="hidden h-1.5 w-12 overflow-hidden rounded-full bg-muted sm:block"><div className={`h-full rounded-full ${pct >= 100 ? "bg-emerald-500" : "bg-sky-500"}`} style={{ width: `${pct}%` }} /></div>}</div></td>
                      <td className="px-4 py-3"><PkgStatusBadge status={p.status} /></td>
                      <td className="hidden px-4 py-3 text-xs capitalize text-slate-400 md:table-cell">{operatorDisplayLabel(p)}</td>
                      <td className="hidden px-4 py-3 text-xs text-slate-400 md:table-cell">{fmt(p.created_at)}</td>
                      <td className="px-3 py-3">
                        <RowActionMenu
                          onView={() => onRowClick(p)} onEdit={() => onRowEdit(p)}
                          onDelete={canDelete(role) ? async () => { if (!window.confirm("Delete this package?")) return; const r = await deletePackage(p.id, actor); if (r.ok) onBulkDeleted([p.id]); } : undefined}
                        />
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr className="bg-violet-50/40 dark:bg-violet-950/10">
                        <td colSpan={10} className="px-6 py-3">
                          {pkgItems.length === 0
                            ? <p className="py-2 text-center text-xs text-slate-400">No items scanned for this package yet.</p>
                            : (
                              <div className="overflow-hidden rounded-xl border border-violet-200 dark:border-violet-800/50">
                                <table className="w-full text-xs">
                                  <thead><tr className="border-b border-violet-200 bg-violet-100/60 dark:border-violet-800/50 dark:bg-violet-950/40">
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Item</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Store</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Condition</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Status</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Operator</th>
                                  </tr></thead>
                                  <tbody className="divide-y divide-violet-100 dark:divide-violet-900/40">
                                    {pkgItems.map((r) => (
                                      <tr key={r.id} className="hover:bg-violet-50 dark:hover:bg-violet-950/20">
                                        <td className="px-3 py-2">
                                          <ReturnIdentifiersColumn
                                            compact
                                            itemName={r.item_name}
                                            asin={r.asin}
                                            fnsku={r.fnsku}
                                            sku={r.sku}
                                            storePlatform={r.stores?.platform}
                                            onToast={onToast}
                                          />
                                        </td>
                                        <td className="px-3 py-2">
                                          {r.stores ? (
                                            <span className="max-w-[100px] truncate text-[11px] font-medium text-slate-600 dark:text-slate-300" title={r.stores.name}>{r.stores.name}</span>
                                          ) : (
                                            <span className="text-[11px] text-slate-500">{formatMarketplaceSource(r.marketplace)}</span>
                                          )}
                                        </td>
                                        <td className="px-3 py-2"><div className="flex flex-wrap gap-1">{r.conditions.slice(0,2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                                        <td className="px-3 py-2"><StatusBadge status={r.status} /></td>
                                        <td className="px-3 py-2 capitalize text-slate-400">{operatorDisplayLabel(r)}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
        {rows.length === 0 && (
          <p className="py-10 text-center text-sm text-slate-400">
            {packages.length === 0 && !hasActiveFilters
              ? "No packages yet. Create a package to receive returns."
              : "No packages match your filters."}
          </p>
        )}
      </div>
      {total > 1 && <div className="flex items-center justify-between text-sm text-slate-500"><p>Page {page} of {total}</p><div className="flex gap-2"><button disabled={page<=1} onClick={() => setPage((p)=>p-1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">← Prev</button><button disabled={page>=total} onClick={() => setPage((p)=>p+1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">Next →</button></div></div>}

      {showBulkPallet && (
        <BulkAssignPackagesModal
          selectedIds={[...selectedIds]}
          pallets={pallets}
          actor={actor}
          onClose={() => setShowBulkPallet(false)}
          onDone={(updated, failed) => {
            if (onBulkPackagesUpdated && updated.length) onBulkPackagesUpdated(updated);
            setSelectedIds(new Set());
            setShowBulkPallet(false);
            if (failed > 0) window.alert(`${failed} package(s) failed to update.`);
          }}
        />
      )}
    </div>
  );
}

// ─── Pallets Data Table ────────────────────────────────────────────────────────

export function PalletsDataTable({ pallets, packages: allPackages = [], returns: allReturns = [], role, actor, onRowClick, onRowEdit, onBulkDeleted, onNewPallet, externalSearch = "", onToast }: {
  pallets: PalletRecord[]; packages?: PackageRecord[]; returns?: ReturnRecord[]; role: UserRole; actor: string;
  onRowClick: (p: PalletRecord) => void; onRowEdit: (p: PalletRecord) => void;
  onBulkDeleted: (ids: string[]) => void; onNewPallet: () => void;
  externalSearch?: string;
  onToast?: (msg: string, kind?: ToastKind) => void;
}) {
  const [search, setSearch] = useState(""); const [statusF, setStatusF] = useState("");
  const [dateFrom, setDateFrom] = useState(""); const [dateTo, setDateTo] = useState("");
  const [sortField, setSortField] = useState("created_at"); const [sortAsc, setSortAsc] = useState(false);
  const [page, setPage] = useState(1);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  /** Package rows expanded inside the pallet sub-table (second-level: items). */
  const [nestedPkgExpandedIds, setNestedPkgExpandedIds] = useState<Set<string>>(new Set());
  const PER = 25;

  function toggleExpand(id: string, e: React.MouseEvent) { e.stopPropagation(); setExpandedIds((s) => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n; }); }

  function toggleNestedPkgExpand(pkgId: string, e: React.MouseEvent) {
    e.stopPropagation();
    setNestedPkgExpandedIds((s) => {
      const n = new Set(s);
      if (n.has(pkgId)) n.delete(pkgId);
      else n.add(pkgId);
      return n;
    });
  }

  function handleSort(f: string) { if (sortField === f) setSortAsc((a) => !a); else { setSortField(f); setSortAsc(false); } setPage(1); }

  const filtered = useMemo(() => {
    let d: PalletSortRow[] = pallets.map((p) => {
      const pkgsOnPallet = allPackages.filter((pk) => pk.pallet_id === p.id);
      const rollupItems = allReturns.filter((r) => r.pallet_id === p.id).length;
      return {
        ...p,
        _rollupPkgs: p.child_packages_count ?? pkgsOnPallet.length,
        _rollupItems: p.child_returns_count ?? rollupItems,
      };
    });
    const q = (externalSearch.trim() || search).trim().toLowerCase();
    if (q) d = d.filter((p) => `${p.id} ${p.pallet_number}`.toLowerCase().includes(q));
    if (statusF)  d = d.filter((p) => p.status === statusF);
    if (dateFrom) d = d.filter((p) => p.created_at >= dateFrom);
    if (dateTo)   d = d.filter((p) => p.created_at <= dateTo + "T23:59:59.999Z");
    d.sort((a, b) =>
      compareSortKeys(sortKeyPallet(a, sortField), sortKeyPallet(b, sortField), sortAsc),
    );
    return d;
  }, [pallets, allPackages, allReturns, search, externalSearch, statusF, dateFrom, dateTo, sortField, sortAsc]);

  const hasActiveFilters = !!(externalSearch.trim() || search || statusF || dateFrom || dateTo);

  const total = Math.max(1, Math.ceil(filtered.length / PER));
  const rows  = filtered.slice((page-1)*PER, page*PER);
  const allSelected = filtered.length > 0 && filtered.every((p) => selectedIds.has(p.id));
  const INPUT_SM_DARK = `${INPUT_SM} dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500`;

  async function handleBulkDelete() {
    if (!window.confirm(`Delete ${selectedIds.size} pallet(s)?`)) return;
    setBulkDeleting(true);
    await Promise.all([...selectedIds].map((id) => deletePallet(id, actor)));
    onBulkDeleted([...selectedIds]); setSelectedIds(new Set()); setBulkDeleting(false);
  }

  return (
    <div className="space-y-3">
      {selectedIds.size > 0 && <BulkActionsBar count={selectedIds.size} onDelete={canDelete(role) ? handleBulkDelete : undefined} onClear={() => setSelectedIds(new Set())} deleting={bulkDeleting} />}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[180px] flex-1"><Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" /><input placeholder="Search pallet #…" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} pl-9`} /></div>
        <select value={statusF} onChange={(e) => { setStatusF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`}><option value="">All Statuses</option>{Object.entries(PALLET_STATUS_CFG).map(([k,v]) => <option key={k} value={k}>{v.label}</option>)}</select>
        <div className="flex items-center gap-1.5"><Calendar className="h-4 w-4 shrink-0 text-slate-400" /><input type="date" value={dateFrom} onChange={(e) => { setDateFrom(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} /><span className="text-xs text-slate-400">–</span><input type="date" value={dateTo} onChange={(e) => { setDateTo(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} /></div>
        <button onClick={onNewPallet} className="ml-auto flex h-10 items-center gap-2 rounded-xl bg-slate-700 px-4 text-sm font-semibold text-white hover:bg-slate-800 dark:bg-slate-600 dark:hover:bg-slate-500"><Plus className="h-4 w-4" />New Pallet</button>
      </div>
      <div className="w-full overflow-hidden rounded-2xl border border-border">
        <div className="w-full min-w-0 overflow-x-auto">
          <table className="w-full min-w-[640px] text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                <th className={TH_EXP} aria-hidden />
                <th className={TH_CHK} onClick={(e) => e.stopPropagation()}>
                  <div className={CHK_FLEX}>
                    <input type="checkbox" checked={allSelected} onChange={(e) => setSelectedIds(e.target.checked ? new Set(filtered.map((p) => p.id)) : new Set())} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                  </div>
                </th>
                <th className="px-4 py-3 text-left"><SortButton field="pallet_number" label="Pallet #" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="store_name" label="Store" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left">
                  <div className="flex flex-wrap items-center gap-x-1.5 gap-y-1">
                    <SortButton field="rollup_pkgs" label="Pkgs" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} />
                    <span className="text-[10px] font-bold text-slate-300 dark:text-slate-600">/</span>
                    <SortButton field="rollup_items" label="Items" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} />
                  </div>
                </th>
                <th className="px-4 py-3 text-left"><SortButton field="status" label="Status" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="created_by" label="Operator" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left lg:table-cell"><SortButton field="created_at" label="Date" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-3 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {rows.map((p) => {
                const isExpanded = expandedIds.has(p.id);
                const pltPackages = allPackages.filter((pk) => pk.pallet_id === p.id);
                return (
                  <React.Fragment key={p.id}>
                    <tr onClick={() => onRowClick(p)} className="group cursor-pointer transition hover:bg-accent hover:text-accent-foreground/50">
                      <td className={TD_EXP} onClick={(e) => toggleExpand(p.id, e)}>
                        <button type="button" className="flex h-6 w-6 items-center justify-center rounded-lg text-slate-400 hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800 dark:hover:text-slate-300" title={isExpanded ? "Collapse packages" : `Show ${pltPackages.length} package(s)`}>
                          {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                        </button>
                      </td>
                      <td className={TD_CHK} onClick={(e) => e.stopPropagation()}>
                        <div className={CHK_FLEX}>
                          <input type="checkbox" checked={selectedIds.has(p.id)} onChange={(e) => { const s = new Set(selectedIds); e.target.checked ? s.add(p.id) : s.delete(p.id); setSelectedIds(s); }} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                        </div>
                      </td>
                      <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-center gap-1.5 font-mono text-xs font-bold text-foreground">
                          <span>{p.pallet_number}</span>
                          <InlineCopy value={p.pallet_number} label="Pallet #" onToast={onToast} stopPropagation />
                        </div>
                      </td>
                      <td className="hidden px-4 py-3 md:table-cell">
                        {p.stores ? (
                          <span className="max-w-[140px] truncate text-xs font-medium text-slate-700 dark:text-slate-300" title={p.stores.name}>{p.stores.name}</span>
                        ) : (
                          <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-medium text-slate-400 dark:border-slate-700 dark:bg-slate-900">Mixed / Unassigned</span>
                        )}
                      </td>
                      <td className="px-4 py-3"><span className="font-bold text-slate-700 dark:text-slate-300">{p._rollupPkgs}</span><span className="mx-1 text-slate-300 dark:text-slate-600">pkgs</span><span className="font-bold text-slate-500">{p._rollupItems}</span><span className="ml-1 text-slate-300 dark:text-slate-600">items</span></td>
                      <td className="px-4 py-3"><PalletStatusBadge status={p.status} /></td>
                      <td className="hidden px-4 py-3 text-xs capitalize text-slate-400 md:table-cell">{operatorDisplayLabel(p)}</td>
                      <td className="hidden px-4 py-3 text-xs text-slate-400 lg:table-cell">{fmt(p.created_at)}</td>
                      <td className="px-3 py-3">
                        <RowActionMenu
                          onView={() => onRowClick(p)} onEdit={() => onRowEdit(p)}
                          onDelete={canDelete(role) ? async () => { if (!window.confirm("Delete this pallet?")) return; const r = await deletePallet(p.id, actor); if (r.ok) onBulkDeleted([p.id]); } : undefined}
                        />
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr className="bg-slate-50/70 dark:bg-slate-900/50">
                        <td colSpan={9} className="px-6 py-3">
                          {pltPackages.length === 0
                            ? <p className="py-2 text-center text-xs text-slate-400">No packages linked to this pallet yet.</p>
                            : (
                              <div className="overflow-hidden rounded-xl border border-slate-200 dark:border-slate-700">
                                <table className="w-full text-xs">
                                  <thead><tr className="border-b border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800">
                                    <th className={TH_EXP} aria-hidden />
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Package #</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Carrier</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Tracking</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Items</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Status</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Operator</th>
                                  </tr></thead>
                                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                    {pltPackages.map((pk) => {
                                      const pkItemCount = allReturns.filter((r) => r.package_id === pk.id).length;
                                      const pkgItems = allReturns.filter((r) => r.package_id === pk.id);
                                      const nestedOpen = nestedPkgExpandedIds.has(pk.id);
                                      const pct = pk.expected_item_count > 0 ? Math.min(100, (pk.actual_item_count / pk.expected_item_count) * 100) : null;
                                      return (
                                        <React.Fragment key={pk.id}>
                                          <tr className="group hover:bg-slate-50 dark:hover:bg-slate-800/50">
                                            <td className={TD_EXP} onClick={(e) => toggleNestedPkgExpand(pk.id, e)}>
                                              <button type="button" className="flex h-6 w-6 items-center justify-center rounded-lg text-slate-400 hover:bg-slate-200 hover:text-slate-600 dark:hover:bg-slate-700 dark:hover:text-slate-300" title={nestedOpen ? "Collapse items" : `Show ${pkgItems.length} item(s)`}>
                                                {nestedOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                                              </button>
                                            </td>
                                            <td className="px-3 py-2" onClick={(e) => e.stopPropagation()}>
                                              <div className="flex items-center gap-1 font-mono font-semibold text-slate-700 dark:text-slate-300">
                                                <span>{pk.package_number}</span>
                                                <InlineCopy value={pk.package_number} label="Package #" onToast={onToast} stopPropagation />
                                              </div>
                                            </td>
                                            <td className="px-3 py-2 text-slate-500">{pk.carrier_name ?? "—"}</td>
                                            <td className="px-3 py-2" onClick={(e) => e.stopPropagation()}>
                                              <div className="flex items-center gap-1 font-mono text-slate-400">
                                                <span className="min-w-0 truncate">{pk.tracking_number ?? "—"}</span>
                                                {pk.tracking_number ? (
                                                  <InlineCopy value={pk.tracking_number} label="Tracking #" onToast={onToast} stopPropagation />
                                                ) : null}
                                              </div>
                                            </td>
                                            <td className="px-3 py-2">
                                              <div className="flex items-center gap-2">
                                                <span className="font-bold text-slate-600 dark:text-slate-300">{pkItemCount}/{pk.expected_item_count > 0 ? pk.expected_item_count : "?"}</span>
                                                {pct !== null && <div className="hidden h-1.5 w-10 overflow-hidden rounded-full bg-muted sm:block"><div className={`h-full rounded-full ${pct >= 100 ? "bg-emerald-500" : "bg-sky-500"}`} style={{ width: `${pct}%` }} /></div>}
                                              </div>
                                            </td>
                                            <td className="px-3 py-2"><PkgStatusBadge status={pk.status} /></td>
                                            <td className="px-3 py-2 capitalize text-slate-400">{operatorDisplayLabel(pk)}</td>
                                          </tr>
                                          {nestedOpen && (
                                            <tr className="bg-slate-100/60 dark:bg-slate-900/40">
                                              <td colSpan={7} className="px-4 py-2">
                                                {pkgItems.length === 0
                                                  ? <p className="py-2 text-center text-[11px] text-slate-400">No items scanned for this package yet.</p>
                                                  : (
                                                    <div className="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-600">
                                                      <table className="w-full text-[11px]">
                                                        <thead><tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-950/40">
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Item</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Store</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Condition</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Status</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Operator</th>
                                                        </tr></thead>
                                                        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                                          {pkgItems.map((r) => (
                                                            <tr key={r.id} className="hover:bg-slate-50 dark:hover:bg-slate-950/30">
                                                              <td className="px-2 py-1.5">
                                                                <ReturnIdentifiersColumn
                                                                  compact
                                                                  itemName={r.item_name}
                                                                  asin={r.asin}
                                                                  fnsku={r.fnsku}
                                                                  sku={r.sku}
                                                                  storePlatform={r.stores?.platform}
                                                                  onToast={onToast}
                                                                />
                                                              </td>
                                                              <td className="px-2 py-1.5">
                                                                {r.stores ? (
                                                                  <span className="max-w-[90px] truncate text-[10px] font-medium text-slate-600 dark:text-slate-300" title={r.stores.name}>{r.stores.name}</span>
                                                                ) : (
                                                                  <span className="text-[10px] text-slate-500">{formatMarketplaceSource(r.marketplace)}</span>
                                                                )}
                                                              </td>
                                                              <td className="px-2 py-1.5"><div className="flex flex-wrap gap-1">{r.conditions.slice(0, 2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                                                              <td className="px-2 py-1.5"><StatusBadge status={r.status} /></td>
                                                              <td className="px-2 py-1.5 capitalize text-slate-400">{operatorDisplayLabel(r)}</td>
                                                            </tr>
                                                          ))}
                                                        </tbody>
                                                      </table>
                                                    </div>
                                                  )}
                                              </td>
                                            </tr>
                                          )}
                                        </React.Fragment>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            )}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
        {rows.length === 0 && (
          <p className="py-10 text-center text-sm text-slate-400">
            {pallets.length === 0 && !hasActiveFilters
              ? "No pallets yet. Create a pallet to start a batch."
              : "No pallets match your filters."}
          </p>
        )}
      </div>
      {total > 1 && <div className="flex items-center justify-between text-sm text-slate-500"><p>Page {page} of {total}</p><div className="flex gap-2"><button disabled={page<=1} onClick={() => setPage((p)=>p-1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">← Prev</button><button disabled={page>=total} onClick={() => setPage((p)=>p+1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">Next →</button></div></div>}
    </div>
  );
}
