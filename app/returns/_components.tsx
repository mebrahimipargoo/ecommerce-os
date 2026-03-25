"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePhysicalScanner } from "../../hooks/usePhysicalScanner";
import { createPortal } from "react-dom";
import Link from "next/link";
import {
  AlertTriangle, ArrowLeft, ArrowRight, Barcode, Boxes, Calendar, CalendarX2,
  Camera, CheckCircle2, CheckSquare, ChevronDown, ChevronRight, ChevronUp, CircleDot, ClipboardCheck,
  Clock, Eye, FileImage, FileText, Loader2, Minus, MoreHorizontal, Package2,
  PackageCheck, PackageX, Pencil, Plus, QrCode, Save, ScanLine, Search,
  ShieldAlert, ShieldCheck, Sparkles, Tag, Trash2, Truck, User, X, XCircle, ZoomIn,
} from "lucide-react";
import type { OrgSettings } from "./actions";
import { SmartCameraUpload } from "../../components/ui/SmartCameraUpload";
import { BarcodeScannerModal } from "../../components/ui/BarcodeScannerModal";
import {
  type ReturnRecord, type ReturnUpdatePayload,
  type PalletRecord, type PalletStatus,
  type PackageRecord, type PackageStatus,
  insertReturn, updateReturn, deleteReturn,
  createPallet, updatePalletStatus, deletePallet,
  createPackage, updatePackage, closePackage, deletePackage,
  listReturnsByPackage,
} from "./actions";
import { itemMatchesPackageExpectation } from "../../lib/package-expectations";
import { getBarcodeModeFromStorage, getDefaultStoreIdFromStorage } from "../../lib/openai-settings";
import { parseBarcodeSource } from "../../lib/utils/barcode-parser";
import { supabase as supabaseBrowser } from "../../src/lib/supabase";
import { uploadToStorage } from "../../lib/supabase/storage";
import { fetchProductFromAmazon } from "../../lib/api/amazon-mock";

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

export type UserRole = "admin" | "operator";
export interface MockUser { name: string; role: UserRole }
export const DEFAULT_USER: MockUser = { name: "Warehouse Op", role: "operator" };
export const canEdit   = (r: UserRole) => r === "admin";
export const canDelete = (r: UserRole) => r === "admin";

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
  ctx?: { hasPackageLink?: boolean; orphanLpn?: boolean },
): PhotoCategoryDef[] {
  if (!conditions.length || conditions.includes("sellable")) return [];
  const ids = new Set<string>(["outer_box", "fnsku_label"]);
  for (const c of conditions) {
    if (c === "empty_box" || c === "missing_item") ids.add("empty_interior");
    if (c === "damaged_box" || c.startsWith("damaged_") || c === "scratched") ids.add("damage_closeup");
    if (c.startsWith("wrong_item_")) ids.add("incorrect_item");
    if (c === "expired") ids.add("expiry_label");
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

/** Whether a return row matches an expected slip line (by barcode or product name). */
function physicalItemMatchesExpectedLine(it: ReturnRecord, exp: SlipExpectedItem): boolean {
  const name = it.item_name.toLowerCase();
  const pid  = (it.product_identifier ?? "").toLowerCase();
  const bc   = exp.barcode.trim().toLowerCase();
  const nm   = exp.name.trim().toLowerCase();
  const nameChunk = nm.split(/\s+/).slice(0, 3).join(" ");
  return (
    (bc.length >= 2 && (pid.includes(bc) || name.includes(bc))) ||
    (nameChunk.length >= 3 && name.includes(nameChunk))
  );
}

type LabelOcrResult = { lpn: string; product_identifier?: string; marketplace: string; confidence: number };

export async function mockLabelOcr(_f: File): Promise<{ ok: boolean; data?: LabelOcrResult; error?: string }> {
  await new Promise((r) => setTimeout(r, 1900));
  if (Math.random() < 0.08) return { ok: false, error: "Label unclear — please enter manually." };
  return {
    ok: true,
    data: {
      lpn:        `LPN${Math.random().toString(36).toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 9)}`,
      product_identifier: `B0${String(Math.floor(Math.random() * 1e7)).padStart(7, "0")}`,
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
  /** Multi-select condition keys — see {@link CONDITION_CHIP_DEFS}. */
  condition_keys: string[];
  expiration_date: string; batch_number: string;
  /** Items link to Packages only. Pallet is inherited from the Package. */
  package_link_id: string;
  notes: string; photos: Record<string, File[]>;
  /** Claim evidence photo URLs — uploaded to Supabase Storage during Step 2. */
  photo_item_url: string;
  photo_expiry_url: string;
  /** Connected store UUID — links this item to a specific marketplace store account. */
  store_id: string;
};

export const EMPTY_WIZARD: WizardState = {
  lpn: "", product_identifier: "", marketplace: "", item_name: "",
  condition_keys: [],
  expiration_date: "", batch_number: "", package_link_id: "",
  notes: "", photos: {},
  photo_item_url: "", photo_expiry_url: "",
  store_id: "",
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
  return (
    <button onClick={onToggle} title="Toggle role (demo)"
      className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-bold transition ${user.role === "admin" ? "border-violet-300 bg-violet-50 text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/50 dark:text-violet-300" : "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300"}`}>
      {user.role === "admin" ? <ShieldCheck className="h-3 w-3" /> : <User className="h-3 w-3" />}
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
    case "product_identifier": return (r.product_identifier ?? "").toLowerCase();
    case "inherited_tracking_number":
    case "tracking_effective": return trackEff.toLowerCase();
    case "lpn": return (r.lpn ?? "").toLowerCase();
    case "marketplace": return r.marketplace.toLowerCase();
    case "item_name": return r.item_name.toLowerCase();
    case "item_conditions": return [...r.conditions].sort().join(",");
    case "status": return r.status.toLowerCase();
    case "hierarchy_key": {
      if (!linkedPkg) return "\uffff";
      const pltPart = linkedPlt?.pallet_number ?? "";
      return `${linkedPkg.package_number}\0${pltPart}`.toLowerCase();
    }
    case "created_by": return (r.created_by ?? "").toLowerCase();
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
    case "created_by": return (p.created_by ?? "").toLowerCase();
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
    case "created_by": return (p.created_by ?? "").toLowerCase();
    case "created_at": return new Date(p.created_at).getTime();
    default: return String((p as unknown as Record<string, unknown>)[field] ?? "").toLowerCase();
  }
}

// ─── ComboboxField ─────────────────────────────────────────────────────────────

interface ComboboxOption { id: string; label: string; sublabel?: string }
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
  const filtered = useMemo(() => { const q = search.toLowerCase(); return q ? options.filter((o) => o.label.toLowerCase().includes(q) || (o.sublabel ?? "").toLowerCase().includes(q)) : options; }, [options, search]);

  useEffect(() => {
    if (!search) return;
    const exact = options.find((o) => o.label.toLowerCase() === search.toLowerCase() || o.sublabel?.toLowerCase() === search.toLowerCase());
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

export function RowActionMenu({ onView, onEdit, onDelete }: {
  onView?: () => void; onEdit?: () => void; onDelete?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  return (
    <div ref={ref} className="relative" onClick={(e) => e.stopPropagation()}>
      <button onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        className="flex h-8 w-8 items-center justify-center rounded-lg text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800 dark:hover:text-slate-300">
        <MoreHorizontal className="h-4 w-4" />
      </button>
      {open && (
        <div className="absolute right-0 top-full z-30 mt-1 w-44 overflow-hidden rounded-2xl border border-slate-200 bg-white py-1 shadow-xl dark:border-slate-700 dark:bg-slate-900">
          {onView && <button onClick={() => { onView(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"><Eye className="h-4 w-4 text-slate-400" />View Detail</button>}
          {onEdit && <button onClick={() => { onEdit(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"><Pencil className="h-4 w-4 text-slate-400" />Edit</button>}
          {onDelete && (
            <>
              <div className="my-1 border-t border-border" />
              <button onClick={() => { onDelete(); setOpen(false); }} className="flex w-full items-center gap-2.5 px-4 py-2.5 text-sm font-medium text-rose-600 transition hover:bg-rose-50 dark:text-rose-400 dark:hover:bg-rose-950/30"><Trash2 className="h-4 w-4" />Delete</button>
            </>
          )}
        </div>
      )}
    </div>
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
  const pkgOpts  = openPkgs.map((p) => ({ id: p.id, label: p.package_number, sublabel: `${p.actual_item_count} items` }));
  const pltOpts  = openPlts.map((p) => ({ id: p.id, label: p.pallet_number,  sublabel: `${p.item_count} items` }));

  async function handleMove() {
    if (!targetPkgId && !targetPltId) { setError("Select at least a target package or pallet."); return; }
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
    setAssigning(true); setError("");
    const palletId = targetPalletId.trim() || null;
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
            <img src={p.src} alt={p.label} className="h-full w-full object-cover transition group-hover:scale-105" />
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
      d = d.filter((r) => [r.lpn ?? "", r.item_name, r.product_identifier ?? "", r.id].some((v) => v.toLowerCase().includes(q)));
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
          <div className="overflow-hidden rounded-xl border border-border">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">LPN</th>
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Item</th>
                  <th className="hidden px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400 sm:table-cell">Status</th>
                  <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-slate-400">Date</th>
                  <th className="px-3 py-2" />
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {filtered.map((r) => (
                  <tr key={r.id} onClick={() => onItemClick(r)} className="cursor-pointer transition hover:bg-sky-50/50 dark:hover:bg-sky-950/20">
                    <td className="px-3 py-2.5 font-mono font-bold text-slate-700 dark:text-slate-300">{r.lpn ?? "—"}</td>
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

function PackagesSubTable({ palletId, packages, onPackageClick }: {
  palletId: string; packages: PackageRecord[]; onPackageClick: (p: PackageRecord) => void;
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
            <tr key={p.id} onClick={() => onPackageClick(p)} className="cursor-pointer transition hover:bg-violet-50/50 dark:hover:bg-violet-950/20">
              <td className="px-3 py-2.5 font-mono font-bold text-foreground">{p.package_number}</td>
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

export function ItemDrawerContent({ record, role, actor, packages, pallets, onUpdated, onDeleted, startInEditMode = false, sessionPhotos }: {
  record: ReturnRecord; role: UserRole; actor: string;
  packages: PackageRecord[]; pallets: PalletRecord[];
  onUpdated: (r: ReturnRecord) => void; onDeleted: (id: string) => void;
  startInEditMode?: boolean;
  /** File objects captured in the current browser session — enables live gallery. */
  sessionPhotos?: Record<string, File[]>;
}) {
  const [editing,    setEditing]    = useState(startInEditMode);
  const [saving,     setSaving]     = useState(false);
  const [confirmDel, setConfirmDel] = useState(false);
  const [deleting,   setDeleting]   = useState(false);
  const [err,        setErr]        = useState("");
  const [editLpn,    setEditLpn]    = useState(record.lpn ?? "");
  const [editProductId, setEditProductId] = useState(record.product_identifier ?? "");
  const [editItem,   setEditItem]   = useState(record.item_name);
  const [editNotes,  setEditNotes]  = useState(record.notes ?? "");
  const [editStatus, setEditStatus] = useState(record.status);
  /**
   * Mutable copy of `photo_evidence` counts for edit mode.
   * Each key is a category slug, value is the remaining count of photos in that bucket.
   * Operator clicks X on a placeholder thumbnail → decrements the count.
   * New photos captured in edit mode are appended to `editNewPhotos`.
   */
  const [editPhotoEvidence, setEditPhotoEvidence] = useState<Record<string, number>>(
    record.photo_evidence ?? {},
  );
  const [editNewPhotos, setEditNewPhotos] = useState<Record<string, File[]>>({});
  const [editCatalogStatus, setEditCatalogStatus] = useState<"idle" | "loading" | "local" | "amazon" | "unknown">("idle");
  const [editCatalogPreview, setEditCatalogPreview] = useState<{ name: string; price?: number; image_url?: string } | null>(null);
  const [editExpiryDate,     setEditExpiryDate]     = useState(record.expiration_date ?? "");
  const [editPhotoItemUrl,   setEditPhotoItemUrl]   = useState(record.photo_item_url ?? "");
  const [editPhotoExpiryUrl, setEditPhotoExpiryUrl] = useState(record.photo_expiry_url ?? "");
  const [itemPhotoUploading,   setItemPhotoUploading]   = useState(false);
  const [expiryPhotoUploading, setExpiryPhotoUploading] = useState(false);

  useEffect(() => {
    setEditLpn(record.lpn ?? "");
    setEditProductId(record.product_identifier ?? "");
    setEditItem(record.item_name);
    setEditNotes(record.notes ?? "");
    setEditStatus(record.status);
    setEditPhotoEvidence(record.photo_evidence ?? {});
    setEditExpiryDate(record.expiration_date ?? "");
    setEditPhotoItemUrl(record.photo_item_url ?? "");
    setEditPhotoExpiryUrl(record.photo_expiry_url ?? "");
    setEditCatalogStatus("idle");
    setEditCatalogPreview(null);
  }, [record.id, record.lpn, record.product_identifier, record.item_name, record.notes, record.status, record.photo_evidence, record.expiration_date, record.photo_item_url, record.photo_expiry_url]);

  async function handleItemEvidencePhoto(
    e: React.ChangeEvent<HTMLInputElement>,
    field: "photo_item_url" | "photo_expiry_url",
  ) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    const setUploading = field === "photo_item_url" ? setItemPhotoUploading : setExpiryPhotoUploading;
    const setUrl       = field === "photo_item_url" ? setEditPhotoItemUrl   : setEditPhotoExpiryUrl;
    setUploading(true);
    try {
      const ext  = f.name.split(".").pop() ?? "jpg";
      const path = `evidence/${field}/${record.id}_${Date.now()}.${ext}`;
      const { error: upErr } = await supabaseBrowser.storage.from("media").upload(path, f, { upsert: true });
      if (upErr) throw new Error(upErr.message);
      const { data: urlData } = supabaseBrowser.storage.from("media").getPublicUrl(path);
      setUrl(urlData.publicUrl);
    } catch {
      setErr("Photo upload failed. Try again.");
    } finally {
      setUploading(false);
    }
  }

  async function handleEditBarcodeLookup(barcode: string) {
    if (!barcode.trim()) { setEditCatalogStatus("idle"); return; }
    setEditCatalogStatus("loading");
    setEditCatalogPreview(null);

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
      await supabaseBrowser
        .from("products")
        .insert({ barcode: barcode.trim(), name: amazon.name, price: amazon.price, image_url: amazon.image_url, source: "Amazon" })
        .then(() => {})
        .catch(() => {});
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
  const photoTotal   = record.photo_evidence ? Object.values(record.photo_evidence).reduce((a, b) => a + b, 0) : 0;

  async function handleSave() {
    setSaving(true); setErr("");
    // Merge new photos (counts only — actual files not yet uploaded to cloud storage)
    const newPhotoCount: Record<string, number> = {};
    Object.entries(editNewPhotos).forEach(([cat, files]) => {
      if (files.length) newPhotoCount[cat] = (newPhotoCount[cat] ?? 0) + files.length;
    });
    const mergedPhotoEvidence: Record<string, number> = { ...editPhotoEvidence };
    Object.entries(newPhotoCount).forEach(([cat, n]) => {
      mergedPhotoEvidence[cat] = (mergedPhotoEvidence[cat] ?? 0) + n;
    });
    const res = await updateReturn(record.id, {
      lpn: editLpn || undefined,
      item_name: editItem,
      notes: editNotes || undefined, status: editStatus,
      expiration_date:  editExpiryDate  || undefined,
      photo_evidence:   Object.keys(mergedPhotoEvidence).length ? mergedPhotoEvidence : undefined,
      photo_item_url:   editPhotoItemUrl   || undefined,
      photo_expiry_url: editPhotoExpiryUrl || undefined,
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
        <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold capitalize text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">{record.marketplace}</span>
        {linkedPkg    && <span className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-2.5 py-0.5 text-xs font-semibold text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/50 dark:text-sky-300"><Tag className="h-3 w-3" />{linkedPkg.package_number}</span>}
        {linkedPallet && <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"><Boxes className="h-3 w-3" />{linkedPallet.pallet_number}</span>}
      </div>

      {editing ? (
        <div className="space-y-4">
          <div>
            <label className={LABEL}>Product barcode (ASIN / UPC)</label>
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
          {!record.package_id && (
            <div><label className={LABEL}>LPN <span className="text-xs font-normal text-slate-400">(optional)</span></label><input className={INPUT} value={editLpn} onChange={(e) => setEditLpn(e.target.value)} placeholder="Orphan label scan…" /></div>
          )}
          <div><label className={LABEL}>Item Name</label><input className={INPUT} value={editItem} onChange={(e) => setEditItem(e.target.value)} /></div>

          {/* ── Expiry Date (FEFO) ──────────────────────────────────────────── */}
          <div className="rounded-2xl border border-orange-200 bg-orange-50 p-4 space-y-3 dark:border-orange-700/40 dark:bg-orange-950/30">
            <p className="flex items-center gap-2 text-sm font-bold text-orange-700 dark:text-orange-400">
              <CalendarX2 className="h-4 w-4" />Expiry Date
              <span className="ml-auto rounded-full border border-orange-300 bg-orange-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-orange-600 dark:border-orange-700/60 dark:bg-orange-950/50 dark:text-orange-400">FEFO Tracking</span>
            </p>
            <input
              type="date"
              className={INPUT}
              value={editExpiryDate}
              onChange={(e) => setEditExpiryDate(e.target.value)}
            />
            {editExpiryDate && (
              <p className="text-xs text-orange-600 dark:text-orange-400 flex items-center gap-1">
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                Expiry date set — upload a photo of the label below for claim evidence.
              </p>
            )}
          </div>

          {/* ── Item Condition & Evidence ───────────────────────────────────── */}
          <div className="rounded-2xl border-2 border-sky-200 bg-sky-50 p-4 space-y-3 dark:border-sky-700/50 dark:bg-sky-950/20">
            <div className="flex items-center gap-2">
              <Camera className="h-4 w-4 text-sky-600 dark:text-sky-400" />
              <p className="text-sm font-bold text-sky-800 dark:text-sky-200">Item Condition &amp; Evidence</p>
            </div>

            {/* Item photo */}
            {editPhotoItemUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Item photo saved ✓</span>
                <button type="button" onClick={() => setEditPhotoItemUrl("")} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${itemPhotoUploading ? "border-sky-300 text-sky-500" : "border-sky-300 bg-sky-50 text-sky-700 hover:bg-sky-100 dark:border-sky-700/60 dark:bg-sky-950/30 dark:text-sky-300"}`}>
                {itemPhotoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {itemPhotoUploading ? "Uploading…" : "📸 Upload Item Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={itemPhotoUploading} onChange={(e) => handleItemEvidencePhoto(e, "photo_item_url")} />
              </label>
            )}

            {/* Expiry label photo */}
            {editPhotoExpiryUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Expiry date photo saved ✓</span>
                <button type="button" onClick={() => setEditPhotoExpiryUrl("")} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${expiryPhotoUploading ? "border-orange-300 text-orange-500" : editExpiryDate ? "border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100 dark:border-rose-700/60 dark:bg-rose-950/30 dark:text-rose-300" : "border-orange-300 bg-orange-50 text-orange-700 hover:bg-orange-100 dark:border-orange-700/60 dark:bg-orange-950/30 dark:text-orange-300"}`}>
                {expiryPhotoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {expiryPhotoUploading ? "Uploading…" : "📸 Upload Expiry Date Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={expiryPhotoUploading} onChange={(e) => handleItemEvidencePhoto(e, "photo_expiry_url")} />
              </label>
            )}
          </div>

          {canEdit(role) && (
            <div><label className={LABEL}>Status</label>
              <select className={INPUT} value={editStatus} onChange={(e) => setEditStatus(e.target.value)}>
                {Object.entries(STATUS_CFG).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
              </select>
            </div>
          )}
          <div><label className={LABEL}>Notes</label><textarea rows={3} className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" value={editNotes} onChange={(e) => setEditNotes(e.target.value)} /></div>

          {/* ── Photo Evidence Edit ───────────────────────────────────────────── */}
          {Object.keys(editPhotoEvidence).length > 0 && (
            <div className="space-y-3 rounded-2xl border border-slate-200 p-4 dark:border-slate-700">
              <p className="text-xs font-bold uppercase tracking-wide text-slate-400">Photo Evidence</p>
              <p className="text-xs text-slate-400">Click <span className="font-semibold">✕</span> on a thumbnail to remove that photo slot before saving.</p>
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
                          {/* Always-visible X button */}
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
          )}

          {/* ── Add new photos in edit mode ───────────────────────────────────── */}
          {Object.keys(editPhotoEvidence).length > 0 && (
            <div className="space-y-2 rounded-2xl border border-slate-200 p-4 dark:border-slate-700">
              <p className="text-xs font-bold uppercase tracking-wide text-slate-400">Add New Photos</p>
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
          )}

          {err && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{err}</p>}
          <div className="flex gap-3">
            <button onClick={() => { setEditing(false); setEditLpn(record.lpn ?? ""); setEditProductId(record.product_identifier ?? ""); setEditPhotoEvidence(record.photo_evidence ?? {}); setEditNewPhotos({}); }} className={`${BTN_GHOST} flex-1 h-12`}><XCircle className="h-4 w-4" />Cancel</button>
            <button onClick={handleSave} disabled={saving} className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl bg-sky-500 font-semibold text-white hover:bg-sky-600 disabled:opacity-50">
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}Save
            </button>
          </div>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div className="col-span-2"><p className="text-xs text-slate-400">Product ID</p><p className="font-mono font-bold text-foreground">{record.product_identifier ?? "—"}</p></div>
            {record.lpn && <div><p className="text-xs text-slate-400">LPN</p><p className="font-mono font-bold text-foreground">{record.lpn}</p></div>}
            <div className="col-span-2"><p className="text-xs text-slate-400">Item</p><p className="font-semibold text-foreground">{record.item_name}</p></div>
            {record.expiration_date && (
              <div className="col-span-2 rounded-xl border border-orange-200 bg-orange-50 px-3 py-2 dark:border-orange-700/40 dark:bg-orange-950/30">
                <p className="text-[10px] font-bold uppercase tracking-wide text-orange-500 mb-0.5">Expiry (FEFO)</p>
                <p className="font-mono font-bold text-orange-700 dark:text-orange-300">{record.expiration_date}</p>
              </div>
            )}
            {(record.photo_item_url || record.photo_expiry_url) && (
              <div className="col-span-2">
                <p className="text-[10px] font-bold uppercase tracking-wide text-slate-400 mb-1.5">Evidence Photos</p>
                <div className="flex flex-wrap gap-2">
                  {record.photo_item_url && <span className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 text-xs font-medium text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/40 dark:text-sky-300"><Camera className="h-3 w-3" />Item Photo ✓</span>}
                  {record.photo_expiry_url && <span className="inline-flex items-center gap-1 rounded-full border border-orange-200 bg-orange-50 px-2.5 py-1 text-xs font-medium text-orange-700 dark:border-orange-700/60 dark:bg-orange-950/30 dark:text-orange-300"><Camera className="h-3 w-3" />Expiry Photo ✓</span>}
                </div>
              </div>
            )}
            {(record.inherited_tracking_number || linkedPkg?.tracking_number) && (
              <div className="col-span-2">
                <p className="text-xs text-slate-400">Tracking (from package)</p>
                <p className="font-mono text-sm text-foreground">{record.inherited_tracking_number ?? linkedPkg?.tracking_number ?? "—"}</p>
                {(record.inherited_carrier || linkedPkg?.carrier_name) && (
                  <p className="text-xs text-muted-foreground">{record.inherited_carrier ?? linkedPkg?.carrier_name}</p>
                )}
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
                Photo Evidence ({photoTotal} photo{photoTotal !== 1 ? "s" : ""})
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
                    {Object.entries(record.photo_evidence).filter(([, n]) => n > 0).map(([cat, n]) => (
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
            <div><p className="text-slate-400">By</p><p className="font-semibold capitalize text-slate-700 dark:text-slate-300">{record.created_by}</p></div>
            <div><p className="text-slate-400">Date</p><p className="font-semibold text-slate-700 dark:text-slate-300">{fmt(record.created_at)}</p></div>
          </div>
          <div className="flex gap-3">
            <button onClick={() => setEditing(true)} className={`${BTN_GHOST} flex-1 h-12`}><Pencil className="h-4 w-4" />Edit</button>
            {canDelete(role) && !confirmDel && (
              <button onClick={() => setConfirmDel(true)} className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl border border-rose-200 text-sm font-semibold text-rose-600 hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30">
                <Trash2 className="h-4 w-4" />Delete
              </button>
            )}
            {confirmDel && (
              <div className="flex flex-1 items-center gap-2">
                <p className="flex-1 text-xs text-rose-600">Cannot be undone.</p>
                <button onClick={() => setConfirmDel(false)} className={BTN_GHOST}>Cancel</button>
                <button onClick={handleDelete} disabled={deleting} className="flex h-10 items-center gap-1.5 rounded-xl bg-rose-600 px-3 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-60">
                  {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}Yes
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
    return allReturns.filter((r) => !currentIds.has(r.id) && (!q || [r.item_name, r.product_identifier ?? ""].join(" ").toLowerCase().includes(q)));
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
                  <p className="font-mono text-xs text-muted-foreground">{r.product_identifier ?? "—"}</p>
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

export function PackageDrawerContent({ pkg: initPkg, role, actor, openPallets = [], allReturns = [], onClose, onPackageUpdated, onItemAdded, onPackageDeleted, onOpenItem, showToast }: {
  pkg: PackageRecord; role: UserRole; actor: string;
  openPallets?: PalletRecord[];
  /** Full returns list from page state — used for the "Assign Existing Item" flow. */
  allReturns?: ReturnRecord[];
  onClose: () => void;
  onPackageUpdated: (p: PackageRecord) => void;
  onItemAdded: (r: ReturnRecord) => void;
  onPackageDeleted: (id: string) => void;
  onOpenItem: (r: ReturnRecord) => void;
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
  const slipCameraRef = useRef<HTMLInputElement>(null);
  const [saveErr,    setSaveErr]    = useState("");
  const [saving,     setSaving]     = useState(false);

  const [editPhotoClosedUrl,        setEditPhotoClosedUrl]        = useState(initPkg.photo_closed_url        ?? "");
  const [editPhotoOpenedUrl,        setEditPhotoOpenedUrl]        = useState(initPkg.photo_opened_url        ?? "");
  const [editPhotoReturnLabelUrl,   setEditPhotoReturnLabelUrl]   = useState(initPkg.photo_return_label_url  ?? "");
  const [editPhotoClosedUploading,  setEditPhotoClosedUploading]  = useState(false);
  const [editPhotoOpenedUploading,  setEditPhotoOpenedUploading]  = useState(false);
  const [editPhotoReturnLabelUploading, setEditPhotoReturnLabelUploading] = useState(false);

  // ── Auto-fill carrier from pallet's existing packages (skip initial mount) ─
  const pkgPalletMounted = useRef(false);
  useEffect(() => {
    if (!pkgPalletMounted.current) { pkgPalletMounted.current = true; return; }
    if (!editPalletId) return;
    supabaseBrowser
      .from("packages")
      .select("carrier_name")
      .eq("pallet_id", editPalletId)
      .not("carrier_name", "is", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle()
      .then(({ data }) => {
        if (data?.carrier_name) setEditCarrier(data.carrier_name as string);
      });
  }, [editPalletId]);

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
    setUploading(true);
    try {
      const ext  = f.name.split(".").pop() ?? "jpg";
      const path = `packages/claim_${field}/${pkg.id}_${Date.now()}.${ext}`;
      const { error: upErr } = await supabaseBrowser.storage.from("media").upload(path, f, { upsert: true });
      if (upErr) throw new Error(upErr.message);
      const { data: urlData } = supabaseBrowser.storage.from("media").getPublicUrl(path);
      setUrl(urlData.publicUrl);
    } catch {
      setSaveErr("Photo upload failed. Try again.");
    } finally {
      setUploading(false);
    }
  }
  /** AI-read expected lines from a real camera/file upload — drives reconciliation. */
  const [expectedItems, setExpectedItems]   = useState<SlipExpectedItem[] | null>(null);
  const [ocrRunning,    setOcrRunning]      = useState(false);
  const [ocrConfidence, setOcrConfidence]   = useState<number | null>(null);
  const [ocrErr,        setOcrErr]          = useState("");
  const [lastSlipName,  setLastSlipName]    = useState<string | null>(null);

  const mismatch   = pkg.expected_item_count > 0 && pkg.actual_item_count !== pkg.expected_item_count;
  const pct        = pkg.expected_item_count > 0 ? Math.min(100, (pkg.actual_item_count / pkg.expected_item_count) * 100) : null;
  const remaining  = pkg.expected_item_count > 0 ? pkg.expected_item_count - pkg.actual_item_count : null;
  const atCapacity = remaining !== null && remaining <= 0;

  useEffect(() => { listReturnsByPackage(pkg.id).then((r) => { if (r.ok) setItems(r.data); setLoading(false); }); }, [pkg.id]);

  /**
   * Mock OCR: simulates reading a packing-slip photo without any real API call.
   * Returns the two fixed test barcodes used for green/yellow/red reconciliation testing.
   */
  async function mockManifestOcr(_file: File): Promise<SlipExpectedItem[]> {
    await new Promise((r) => setTimeout(r, 1400)); // realistic scanning delay
    return [
      { barcode: "111", name: "Item 111", expected_qty: 1 },
      { barcode: "222", name: "Item 222", expected_qty: 2 },
    ];
  }

  /** Native camera / gallery: `<input type="file" accept="image/*" capture="environment" />` */
  async function handleImageUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file || !file.type.startsWith("image/")) {
      setOcrErr("Please choose an image file.");
      return;
    }

    setOcrRunning(true);
    setOcrErr("");
    setOcrConfidence(null);
    setLastSlipName(file.name);

    try {
      const items = await mockManifestOcr(file);
      setExpectedItems(items.length > 0 ? items : null);
      setOcrConfidence(items.length > 0 ? 1 : 0);
      if (items.length === 0) {
        setOcrErr("No items detected on the packing slip — try a clearer photo.");
      }
    } catch (err) {
      setOcrErr(err instanceof Error ? err.message : "Upload or OCR failed. Try again.");
    } finally {
      setOcrRunning(false);
    }
  }

  function handleItemAdded(r: ReturnRecord) { setItems((p) => [r, ...p]); setPkg((p) => ({ ...p, actual_item_count: p.actual_item_count + 1 })); onItemAdded(r); }
  function handleItemDeleted(id: string) { setItems((p) => p.filter((r) => r.id !== id)); setPkg((p) => ({ ...p, actual_item_count: Math.max(0, p.actual_item_count - 1) })); }

  async function handleSaveEdits() {
    setSaving(true); setSaveErr("");
    const res = await updatePackage(pkg.id, {
      carrier_name:         editCarrier    || undefined,
      tracking_number:      editTracking   || undefined,
      rma_number:           editRmaNumber  || null,
      expected_item_count:  parseInt(editExpected, 10) || 0,
      ...(editPalletId           ? { pallet_id:              editPalletId           } : {}),
      ...(editPhotoClosedUrl      ? { photo_closed_url:       editPhotoClosedUrl      } : {}),
      ...(editPhotoOpenedUrl      ? { photo_opened_url:       editPhotoOpenedUrl      } : {}),
      ...(editPhotoReturnLabelUrl ? { photo_return_label_url: editPhotoReturnLabelUrl } : {}),
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
        {pkg.carrier_name && <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"><Truck className="h-3 w-3" />{pkg.carrier_name}</span>}
        {pkg.tracking_number && <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 font-mono text-xs text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400"><QrCode className="h-3 w-3" />{pkg.tracking_number}</span>}
        {pkg.rma_number && <span className="inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-2.5 py-0.5 font-mono text-xs text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300"><Tag className="h-3 w-3" />{pkg.rma_number}</span>}
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
          <div><label className={LABEL}>Carrier</label>
            <select className={INPUT} value={editCarrier} onChange={(e) => setEditCarrier(e.target.value)}>
              <option value="">Select carrier…</option>
              {CARRIERS.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
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

          {/* ── Claim Evidence Photos ──────────────────────────────────── */}
          <div className="rounded-2xl border-2 border-rose-200 bg-rose-50 p-4 space-y-3 dark:border-rose-700/50 dark:bg-rose-950/20">
            <div className="flex items-center gap-2">
              <Camera className="h-4 w-4 text-rose-600 dark:text-rose-400" />
              <p className="text-sm font-bold text-rose-800 dark:text-rose-200">Claim Evidence Photos</p>
            </div>
            {editPhotoClosedUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Closed box photo saved ✓</span>
                <button type="button" onClick={() => setEditPhotoClosedUrl("")} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoClosedUploading ? "border-rose-300 text-rose-500" : "border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100 dark:border-rose-700/60 dark:bg-rose-950/30 dark:text-rose-300"}`}>
                {editPhotoClosedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoClosedUploading ? "Uploading…" : "📸 Upload Closed Box Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoClosedUploading} onChange={(e) => handlePkgClaimPhoto(e, "closed")} />
              </label>
            )}
            {editPhotoOpenedUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Opened box photo saved ✓</span>
                <button type="button" onClick={() => setEditPhotoOpenedUrl("")} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoOpenedUploading ? "border-amber-300 text-amber-500" : "border-amber-300 bg-amber-50 text-amber-700 hover:bg-amber-100 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300"}`}>
                {editPhotoOpenedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoOpenedUploading ? "Uploading…" : "📸 Upload Opened Box Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoOpenedUploading} onChange={(e) => handlePkgClaimPhoto(e, "opened")} />
              </label>
            )}

            {/* Return shipping label */}
            {editPhotoReturnLabelUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Return label photo saved ✓</span>
                <button type="button" onClick={() => setEditPhotoReturnLabelUrl("")} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${editPhotoReturnLabelUploading ? "border-violet-300 text-violet-500" : "border-violet-300 bg-violet-50 text-violet-700 hover:bg-violet-100 dark:border-violet-700/60 dark:bg-violet-950/30 dark:text-violet-300"}`}>
                {editPhotoReturnLabelUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {editPhotoReturnLabelUploading ? "Uploading…" : "📸 Upload Return Label Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={editPhotoReturnLabelUploading} onChange={(e) => handlePkgClaimPhoto(e, "return_label")} />
              </label>
            )}
          </div>

          <div className="rounded-xl border border-violet-200 bg-violet-50 px-3 py-2 text-xs text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/20 dark:text-violet-300">
            💡 Use the <strong>Scan Packing Slip</strong> button above (outside edit mode) to load the manifest and enable reconciliation.
          </div>
          {saveErr && <p className="text-sm text-rose-600 dark:text-rose-400">{saveErr}</p>}
          <div className="flex gap-3">
            <button onClick={() => setEditing(false)} className={`${BTN_GHOST} flex-1 h-12`}><XCircle className="h-4 w-4" />Cancel</button>
            <button onClick={handleSaveEdits} disabled={saving} className="flex h-12 flex-1 items-center justify-center gap-2 rounded-2xl bg-sky-500 font-semibold text-white disabled:opacity-50 hover:bg-sky-600">
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}Save
            </button>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3 text-sm">
          {pkg.rma_number && (
            <div className="col-span-2">
              <p className="text-xs text-slate-400">RMA #</p>
              <p className="font-mono font-bold text-foreground">{pkg.rma_number}</p>
            </div>
          )}
          <div><p className="text-xs text-slate-400">Operator</p><p className="font-semibold capitalize text-foreground">{pkg.created_by}</p></div>
          <div><p className="text-xs text-slate-400">Created</p><p className="font-semibold text-foreground">{fmt(pkg.created_at)}</p></div>
          {(pkg.photo_closed_url || pkg.photo_opened_url || pkg.photo_return_label_url) && (
            <div className="col-span-2 flex flex-wrap gap-1.5 pt-1">
              {pkg.photo_closed_url && <a href={pkg.photo_closed_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 rounded-full bg-rose-100 px-2.5 py-0.5 text-[10px] font-bold text-rose-700 hover:bg-rose-200 dark:bg-rose-900/40 dark:text-rose-300"><Camera className="h-3 w-3" />Closed Box ✓</a>}
              {pkg.photo_opened_url && <a href={pkg.photo_opened_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2.5 py-0.5 text-[10px] font-bold text-amber-700 hover:bg-amber-200 dark:bg-amber-900/40 dark:text-amber-300"><Camera className="h-3 w-3" />Opened Box ✓</a>}
              {pkg.photo_return_label_url && <a href={pkg.photo_return_label_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 rounded-full bg-violet-100 px-2.5 py-0.5 text-[10px] font-bold text-violet-700 hover:bg-violet-200 dark:bg-violet-900/40 dark:text-violet-300"><Camera className="h-3 w-3" />Return Label ✓</a>}
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      {!editing && (
        <div className="flex flex-wrap gap-2">
          {pkg.status === "open" && <button onClick={() => setWizardOpen(true)} className="flex flex-1 h-12 items-center justify-center gap-2 rounded-2xl bg-sky-500 text-sm font-semibold text-white hover:bg-sky-600"><ScanLine className="h-4 w-4" />Scan New Item</button>}
          {pkg.status === "open" && allReturns.length > 0 && <button onClick={() => setAssignOpen(true)} className="flex h-12 items-center justify-center gap-2 rounded-2xl border border-sky-300 bg-sky-50 px-4 text-sm font-semibold text-sky-700 hover:bg-sky-100 dark:border-sky-700/60 dark:bg-sky-950/30 dark:text-sky-300 dark:hover:bg-sky-950/50"><Plus className="h-4 w-4" />Assign Existing</button>}
          <button onClick={() => setEditing(true)} className={`${BTN_GHOST} h-12 px-4`}><Pencil className="h-4 w-4" />Edit</button>
          {pkg.status === "open" && <button onClick={() => mismatch ? setDiscOpen(true) : handleClose()} disabled={closing} className={`${BTN_GHOST} h-12 px-4`}>{closing ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckSquare className="h-4 w-4" />}Close</button>}
          {canDelete(role) && <button onClick={async () => { const r = await deletePackage(pkg.id, actor); if (r.ok) { onPackageDeleted(pkg.id); onClose(); } else showToast(r.error ?? "Delete failed.", "error"); }} className="flex h-12 items-center gap-2 rounded-2xl border border-rose-200 px-4 text-sm font-semibold text-rose-600 hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30"><Trash2 className="h-4 w-4" /></button>}
        </div>
      )}

      {/* Assign Existing Item Modal */}
      {assignOpen && <AssignExistingItemModal pkg={pkg} allReturns={allReturns} currentItems={items} actor={actor} onAssigned={(updated) => { setItems((p) => [...p.filter((i) => i.id !== updated.id), updated]); showToast(`✓ Item assigned to ${pkg.package_number}`); setAssignOpen(false); }} onClose={() => setAssignOpen(false)} />}

      {/* ── Packing Slip / Manifest ── */}
      <div className="rounded-2xl border-2 border-dashed border-violet-200 p-4 space-y-3 dark:border-violet-800/50">
        <p className="text-sm font-bold text-violet-700 dark:text-violet-300">Packing Slip / Manifest</p>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1 rounded-full bg-violet-100 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:bg-violet-900/40 dark:text-violet-300"><Sparkles className="h-3 w-3" />Packing slip</span>
            {ocrConfidence !== null && !ocrRunning && expectedItems && (
              <span className="text-[10px] font-semibold text-emerald-600 dark:text-emerald-400">{Math.round(ocrConfidence * 100)}% confidence</span>
            )}
          </div>
          {expectedItems && (
            <button type="button" onClick={() => { setExpectedItems(null); setOcrConfidence(null); setLastSlipName(null); }} className="text-[10px] text-slate-400 hover:text-slate-600 underline">
              Clear expected list
            </button>
          )}
        </div>

        <input
          ref={slipCameraRef}
          type="file"
          accept="image/*"
          capture="environment"
          className="hidden"
          onChange={handleImageUpload}
        />

        {ocrRunning ? (
          <div className="flex items-center gap-3 rounded-xl bg-violet-50 px-4 py-3 dark:bg-violet-950/30">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin text-violet-500" />
            <span className="text-sm font-semibold text-violet-800 dark:text-violet-200">AI analyzing…</span>
          </div>
        ) : expectedItems ? (
          <p className="text-xs text-emerald-700 dark:text-emerald-400">
            ✓ <strong>{expectedItems.length}</strong> expected line-items from{" "}
            {lastSlipName ? <span className="font-mono">{lastSlipName}</span> : "your photo"}.
            {" "}Reconciliation table shown below.
          </p>
        ) : (
          <div className="space-y-2">
            <button
              type="button"
              onClick={() => slipCameraRef.current?.click()}
              className="flex w-full items-center justify-center gap-2 rounded-xl bg-violet-600 py-3.5 text-sm font-semibold text-white shadow-sm transition hover:bg-violet-700"
            >
              📸 Take Photo of List
            </button>
            {/* Mock data button — loads demo manifest to test the reconciliation table */}
            <button
              type="button"
              onClick={() => {
                const mockItems: SlipExpectedItem[] = [
                  { barcode: "111", name: "Item 111", expected_qty: 1 },
                  { barcode: "222", name: "Item 222", expected_qty: 2 },
                ];
                setExpectedItems(mockItems);
                setOcrConfidence(1);
                setLastSlipName("mock-manifest.json");
                setOcrErr("");
              }}
              className="flex w-full items-center justify-center gap-2 rounded-xl border border-violet-300 bg-violet-50 py-2.5 text-xs font-semibold text-violet-700 transition hover:bg-violet-100 dark:border-violet-700/50 dark:bg-violet-950/30 dark:text-violet-300"
            >
              🧪 Load Mock Manifest (Test Green/Yellow/Red)
            </button>
          </div>
        )}

        {!ocrRunning && !expectedItems && (
          <p className="text-center text-[11px] text-muted-foreground">
            Upload or photograph a packing slip — mock OCR will return test barcodes{" "}
            <span className="font-mono">111</span> &amp; <span className="font-mono">222</span> and render the reconciliation table below.
          </p>
        )}

        {ocrErr && (
          <p className="text-xs text-rose-600 dark:text-rose-400">
            {ocrErr}{" "}
            <button type="button" className="font-semibold underline" onClick={() => slipCameraRef.current?.click()}>
              Try again
            </button>
          </p>
        )}

        {/* ── Reconciliation Table — rendered directly below the upload button ── */}
        {expectedItems && !loading && (
          <div className="mt-4 space-y-2">
            <div className="flex items-center gap-2">
              <p className="text-xs font-bold uppercase tracking-widest text-slate-400">Reconciliation</p>
              <span className="inline-flex items-center gap-1 rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-bold text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
                <Sparkles className="h-2.5 w-2.5" />Manifest vs Scanned
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
                  {expectedItems.map((exp, slipIdx) => {
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
                    .filter((it) => !expectedItems.some((exp) => physicalItemMatchesExpectedLine(it, exp)))
                    .map((it) => (
                      <tr key={it.id} className="bg-amber-50/70 dark:bg-amber-950/20">
                        <td className="px-3 py-2.5">
                          <p className="font-semibold text-slate-700 dark:text-slate-300">{it.item_name}</p>
                          <p className="font-mono text-slate-400">{it.product_identifier ?? "—"}</p>
                        </td>
                        <td className="px-3 py-2.5 text-center text-slate-400">—</td>
                        <td className="px-3 py-2.5 text-center font-bold text-amber-600">1</td>
                        <td className="px-3 py-2.5">
                          <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">🟡 Unexpected</span>
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
              {items.length === 0 && expectedItems.length > 0 && (
                <p className="py-4 text-center text-xs text-slate-400">
                  No items scanned yet — all {expectedItems.length} expected line-items are missing.
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
        // Merge session-local expectedItems into the package so the wizard's
        // itemMatchesPackageExpectation can check against the real manifest.
        const pkgWithExpected: PackageRecord = {
          ...pkg,
          expected_items: (expectedItems ?? []).map((e) => ({
            sku: e.barcode,
            expected_qty: e.expected_qty ?? 1,
            description: e.name,
          })),
        };
        return (
          <SingleItemWizardModal
            onClose={() => setWizardOpen(false)}
            onSuccess={(r) => { handleItemAdded(r); showToast(`✓ Item logged to ${pkg.package_number} — ${r.product_identifier ?? r.item_name}`); }}
            actor={actor}
            openPackages={[pkgWithExpected]}
            openPallets={[]}
            onCreatePackage={() => {}}
            onCreatePallet={() => {}}
            inheritedContext={{ packageId: pkg.id, packageLabel: pkg.package_number }}
            onSoftPackageWarning={() => showToast("⚠ This item is not on the scanned packing slip.", "warning")}
          />
        );
      })()}
      {discOpen && <DiscrepancyModal pkg={pkg} onConfirm={(note) => handleClose(note)} onCancel={() => setDiscOpen(false)} />}
    </div>
  );
}

// ─── Pallet Drawer Content ─────────────────────────────────────────────────────

export function PalletDrawerContent({ pallet, role, actor, packages, onClose, onPalletUpdated, onPalletDeleted, onOpenPackage, showToast }: {
  pallet: PalletRecord; role: UserRole; actor: string; packages: PackageRecord[];
  onClose: () => void;
  onPalletUpdated: (p: PalletRecord) => void;
  onPalletDeleted: (id: string) => void;
  onOpenPackage: (p: PackageRecord) => void;
  showToast: (msg: string, kind?: ToastKind) => void;
}) {
  const [plt, setPlt] = useState(pallet);

  async function handleClose() {
    const res = await updatePalletStatus(plt.id, "closed", actor);
    if (res.ok) { setPlt((p) => ({ ...p, status: "closed" })); onPalletUpdated({ ...plt, status: "closed" }); showToast("Pallet closed."); }
    else showToast(res.error ?? "Failed.", "error");
  }

  return (
    <div className="p-4 sm:p-6 space-y-5">
      <PalletStatusBadge status={plt.status} />
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div><p className="text-xs text-slate-400">Total Items</p><p className="text-3xl font-extrabold text-foreground">{plt.item_count}</p></div>
        <div><p className="text-xs text-slate-400">Operator</p><p className="font-semibold capitalize text-foreground">{plt.created_by}</p></div>
        <div><p className="text-xs text-slate-400">Created</p><p className="font-semibold">{fmt(plt.created_at)}</p></div>
        <div><p className="text-xs text-slate-400">Updated</p><p className="font-semibold">{fmt(plt.updated_at)}</p></div>
      </div>
      {plt.notes && <p className="rounded-xl bg-slate-50 p-3 text-sm text-slate-700 dark:bg-slate-900 dark:text-slate-300">{plt.notes}</p>}
      {plt.manifest_photo_url && (
        <div><p className="text-xs font-semibold uppercase tracking-wide text-slate-400 mb-2">Manifest</p><img src={plt.manifest_photo_url} alt="Manifest" className="h-36 w-full rounded-xl border border-slate-200 object-cover dark:border-slate-700" /></div>
      )}

      {/* Packages sub-table */}
      <div>
        <p className="mb-2 text-xs font-bold uppercase tracking-widest text-slate-400">Packages in this Pallet</p>
        <PackagesSubTable palletId={plt.id} packages={packages} onPackageClick={onOpenPackage} />
      </div>

      {/* Pallet info note: drill into packages above to see items */}
      <p className="rounded-xl bg-sky-50 px-4 py-2 text-xs text-sky-700 dark:bg-sky-950/40 dark:text-sky-300">
        💡 Tap a package above to open it and view or scan its items.
      </p>

      <div className="flex flex-wrap gap-2">
        {canEdit(role) && plt.status === "open" && <button onClick={handleClose} className={`${BTN_GHOST} h-12 flex-1 px-4`}><CheckSquare className="h-4 w-4" />Close Pallet</button>}
        {canDelete(role) && <button onClick={async () => { const r = await deletePallet(plt.id, actor); if (r.ok) { onPalletDeleted(plt.id); onClose(); } else showToast(r.error ?? "Failed.", "error"); }} className="flex h-12 items-center gap-2 rounded-2xl border border-rose-200 px-4 text-sm font-semibold text-rose-600 hover:bg-rose-50 dark:border-rose-700/60 dark:text-rose-400 dark:hover:bg-rose-950/30"><Trash2 className="h-4 w-4" /></button>}
      </div>
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

export function WizardStep1({ state, setState, openPackages, openPallets, onCreatePackage, onCreatePallet, inherited, aiLabelEnabled = false, onAdvance }: {
  state: WizardState; setState: React.Dispatch<React.SetStateAction<WizardState>>;
  openPackages: PackageRecord[]; openPallets: PalletRecord[];
  onCreatePackage: () => void; onCreatePallet: () => void;
  inherited?: WizardInheritedContext;
  aiLabelEnabled?: boolean;
  /** Scanner UX: called when Enter is pressed on the last wizard field and all step-1 fields are valid. */
  onAdvance?: () => void;
}) {
  const up = (k: keyof WizardState, v: unknown) => setState((p) => ({ ...p, [k]: v }));
  const pkgOpts = openPackages.map((p) => ({ id: p.id, label: p.package_number, sublabel: `${p.actual_item_count}/${p.expected_item_count > 0 ? p.expected_item_count : "?"} items` }));
  const pltOpts = openPallets.map((p) => ({ id: p.id, label: p.pallet_number, sublabel: `${p.item_count} items` }));
  const hasPackageLink = !!(inherited?.packageId ?? state.package_link_id);
  const [ocrLoad, setOcrLoad] = useState(false);
  const [ocrBanner, setOcrBanner] = useState<{ ok: boolean; msg: string } | null>(null);
  const ocrFileRef = useRef<HTMLInputElement>(null);
  // Scanner keyboard-nav refs
  const itemNameRef = useRef<HTMLInputElement>(null);

  // ── Connected stores for the Store ID dropdown (fetched from stores table) ─
  const [connectedStores, setConnectedStores] = useState<
    { id: string; name: string; platform: string }[]
  >([]);
  const [storeInherited, setStoreInherited] = useState(false);
  useEffect(() => {
    supabaseBrowser
      .from("stores")
      .select("id, name, platform")
      .eq("is_active", true)
      .order("created_at", { ascending: false })
      .then(({ data }) => { if (data) setConnectedStores(data as { id: string; name: string; platform: string }[]); })
      .catch(() => {});
  }, []);

  // ── Auto-fill store_id from linked package (Package → Item inheritance) ───
  useEffect(() => {
    const pkgId = state.package_link_id || inherited?.packageId;
    if (!pkgId) { setStoreInherited(false); return; }
    const pkg = openPackages.find((p) => p.id === pkgId);
    if (pkg?.store_id) {
      up("store_id", pkg.store_id);
      setStoreInherited(true);
    } else {
      setStoreInherited(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.package_link_id, inherited?.packageId]);

  // ── Catalog lookup state ──────────────────────────────────────────────────
  const [catalogStatus, setCatalogStatus] = useState<"idle" | "loading" | "local" | "amazon" | "unknown">("idle");
  const [catalogPreview, setCatalogPreview] = useState<{ name: string; price?: number; image_url?: string } | null>(null);

  async function handleBarcodeLookup(barcode: string) {
    if (!barcode.trim()) { setCatalogStatus("idle"); return; }
    setCatalogStatus("loading");
    setCatalogPreview(null);

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
        up("store_id", amazonStore?.id ?? getDefaultStoreIdFromStorage());
      } else if (!state.store_id) {
        // No known prefix → fallback to operator's saved default store
        const fallback = getDefaultStoreIdFromStorage();
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
      setCatalogStatus("local");
      return;
    }

    // Step B: call the Amazon adapter
    const amazon = await fetchProductFromAmazon(barcode.trim());
    if (amazon) {
      up("item_name", amazon.name);
      setCatalogPreview({ name: amazon.name, price: amazon.price, image_url: amazon.image_url });
      setCatalogStatus("amazon");
      // Cache the result locally so the next scan is instant
      await supabaseBrowser
        .from("products")
        .insert({ barcode: barcode.trim(), name: amazon.name, price: amazon.price, image_url: amazon.image_url, source: "Amazon" })
        .then(() => {})
        .catch(() => {});
      return;
    }

    setCatalogStatus("unknown");
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
        product_identifier: res.data!.product_identifier ?? p.product_identifier,
        marketplace: res.data!.marketplace as typeof p.marketplace,
      }));
      setOcrBanner({ ok: true, msg: `AI Scan — ${Math.round(res.data.confidence * 100)}% confidence. Verify fields below.` });
    } else {
      setOcrBanner({ ok: false, msg: res.error ?? "Scan failed. Enter manually." });
    }
  }

  return (
    <div className="space-y-5">
      {/* ── HIERARCHY: Package link at the very top (Items → Package → Pallet) ── */}
      {!inherited?.packageId && (
        <div className="rounded-2xl border-2 border-sky-200 bg-sky-50 p-3 dark:border-sky-700/50 dark:bg-sky-950/30">
          <p className="mb-2 flex items-center gap-2 text-xs font-bold text-sky-700 dark:text-sky-300">
            <Package2 className="h-3.5 w-3.5" />Step 1 — Assign to Package <span className="font-normal text-sky-500">(recommended)</span>
          </p>
          <ComboboxField label="" hint="" icon={Tag} options={pkgOpts} value={state.package_link_id} onChange={(id) => up("package_link_id", id)} onClear={() => up("package_link_id", "")} placeholder="Scan tracking # or search package…" onCreateNew={onCreatePackage} createLabel="Create new package…" />
          {!state.package_link_id && <p className="mt-1.5 text-[10px] text-sky-500">⚠ Without a package this item will be marked <strong>Orphaned / Loose</strong></p>}
        </div>
      )}
      {inherited && (
        <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-700/50 dark:bg-emerald-950/30">
          <CheckCircle2 className="h-5 w-5 shrink-0 text-emerald-600 dark:text-emerald-400" />
          <div><p className="text-xs font-bold text-emerald-700 dark:text-emerald-300">Context Inherited — fields pre-filled</p>
            <p className="text-xs text-emerald-600 dark:text-emerald-400">{inherited.packageLabel && <>Package: <span className="font-mono font-bold">{inherited.packageLabel}</span></>}{inherited.palletLabel && <> · Pallet: <span className="font-mono font-bold">{inherited.palletLabel}</span></>}</p>
          </div>
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
              className={`${INPUT} pl-11 transition-all ${catalogStatus === "unknown" ? "border-yellow-400 ring-2 ring-yellow-300 focus:border-yellow-400 focus:ring-yellow-300" : ""}`}
              placeholder="Scan or type product identifier…"
              value={state.product_identifier}
              onChange={(e) => { up("product_identifier", e.target.value); setCatalogStatus("idle"); }}
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
        </div>
        {/* Catalog lookup feedback */}
        {catalogStatus === "loading" && (
          <div className="mt-1.5 flex items-center gap-2 text-xs text-slate-400">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />Looking up barcode…
          </div>
        )}
        {catalogStatus === "local" && catalogPreview && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-300">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
            Found in Catalog — {catalogPreview.name}
          </div>
        )}
        {catalogStatus === "amazon" && catalogPreview && (
          <div className="mt-1.5 flex items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-3 py-1.5 text-xs font-semibold text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/30 dark:text-sky-300">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
            Found on Amazon — {catalogPreview.name}{catalogPreview.price != null ? ` · $${catalogPreview.price.toFixed(2)}` : ""}
          </div>
        )}
        {catalogStatus === "unknown" && (
          <div className="mt-1.5 rounded-xl border-2 border-yellow-400 bg-yellow-50 px-3 py-2 text-xs font-bold text-yellow-800 dark:border-yellow-500/60 dark:bg-yellow-950/20 dark:text-yellow-300">
            ⚠️ Unknown Item — Not found locally or on Amazon.
          </div>
        )}
      </div>
      {!hasPackageLink && (
        <div>
          <label className={LABEL}>Return label / LPN <span className="ml-1 text-xs font-normal text-slate-400">(optional — orphaned items)</span></label>
          <div className="relative"><Barcode className="pointer-events-none absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" /><input type="text" className={`${INPUT} pl-11`} placeholder="Only if not assigned to a package…" value={state.lpn} onChange={(e) => up("lpn", e.target.value)} /></div>
        </div>
      )}
      {hasPackageLink && (
        <p className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-xs text-sky-800 dark:border-sky-800/60 dark:bg-sky-950/30 dark:text-sky-200">
          Tracking and carrier apply at the <span className="font-semibold">package</span> level — this item inherits them when saved.
        </p>
      )}
      <div>
        <label className={LABEL}>Marketplace <span className="text-rose-500">*</span></label>
        <select className={INPUT} value={state.marketplace} onChange={(e) => up("marketplace", e.target.value)}>
          <option value="">Select…</option>
          {MARKETPLACES.map((m) => <option key={m} value={m}>{MP_LABELS[m]}</option>)}
        </select>
      </div>
      {connectedStores.length > 0 && (
        <div>
          <label className={LABEL}>
            Source Store
            {storeInherited && (
              <span className="ml-2 text-xs font-normal text-emerald-600 dark:text-emerald-400">
                · Inherited from Package
              </span>
            )}
          </label>
          <div className="flex gap-2">
            <select
              className={`${INPUT} flex-1`}
              value={state.store_id}
              onChange={(e) => { up("store_id", e.target.value); setStoreInherited(false); }}
              disabled={storeInherited}
            >
              <option value="">— Select store (optional) —</option>
              {connectedStores.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name} ({s.platform})
                </option>
              ))}
            </select>
            {storeInherited && (
              <button
                type="button"
                onClick={() => { up("store_id", ""); setStoreInherited(false); }}
                className="shrink-0 rounded-xl border border-slate-200 px-3 text-xs font-medium text-muted-foreground hover:bg-accent dark:border-slate-700"
              >
                Override
              </button>
            )}
          </div>
          {storeInherited && (
            <p className="mt-1 text-[11px] text-emerald-600 dark:text-emerald-400">
              🔒 Auto-filled from package. Click Override to select a different store.
            </p>
          )}
        </div>
      )}
      <div>
        <label className={LABEL}>Item Name <span className="text-rose-500">*</span></label>
        <input
          ref={itemNameRef} type="text" className={INPUT} placeholder="Product name…"
          value={state.item_name} onChange={(e) => up("item_name", e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter" && onAdvance) { e.preventDefault(); onAdvance(); } }}
        />
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
        <div className="flex items-start justify-between gap-2">
          <p className="flex items-center gap-2 text-sm font-bold text-orange-700 dark:text-orange-400">
            <CalendarX2 className="h-4 w-4" />Expiry &amp; Batch
          </p>
          <span className="rounded-full border border-orange-300 bg-orange-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-orange-600 dark:border-orange-700/60 dark:bg-orange-950/50 dark:text-orange-400">
            FEFO Tracking
          </span>
        </div>
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

export function WizardStep2({ state, setState, conditions, photoCtx }: {
  state: WizardState; setState: React.Dispatch<React.SetStateAction<WizardState>>;
  conditions: string[];
  photoCtx?: { hasPackageLink: boolean; orphanLpn: boolean };
}) {
  const categories = getCategoriesForConditions(conditions, photoCtx);

  // ── Item Condition & Evidence: upload states ─────────────────────────────
  const [itemPhotoUploading,   setItemPhotoUploading]   = useState(false);
  const [expiryPhotoUploading, setExpiryPhotoUploading] = useState(false);
  const itemPhotoRef   = useRef<HTMLInputElement>(null);
  const expiryPhotoRef = useRef<HTMLInputElement>(null);

  async function uploadEvidencePhoto(
    file: File,
    field: "photo_item_url" | "photo_expiry_url",
    setUploading: (v: boolean) => void,
  ) {
    setUploading(true);
    try {
      const ext  = file.name.split(".").pop() ?? "jpg";
      const path = `evidence/${field}/${Date.now()}.${ext}`;
      const { error: upErr } = await supabaseBrowser.storage
        .from("media")
        .upload(path, file, { upsert: true });
      if (upErr) throw new Error(upErr.message);
      const { data: urlData } = supabaseBrowser.storage.from("media").getPublicUrl(path);
      setState((p) => ({ ...p, [field]: urlData.publicUrl }));
    } catch {
      // silently ignore — operator can retry
    } finally {
      setUploading(false);
    }
  }

  return (
    <div className="space-y-5">
      {/* ── Item Condition & Evidence ──────────────────────────────────────── */}
      <div className="rounded-2xl border-2 border-sky-200 bg-sky-50 p-4 space-y-4 dark:border-sky-700/50 dark:bg-sky-950/20">
        <div className="flex items-center gap-2">
          <Camera className="h-4 w-4 text-sky-600 dark:text-sky-400" />
          <p className="text-sm font-bold text-sky-800 dark:text-sky-200">Item Condition &amp; Evidence</p>
          <span className="ml-auto rounded-full bg-sky-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-sky-600 dark:bg-sky-900/40 dark:text-sky-300">Claim Photos</span>
        </div>

        {/* Item photo */}
        <div>
          <p className="mb-1.5 text-xs font-semibold text-sky-700 dark:text-sky-300">
            Item Photo <span className="font-normal text-slate-400">(optional)</span>
          </p>
          {state.photo_item_url ? (
            <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
              <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
              <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Item photo saved ✓</span>
              <button type="button" onClick={() => setState((p) => ({ ...p, photo_item_url: "" }))} className="text-xs text-slate-400 underline hover:text-slate-600">Remove</button>
            </div>
          ) : (
            <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${itemPhotoUploading ? "border-sky-300 bg-sky-50 text-sky-500" : "border-sky-300 bg-sky-50 text-sky-700 hover:bg-sky-100 dark:border-sky-700/60 dark:bg-sky-950/30 dark:text-sky-300"}`}>
              {itemPhotoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
              {itemPhotoUploading ? "Uploading…" : "📸 Upload Item Photo"}
              <input ref={itemPhotoRef} type="file" accept="image/*" capture="environment" className="hidden" disabled={itemPhotoUploading}
                onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ""; if (f) void uploadEvidencePhoto(f, "photo_item_url", setItemPhotoUploading); }} />
            </label>
          )}
        </div>

        {/* Expiry date photo */}
        <div>
          <p className="mb-1.5 text-xs font-semibold text-sky-700 dark:text-sky-300">
            Expiry Date Photo
            {state.expiration_date && <span className="ml-1.5 text-rose-500 font-bold">— Required (expiry set)</span>}
            {!state.expiration_date && <span className="font-normal text-slate-400">(recommended if expiry label exists)</span>}
          </p>
          {state.photo_expiry_url ? (
            <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
              <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
              <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Expiry photo saved ✓</span>
              <button type="button" onClick={() => setState((p) => ({ ...p, photo_expiry_url: "" }))} className="text-xs text-slate-400 underline hover:text-slate-600">Remove</button>
            </div>
          ) : (
            <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${expiryPhotoUploading ? "border-orange-300 bg-orange-50 text-orange-500" : state.expiration_date ? "border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100 dark:border-rose-700/60 dark:bg-rose-950/30 dark:text-rose-300" : "border-orange-300 bg-orange-50 text-orange-700 hover:bg-orange-100 dark:border-orange-700/60 dark:bg-orange-950/30 dark:text-orange-300"}`}>
              {expiryPhotoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
              {expiryPhotoUploading ? "Uploading…" : "📸 Upload Expiry Date Photo"}
              <input ref={expiryPhotoRef} type="file" accept="image/*" capture="environment" className="hidden" disabled={expiryPhotoUploading}
                onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ""; if (f) void uploadEvidencePhoto(f, "photo_expiry_url", setExpiryPhotoUploading); }} />
            </label>
          )}
        </div>
      </div>

      {/* ── Existing condition-based photo categories ─────────────────────── */}
      {categories.length > 0 && (
        <>
          <p className="text-xs text-muted-foreground">Packing slip / manifest scans belong to <span className="font-semibold">Package</span> setup only — not here.</p>
          {categories.map((cat) => (
            <div key={cat.id} className={`rounded-2xl border p-4 ${cat.accentClass}`}>
              <div className="mb-3 flex items-center gap-2">
                <cat.icon className={`h-5 w-5 ${cat.iconColor}`} />
                <div><p className="text-sm font-bold text-foreground">{cat.label}<span className={`ml-2 text-[10px] font-semibold uppercase tracking-wide ${cat.optional ? "text-slate-400" : "text-rose-500"}`}>{cat.optional ? "Optional" : "Required"}</span></p><p className="text-xs text-slate-400">{cat.hint}</p></div>
              </div>
              <SmartCameraUpload label={cat.label} hint={cat.hint} required={!cat.optional} icon={cat.icon} iconColor={cat.iconColor} accentClass={cat.accentClass}
                files={state.photos[cat.id] ?? []}
                onChange={(files) => setState((p) => ({ ...p, photos: { ...p.photos, [cat.id]: files } }))}
              />
            </div>
          ))}
        </>
      )}
      {categories.length === 0 && (
        <div className="flex flex-col items-center gap-3 py-8 text-center">
          <CheckCircle2 className="h-10 w-10 text-emerald-500" />
          <p className="font-bold text-foreground">No Condition Photos Required</p>
          <p className="text-sm text-slate-400">Use the evidence buttons above if you have a claim photo.</p>
        </div>
      )}
    </div>
  );
}

export function WizardStep3({ state, conditions, packages, pallets, inherited, onNotesChange, packageExpectationMismatch }: {
  state: WizardState; conditions: string[]; packages: PackageRecord[]; pallets: PalletRecord[];
  inherited?: WizardInheritedContext; onNotesChange: (v: string) => void;
  /** Soft warning — does not block submit */
  packageExpectationMismatch?: boolean;
}) {
  const photoTotal = Object.values(state.photos).reduce((a, files) => a + files.length, 0);
  const linkedPkg  = packages.find((p) => p.id === ((inherited?.packageId) ?? state.package_link_id));
  const linkedPlt  = linkedPkg ? pallets.find((p) => p.id === linkedPkg.pallet_id) : undefined;
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
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-5 dark:border-slate-700 dark:bg-slate-900 space-y-3">
        <p className="text-xs font-bold uppercase tracking-widest text-slate-400">Review Summary</p>
        <div className="grid grid-cols-2 gap-y-3 gap-x-6 text-sm">
          <div><p className="text-xs text-slate-400">Product ID</p><p className="font-mono font-bold">{state.product_identifier || "—"}</p></div>
          {state.lpn && <div><p className="text-xs text-slate-400">LPN (optional)</p><p className="font-mono font-bold">{state.lpn}</p></div>}
          <div><p className="text-xs text-slate-400">Marketplace</p><p className="font-bold capitalize">{state.marketplace}</p></div>
          <div className="col-span-2"><p className="text-xs text-slate-400">Item</p><p className="font-bold">{state.item_name}</p></div>
        </div>
        <div><p className="text-xs text-slate-400 mb-1.5">Conditions</p><div className="flex flex-wrap gap-1.5">{conditions.map((c) => <ConditionBadge key={c} value={c} />)}</div></div>
        {(linkedPkg || linkedPlt) && (
          <div className="flex flex-col gap-2">
            <div className="flex flex-wrap gap-2">
              {linkedPkg && <span className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-2.5 py-0.5 text-xs font-semibold text-sky-700 dark:border-sky-700/60 dark:bg-sky-950/40 dark:text-sky-300"><Tag className="h-3 w-3" />{linkedPkg.package_number}</span>}
              {linkedPlt && <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2.5 py-0.5 text-xs font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"><Boxes className="h-3 w-3" />{linkedPlt.pallet_number}</span>}
            </div>
            {linkedPkg?.tracking_number && (
              <p className="text-xs text-muted-foreground"><span className="font-semibold text-foreground">Inherits tracking:</span>{" "}
                <span className="font-mono">{linkedPkg.tracking_number}</span>
                {linkedPkg.carrier_name ? <span> · {linkedPkg.carrier_name}</span> : null}
              </p>
            )}
          </div>
        )}
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Camera className="h-4 w-4" />
          {photoTotal > 0 ? `${photoTotal} photo${photoTotal > 1 ? "s" : ""} attached` : <span className="text-amber-600 dark:text-amber-400">No photos attached</span>}
        </div>
        {photoTotal > 0 && (
          <div className="mt-2">
            <PhotoGallery
              photos={Object.entries(state.photos).flatMap(([cat, files]) =>
                files.map((f, i) => ({
                  src: URL.createObjectURL(f),
                  label: `${ALL_PHOTO_CATEGORIES[cat]?.label ?? cat} ${i + 1}`,
                }))
              )}
            />
          </div>
        )}
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

export function SingleItemWizardModal({ onClose, onSuccess, actor, openPackages, openPallets, onCreatePackage, onCreatePallet, inheritedContext, aiLabelEnabled = false, onSoftPackageWarning }: {
  onClose: () => void;
  /** Called with the saved record AND the in-session photo files for gallery display. */
  onSuccess: (r: ReturnRecord, photos: Record<string, File[]>) => void;
  actor: string; openPackages: PackageRecord[]; openPallets: PalletRecord[];
  onCreatePackage: () => void; onCreatePallet: () => void;
  inheritedContext?: WizardInheritedContext;
  aiLabelEnabled?: boolean;
  /** Non-blocking: called when item may not match package manifest (simulated) — save still proceeds. */
  onSoftPackageWarning?: () => void;
}) {
  const [step, setStep] = useState(1);
  const [state, setState] = useState<WizardState>({ ...EMPTY_WIZARD, package_link_id: inheritedContext?.packageId ?? "" });
  const [submitting, setSubmitting] = useState(false);
  const [submitErr, setSubmitErr] = useState("");
  const [flash, setFlash] = useState(false);

  const conditions = conditionsFromKeys(state.condition_keys);
  const pkgIdForWarn = (inheritedContext?.packageId ?? state.package_link_id) || undefined;
  const pkgForWarn = pkgIdForWarn ? openPackages.find((p) => p.id === pkgIdForWarn) : undefined;
  const packageExpectationMismatch = !!(pkgForWarn && !itemMatchesPackageExpectation(state.item_name, pkgForWarn));

  const step1Valid =
    !!state.marketplace &&
    !!state.item_name.trim() &&
    !!state.product_identifier.trim() &&
    state.condition_keys.length > 0 &&
    (!state.condition_keys.includes("expired") || !!state.expiration_date.trim());

  async function handleSubmit() {
    setSubmitting(true); setSubmitErr("");
    const pkgId = (inheritedContext?.packageId ?? state.package_link_id) || undefined;
    if (pkgId && onSoftPackageWarning) {
      const pkg = openPackages.find((p) => p.id === pkgId);
      if (pkg && !itemMatchesPackageExpectation(state.item_name, pkg)) onSoftPackageWarning();
    }
    const photoEvidence = Object.fromEntries(Object.entries(state.photos).map(([k, v]) => [k, v.length]));
    const res = await insertReturn({
      lpn: state.lpn || undefined,
      product_identifier: state.product_identifier.trim(),
      marketplace: state.marketplace as string, item_name: state.item_name, conditions,
      notes: state.notes, photo_evidence: photoEvidence,
      expiration_date: state.expiration_date || undefined,
      batch_number: state.batch_number || undefined,
      photo_item_url:   state.photo_item_url   || undefined,
      photo_expiry_url: state.photo_expiry_url || undefined,
      package_id: pkgId,
      store_id:   state.store_id || undefined,
      created_by: actor,
    });
    setSubmitting(false);
    if (res.ok && res.data) {
      onSuccess(res.data, state.photos);
      setFlash(true);
      setTimeout(() => { setFlash(false); setStep(1); setState({ ...EMPTY_WIZARD, package_link_id: inheritedContext?.packageId ?? "" }); setSubmitErr(""); }, 700);
    } else setSubmitErr(res.error ?? "Submission failed.");
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
          {step === 1 && <WizardStep1 state={state} setState={setState} openPackages={openPackages} openPallets={openPallets} onCreatePackage={onCreatePackage} onCreatePallet={onCreatePallet} inherited={inheritedContext} aiLabelEnabled={aiLabelEnabled} onAdvance={step1Valid ? () => setStep(2) : undefined} />}
          {step === 2 && (
            <WizardStep2
              state={state}
              setState={setState}
              conditions={conditions}
              photoCtx={{
                hasPackageLink: !!(inheritedContext?.packageId ?? state.package_link_id),
                orphanLpn: !!state.lpn.trim(),
              }}
            />
          )}
          {step === 3 && <WizardStep3 state={state} conditions={conditions} packages={openPackages} pallets={openPallets} inherited={inheritedContext} onNotesChange={(v) => setState((p) => ({ ...p, notes: v }))} packageExpectationMismatch={packageExpectationMismatch} />}
        </div>
        {submitErr && <p className="shrink-0 px-4 sm:px-6 pb-2 text-sm font-semibold text-rose-600 dark:text-rose-400">{submitErr}</p>}
        <div className="flex shrink-0 items-center gap-3 border-t border-slate-200 px-4 py-3 sm:px-6 sm:py-4 dark:border-slate-700">
          {step > 1 && <button onClick={() => setStep((s) => s-1)} className={`${BTN_GHOST} h-14 px-4`}><ArrowLeft className="h-5 w-5" /></button>}
          {step < 3 ? <button onClick={() => setStep((s) => s+1)} disabled={step === 1 && !step1Valid} className={BTN_PRIMARY}>Next <ArrowRight className="h-5 w-5" /></button>
            : <button onClick={handleSubmit} disabled={submitting} className={BTN_PRIMARY}>{submitting ? <Loader2 className="h-5 w-5 animate-spin" /> : <CheckCircle2 className="h-5 w-5" />}{submitting ? "Saving…" : "Submit to Claims"}</button>}
        </div>
      </div>
    </div>
  );
}

// ─── Create Package Modal ──────────────────────────────────────────────────────

export function CreatePackageModal({ onClose, onCreated, actor, openPallets, aiPackingSlipEnabled = false }: {
  onClose: () => void; onCreated: (p: PackageRecord) => void; actor: string; openPallets: PalletRecord[];
  aiPackingSlipEnabled?: boolean;
}) {
  const [pkgNum, setPkgNum] = useState(generatePackageNumber());
  const [tracking, setTracking] = useState(""); const [carrier, setCarrier] = useState(""); const [expected, setExpected] = useState(""); const [palletId, setPalletId] = useState("");
  const [rmaNumber, setRmaNumber] = useState("");
  const [ocrFile, setOcrFile] = useState<File | null>(null); const [ocrLoad, setOcrLoad] = useState(false); const [ocrDone, setOcrDone] = useState(false);
  const [saving, setSaving] = useState(false); const [error, setError] = useState("");
  const [photoClosedUrl, setPhotoClosedUrl] = useState<string | null>(null);
  const [photoClosedUploading, setPhotoClosedUploading] = useState(false);
  const [photoOpenedUrl, setPhotoOpenedUrl] = useState<string | null>(null);
  const [photoOpenedUploading, setPhotoOpenedUploading] = useState(false);
  const [photoReturnLabelUrl, setPhotoReturnLabelUrl] = useState<string | null>(null);
  const [photoReturnLabelUploading, setPhotoReturnLabelUploading] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);
  const palletOptions = openPallets.map((p) => ({ id: p.id, label: p.pallet_number, sublabel: `${p.item_count} items` }));

  // ── Auto-fill carrier from the pallet's existing packages ─────────────────
  useEffect(() => {
    if (!palletId) return;
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
    supabaseBrowser
      .from("stores")
      .select("id, name, platform")
      .eq("is_active", true)
      .order("created_at", { ascending: false })
      .then(({ data }) => { if (data) setPkgStoresList(data as { id: string; name: string; platform: string }[]); })
      .catch(() => {});
  }, []);

  // ── Inherit store_id from pallet (Pallet → Package) ──────────────────────
  useEffect(() => {
    if (!palletId) { setPkgStoreInherited(false); return; }
    const pallet = openPallets.find((p) => p.id === palletId);
    if (pallet?.store_id) {
      setPkgStoreId(pallet.store_id);
      setPkgStoreInherited(true);
    } else {
      setPkgStoreInherited(false);
    }
  }, [palletId, openPallets]);

  // ── Physical hardware scanner — attached only to the tracking # input ───────
  const { onKeyDown: trackingKeyDown } = usePhysicalScanner({
    onScan: (code) => setTracking(code),
  });

  // ── Physical hardware scanner — attached only to the RMA # input ─────────
  const { onKeyDown: rmaKeyDown } = usePhysicalScanner({
    onScan: (code) => setRmaNumber(code),
  });

  async function handleOcr(file: File) { setOcrFile(file); setOcrLoad(true); setOcrDone(false); const res = await mockPackageOcr(file); setOcrLoad(false); if (res.ok && res.data) { setExpected(String(res.data.expected_item_count)); setCarrier(res.data.carrier_name); setTracking(res.data.tracking_number); setOcrDone(true); } else setError(res.error ?? "OCR failed."); }
  async function handleManifestUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    if (f) handleOcr(f);
    e.target.value = "";
  }
  async function handleClaimPhotoCapture(
    e: React.ChangeEvent<HTMLInputElement>,
    field: "closed" | "opened" | "return_label",
  ) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    const setUploading = field === "closed" ? setPhotoClosedUploading
                       : field === "opened" ? setPhotoOpenedUploading
                       : setPhotoReturnLabelUploading;
    const setUrl       = field === "closed" ? setPhotoClosedUrl
                       : field === "opened" ? setPhotoOpenedUrl
                       : setPhotoReturnLabelUrl;
    setUploading(true);
    try {
      const folder =
        field === "closed"       ? "packages/claim_closed"       :
        field === "opened"       ? "packages/claim_opened"       :
                                   "packages/claim_return_label";
      const publicUrl = await uploadToStorage(f, folder);
      setUrl(publicUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Photo upload failed.");
    } finally {
      setUploading(false);
    }
  }

  async function handleCreate() {
    if (!pkgNum.trim()) return; setSaving(true); setError("");
    const res = await createPackage({
      package_number: pkgNum.trim(),
      tracking_number: tracking.trim() || undefined,
      carrier_name: carrier || undefined,
      rma_number: rmaNumber.trim() || undefined,
      expected_item_count: expected ? parseInt(expected, 10) : 0,
      pallet_id: palletId || undefined,
      store_id: pkgStoreId || undefined,
      created_by: actor,
      photo_closed_url:        photoClosedUrl         ?? undefined,
      photo_opened_url:        photoOpenedUrl         ?? undefined,
      photo_return_label_url:  photoReturnLabelUrl    ?? undefined,
    });
    setSaving(false);
    if (res.ok && res.data) onCreated(res.data); else setError(res.error ?? "Failed.");
  }

  return (
    <div className="fixed inset-0 z-[300] flex items-center justify-center bg-black/50 p-2 sm:p-4 backdrop-blur-sm">
      <div className="w-[95vw] max-w-lg overflow-hidden rounded-2xl sm:rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950">
        <div className="flex items-center justify-between border-b border-slate-200 p-4 sm:p-6 dark:border-slate-700">
          <div>
            <div className="flex items-center gap-2">
              <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Batch Flow</p>
              <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"><ScanLine className="h-3 w-3" />Scanner-Ready</span>
            </div>
            <h2 className="mt-0.5 text-xl font-bold text-foreground">Create Package</h2>
          </div>
          <button onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button>
        </div>
        <div className="max-h-[78vh] sm:max-h-[70vh] overflow-y-auto p-4 sm:p-6 space-y-5">
          {aiPackingSlipEnabled && (
            <>
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-300"><Sparkles className="h-3 w-3" />AI Feature</span>
                <span className="text-xs text-slate-400">Packing Slip OCR</span>
              </div>
              {!ocrFile ? <button type="button" onClick={() => fileRef.current?.click()} className="flex w-full flex-col items-center gap-3 rounded-2xl border-2 border-dashed border-violet-300 bg-violet-50 p-5 transition hover:border-violet-400 hover:bg-violet-100 dark:border-violet-700/60 dark:bg-violet-950/30"><FileImage className="h-10 w-10 text-violet-400" /><p className="text-sm font-semibold text-violet-700 dark:text-violet-300">Photo Packing Slip for AI Scan</p><p className="text-xs text-slate-400">Extracts carrier, tracking, expected count</p></button>
                : ocrLoad ? <div className="flex items-center gap-3 rounded-2xl border border-sky-200 bg-sky-50 p-4 dark:border-sky-700/60 dark:bg-sky-950/30"><Loader2 className="h-5 w-5 animate-spin text-sky-500" /><p className="text-sm font-semibold text-sky-700 dark:text-sky-300">Scanning with AI…</p></div>
                : ocrDone ? <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-700/60 dark:bg-emerald-950/30"><Sparkles className="h-5 w-5 text-emerald-500" /><div className="flex-1"><p className="text-sm font-bold text-emerald-700 dark:text-emerald-300">AI Scan Complete</p><p className="text-xs text-slate-400">Fields pre-filled — override if needed.</p></div><button onClick={() => { setOcrFile(null); setOcrDone(false); }} className="text-xs text-slate-400 underline">Rescan</button></div>
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
          <div><label className={LABEL}>Expected Items</label><input type="number" min="0" className={INPUT} placeholder="0" value={expected} onChange={(e) => setExpected(e.target.value)} /></div>
          <ComboboxField label="Link to Pallet" hint="(optional)" icon={Boxes} options={palletOptions} value={palletId} onChange={setPalletId} onClear={() => setPalletId("")} placeholder="Search pallets…" />

          {pkgStoresList.length > 0 && (
            <div>
              <label className={LABEL}>
                Source Store
                {pkgStoreInherited && (
                  <span className="ml-2 text-xs font-normal text-emerald-600 dark:text-emerald-400">
                    · Inherited from Pallet
                  </span>
                )}
              </label>
              <div className="flex gap-2">
                <select
                  className={`${INPUT} flex-1`}
                  value={pkgStoreId}
                  onChange={(e) => { setPkgStoreId(e.target.value); setPkgStoreInherited(false); }}
                  disabled={pkgStoreInherited}
                >
                  <option value="">— Select store (optional) —</option>
                  {pkgStoresList.map((s) => (
                    <option key={s.id} value={s.id}>{s.name} ({s.platform})</option>
                  ))}
                </select>
                {pkgStoreInherited && (
                  <button
                    type="button"
                    onClick={() => { setPkgStoreId(""); setPkgStoreInherited(false); }}
                    className="shrink-0 rounded-xl border border-slate-200 px-3 text-xs font-medium text-muted-foreground hover:bg-accent dark:border-slate-700"
                  >
                    Override
                  </button>
                )}
              </div>
              {pkgStoreInherited && (
                <p className="mt-1 text-[11px] text-emerald-600 dark:text-emerald-400">
                  🔒 Auto-filled from pallet. Click Override to select a different store.
                </p>
              )}
            </div>
          )}

          {/* ── Claim Evidence Photos ──────────────────────────────────────── */}
          <div className="rounded-2xl border-2 border-rose-200 bg-rose-50 p-4 space-y-3 dark:border-rose-700/50 dark:bg-rose-950/20">
            <div className="flex items-center gap-2">
              <Camera className="h-4 w-4 text-rose-600 dark:text-rose-400" />
              <p className="text-sm font-bold text-rose-800 dark:text-rose-200">Claim Evidence Photos</p>
              <span className="ml-auto rounded-full bg-rose-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-rose-600 dark:bg-rose-900/40 dark:text-rose-300">Box Level</span>
            </div>

            {/* Closed box */}
            {photoClosedUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Closed box photo saved ✓</span>
                <button type="button" onClick={() => setPhotoClosedUrl(null)} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${photoClosedUploading ? "border-rose-300 bg-rose-50/80 text-rose-500" : "border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100 dark:border-rose-700/60 dark:bg-rose-950/30 dark:text-rose-300"}`}>
                {photoClosedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {photoClosedUploading ? "Uploading…" : "📸 Upload Closed Box Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={photoClosedUploading}
                  onChange={(e) => handleClaimPhotoCapture(e, "closed")} />
              </label>
            )}

            {/* Opened box */}
            {photoOpenedUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Opened box photo saved ✓</span>
                <button type="button" onClick={() => setPhotoOpenedUrl(null)} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${photoOpenedUploading ? "border-amber-300 bg-amber-50/80 text-amber-500" : "border-amber-300 bg-amber-50 text-amber-700 hover:bg-amber-100 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300"}`}>
                {photoOpenedUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {photoOpenedUploading ? "Uploading…" : "📸 Upload Opened Box Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={photoOpenedUploading}
                  onChange={(e) => handleClaimPhotoCapture(e, "opened")} />
              </label>
            )}

            {/* Return shipping label */}
            {photoReturnLabelUrl ? (
              <div className="flex items-center gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <span className="flex-1 truncate text-xs font-semibold text-emerald-700 dark:text-emerald-300">Return label photo saved ✓</span>
                <button type="button" onClick={() => setPhotoReturnLabelUrl(null)} className="text-xs text-slate-400 underline">Remove</button>
              </div>
            ) : (
              <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border-2 border-dashed py-3 text-sm font-semibold transition ${photoReturnLabelUploading ? "border-violet-300 bg-violet-50/80 text-violet-500" : "border-violet-300 bg-violet-50 text-violet-700 hover:bg-violet-100 dark:border-violet-700/60 dark:bg-violet-950/30 dark:text-violet-300"}`}>
                {photoReturnLabelUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
                {photoReturnLabelUploading ? "Uploading…" : "📸 Upload Return Label Photo"}
                <input type="file" accept="image/*" capture="environment" className="hidden" disabled={photoReturnLabelUploading}
                  onChange={(e) => handleClaimPhotoCapture(e, "return_label")} />
              </label>
            )}
          </div>

          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="border-t border-slate-200 p-4 dark:border-slate-700 flex flex-col gap-3">
          <label className="flex w-full cursor-pointer items-center justify-center gap-2 rounded-2xl border-2 border-dashed border-slate-300 bg-slate-50 py-3 text-sm font-semibold text-slate-600 transition hover:border-slate-400 hover:bg-slate-100 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-300">
            📸 Scan Packing Slip / Manifest
            <input type="file" accept="image/*" capture="environment" className="hidden" onChange={handleManifestUpload} />
          </label>
          <button onClick={handleCreate} disabled={saving || !pkgNum.trim()} className={BTN_PRIMARY}>{saving ? <Loader2 className="h-5 w-5 animate-spin" /> : <Plus className="h-5 w-5" />}{saving ? "Creating…" : "Create Package"}</button>
        </div>
      </div>
    </div>
  );
}

// ─── Create Pallet Modal ───────────────────────────────────────────────────────

export function CreatePalletModal({ onClose, onCreated, actor, aiManifestEnabled = false }: {
  onClose: () => void; onCreated: (p: PalletRecord) => void; actor: string;
  aiManifestEnabled?: boolean;
}) {
  const [palletNum, setPalletNum] = useState(generatePalletNumber()); const [notes, setNotes] = useState(""); const [file, setFile] = useState<File | null>(null);
  const [bolFile, setBolFile] = useState<File | null>(null);
  const [ocrLoad, setOcrLoad] = useState(false); const [ocrResult, setOcrResult] = useState<{ pallet_number: string; total_items: number; confidence: number } | null>(null);
  const [saving, setSaving] = useState(false); const [error, setError] = useState("");
  const [photoUrl, setPhotoUrl] = useState<string | null>(null);
  const [photoUploading, setPhotoUploading] = useState(false);
  const photoRef = useRef<HTMLInputElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const bolRef = useRef<HTMLInputElement>(null);

  // ── Store state (Pallet form — top of hierarchy) ──────────────────────────
  const [palletStoreId,  setPalletStoreId]  = useState("");
  const [palletStoresList, setPalletStoresList] = useState<{ id: string; name: string; platform: string }[]>([]);
  useEffect(() => {
    supabaseBrowser
      .from("stores")
      .select("id, name, platform")
      .eq("is_active", true)
      .order("created_at", { ascending: false })
      .then(({ data }) => { if (data) setPalletStoresList(data as { id: string; name: string; platform: string }[]); })
      .catch(() => {});
  }, []);

  async function handleOcr(f: File) { setFile(f); setOcrLoad(true); const res = await mockPalletOcr(f); setOcrLoad(false); if (res.ok && res.data) { setOcrResult(res.data); setPalletNum(res.data.pallet_number); } else setError(res.error ?? "OCR failed."); }
  async function handlePhotoCapture(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    setPhotoUploading(true);
    try {
      const ext = f.name.split(".").pop() ?? "jpg";
      const path = `pallets/${Date.now()}.${ext}`;
      const { error: upErr } = await supabaseBrowser.storage.from("media").upload(path, f, { upsert: true });
      if (upErr) throw new Error(upErr.message);
      const { data: urlData } = supabaseBrowser.storage.from("media").getPublicUrl(path);
      setPhotoUrl(urlData.publicUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Photo upload failed.");
    } finally {
      setPhotoUploading(false);
    }
  }
  async function handleCreate() {
    if (!palletNum.trim()) return; setSaving(true); setError("");
    const manifest_photo_url = file ? URL.createObjectURL(file) : undefined;
    const bol_photo_url = bolFile ? URL.createObjectURL(bolFile) : undefined;
    const res = await createPallet({ pallet_number: palletNum.trim(), manifest_photo_url, bol_photo_url, photo_url: photoUrl ?? undefined, store_id: palletStoreId || undefined, notes, created_by: actor });
    setSaving(false);
    if (res.ok && res.data) onCreated(res.data); else setError(res.error ?? "Failed.");
  }

  return (
    <div className="fixed inset-0 z-[300] flex items-center justify-center bg-black/50 p-2 sm:p-4 backdrop-blur-sm">
      <div className="w-[95vw] max-w-lg overflow-hidden rounded-2xl sm:rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950">
        <div className="flex items-center justify-between border-b border-slate-200 p-4 sm:p-6 dark:border-slate-700"><div><p className="text-xs font-semibold uppercase tracking-widest text-slate-400">Pallet Flow</p><h2 className="mt-0.5 text-xl font-bold text-foreground">Create Pallet</h2></div><button onClick={onClose} className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground"><X className="h-5 w-5" /></button></div>
        <div className="p-4 sm:p-6 space-y-4">
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
          {palletStoresList.length > 0 && (
            <div>
              <label className={LABEL}>Source Store</label>
              <select
                className={INPUT}
                value={palletStoreId}
                onChange={(e) => setPalletStoreId(e.target.value)}
              >
                <option value="">— Select store (optional) —</option>
                {palletStoresList.map((s) => (
                  <option key={s.id} value={s.id}>{s.name} ({s.platform})</option>
                ))}
              </select>
              <p className="mt-1 text-[11px] text-muted-foreground">
                Packages created inside this pallet will inherit this store automatically.
              </p>
            </div>
          )}
          <div><label className={LABEL}>Notes (optional)</label><textarea value={notes} onChange={(e) => setNotes(e.target.value)} rows={2} className="w-full resize-none rounded-2xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" /></div>
          {/* General pallet photo */}
          <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-2xl border-2 border-dashed py-3 text-sm font-semibold transition ${photoUrl ? "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-700/60 dark:bg-emerald-950/30 dark:text-emerald-300" : "border-amber-300 bg-amber-50 text-amber-700 hover:border-amber-400 hover:bg-amber-100 dark:border-amber-700/60 dark:bg-amber-950/30 dark:text-amber-300"}`}>
            {photoUploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Camera className="h-4 w-4" />}
            {photoUploading ? "Uploading…" : photoUrl ? "🪣 Pallet Photo Saved ✓" : "📸 Take Photo of Pallet"}
            <input ref={photoRef} type="file" accept="image/*" capture="environment" className="hidden" onChange={handlePhotoCapture} disabled={photoUploading} />
          </label>
          {photoUrl && <p className="truncate text-center text-[10px] text-muted-foreground">{photoUrl}</p>}
          {error && <p className="rounded-xl bg-rose-50 px-4 py-2 text-sm text-rose-700 dark:bg-rose-950/40 dark:text-rose-400">{error}</p>}
        </div>
        <div className="border-t border-slate-200 p-4 dark:border-slate-700"><button onClick={handleCreate} disabled={saving || !palletNum.trim()} className={BTN_PRIMARY}>{saving ? <Loader2 className="h-5 w-5 animate-spin" /> : <Plus className="h-5 w-5" />}{saving ? "Creating…" : "Create Pallet"}</button></div>
      </div>
    </div>
  );
}

// ─── Items Data Table ──────────────────────────────────────────────────────────

export function ItemsDataTable({ items, packages, pallets, role, actor, onRowClick, onRowEdit, onBulkDeleted, onBulkMoved, onNewItem, externalSearch = "" }: {
  items: ReturnRecord[]; packages: PackageRecord[]; pallets: PalletRecord[];
  role: UserRole; actor: string;
  onRowClick: (r: ReturnRecord) => void; onRowEdit: (r: ReturnRecord) => void;
  onBulkDeleted: (ids: string[]) => void;
  onBulkMoved: (updated: ReturnRecord[]) => void;
  onNewItem: () => void;
  /** Merged with local search — set from TopHeader global search on Returns. */
  externalSearch?: string;
}) {
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
          r.product_identifier ?? "", r.inherited_tracking_number ?? "",
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
        <select value={marketF} onChange={(e) => { setMarketF(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-auto`} title="Filter by source / marketplace"><option value="">All sources</option>{MARKETPLACES.map((m) => <option key={m} value={m}>{MP_LABELS[m]}</option>)}</select>
        <div className="flex items-center gap-1.5">
          <Calendar className="h-4 w-4 shrink-0 text-slate-400" />
          <input type="date" value={dateFrom} onChange={(e) => { setDateFrom(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} title="From date" />
          <span className="text-xs text-slate-400">–</span>
          <input type="date" value={dateTo} onChange={(e) => { setDateTo(e.target.value); setPage(1); }} className={`${INPUT_SM_DARK} w-36`} title="To date" />
        </div>
        {(search || statusF || marketF || dateFrom || dateTo) && <button onClick={() => { setSearch(""); setStatusF(""); setMarketF(""); setDateFrom(""); setDateTo(""); setPage(1); }} className="flex h-10 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400"><X className="h-3.5 w-3.5" />Clear</button>}
        <button onClick={onNewItem} className="ml-auto flex h-10 items-center gap-2 rounded-xl bg-sky-500 px-4 text-sm font-semibold text-white hover:bg-sky-600"><Plus className="h-4 w-4" />Scan Item</button>
      </div>

      <div className="w-full overflow-hidden rounded-2xl border border-border">
        <div className="w-full min-w-0 overflow-x-auto">
          <table className="w-full min-w-[1040px] text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-700 dark:bg-slate-900">
                <th className={TH_CHK} onClick={(e) => e.stopPropagation()}>
                  <div className={CHK_FLEX}>
                    <input type="checkbox" checked={allSelected} onChange={(e) => setSelectedIds(e.target.checked ? new Set(filtered.map((r) => r.id)) : new Set())} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                  </div>
                </th>
                <th className="px-4 py-3 text-left"><SortButton field="product_identifier" label="Product ID" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left md:table-cell"><SortButton field="tracking_effective" label="Tracking" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left sm:table-cell"><SortButton field="lpn" label="LPN" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left sm:table-cell"><SortButton field="marketplace" label="Source" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left"><SortButton field="item_name" label="Item" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="hidden px-4 py-3 text-left lg:table-cell"><SortButton field="item_conditions" label="Conditions" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
                <th className="px-4 py-3 text-left"><SortButton field="status" label="Status" sortField={sortField} sortAsc={sortAsc} onSort={handleSort} /></th>
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
                return (
                  <tr key={r.id} onClick={() => onRowClick(r)} className="cursor-pointer transition hover:bg-sky-50/50 dark:hover:bg-sky-950/20">
                    <td className={TD_CHK} onClick={(e) => e.stopPropagation()}>
                      <div className={CHK_FLEX}>
                        <input type="checkbox" checked={selectedIds.has(r.id)} onChange={(e) => { const s = new Set(selectedIds); e.target.checked ? s.add(r.id) : s.delete(r.id); setSelectedIds(s); }} className="h-4 w-4 cursor-pointer rounded border-slate-300 text-sky-500 focus:ring-sky-400" />
                      </div>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs font-semibold text-slate-700 dark:text-slate-300">{r.product_identifier ?? "—"}</td>
                    <td className="hidden px-4 py-3 font-mono text-[11px] text-muted-foreground md:table-cell">{track || "—"}</td>
                    <td className="hidden px-4 py-3 font-mono text-xs text-muted-foreground sm:table-cell">{r.lpn ?? "—"}</td>
                    <td className="hidden px-4 py-3 text-xs font-medium text-slate-600 dark:text-slate-300 sm:table-cell">{formatMarketplaceSource(r.marketplace)}</td>
                    <td className="min-w-0 max-w-none truncate px-4 py-3 text-slate-600 dark:text-slate-300">{r.item_name}</td>
                    <td className="hidden px-4 py-3 lg:table-cell"><div className="flex flex-wrap gap-1">{r.conditions.slice(0,2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                    <td className="px-4 py-3"><StatusBadge status={r.status} /></td>
                    <td className="hidden px-4 py-3 lg:table-cell">
                      {linkedPkg
                        ? <span className="inline-flex items-center gap-1 rounded-full bg-sky-100 px-2 py-0.5 font-mono text-[10px] font-bold text-sky-700 dark:bg-sky-900/40 dark:text-sky-300">📦 {linkedPkg.package_number}{linkedPlt ? ` › ${linkedPlt.pallet_number}` : ""}</span>
                        : <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">⚠ Orphaned / Loose</span>}
                    </td>
                    <td className="hidden px-4 py-3 xl:table-cell text-xs capitalize text-slate-400">{r.created_by ?? "—"}</td>
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
        {rows.length === 0 && <p className="py-10 text-center text-sm text-slate-400">No records match your filters.</p>}
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

export function PackagesDataTable({ packages, returns: allReturns = [], pallets = [], role, actor, onRowClick, onRowEdit, onBulkDeleted, onBulkPackagesUpdated, onNewPackage, externalSearch = "" }: {
  packages: PackageRecord[]; returns?: ReturnRecord[]; pallets?: PalletRecord[];
  role: UserRole; actor: string;
  onRowClick: (p: PackageRecord) => void; onRowEdit: (p: PackageRecord) => void;
  onBulkDeleted: (ids: string[]) => void;
  /** Called after bulk assign to pallet so parent state stays in sync with DB */
  onBulkPackagesUpdated?: (updated: PackageRecord[]) => void;
  onNewPackage: () => void;
  externalSearch?: string;
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
                    <tr onClick={() => onRowClick(p)} className="cursor-pointer transition hover:bg-violet-50/50 dark:hover:bg-violet-950/20">
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
                      <td className="px-4 py-3 font-mono text-xs font-bold text-foreground">{p.package_number}</td>
                      <td className="hidden px-4 py-3 sm:table-cell"><div className="flex flex-col gap-0.5">{p.carrier_name && <span className="flex items-center gap-1 text-xs text-slate-600 dark:text-slate-300"><Truck className="h-3 w-3" />{p.carrier_name}</span>}{p.tracking_number && <span className="font-mono text-[10px] text-slate-400">{p.tracking_number}</span>}</div></td>
                      <td className="px-4 py-3"><div className="flex items-center gap-2"><span className="text-sm font-bold text-slate-700 dark:text-slate-300">{p.actual_item_count}/{p.expected_item_count > 0 ? p.expected_item_count : "?"}</span>{pct !== null && <div className="hidden h-1.5 w-12 overflow-hidden rounded-full bg-muted sm:block"><div className={`h-full rounded-full ${pct >= 100 ? "bg-emerald-500" : "bg-sky-500"}`} style={{ width: `${pct}%` }} /></div>}</div></td>
                      <td className="px-4 py-3"><PkgStatusBadge status={p.status} /></td>
                      <td className="hidden px-4 py-3 text-xs capitalize text-slate-400 md:table-cell">{p.created_by ?? "—"}</td>
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
                        <td colSpan={9} className="px-6 py-3">
                          {pkgItems.length === 0
                            ? <p className="py-2 text-center text-xs text-slate-400">No items scanned for this package yet.</p>
                            : (
                              <div className="overflow-hidden rounded-xl border border-violet-200 dark:border-violet-800/50">
                                <table className="w-full text-xs">
                                  <thead><tr className="border-b border-violet-200 bg-violet-100/60 dark:border-violet-800/50 dark:bg-violet-950/40">
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Item</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Source</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Condition</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Status</th>
                                    <th className="px-3 py-2 text-left font-bold uppercase tracking-wide text-violet-500">Operator</th>
                                  </tr></thead>
                                  <tbody className="divide-y divide-violet-100 dark:divide-violet-900/40">
                                    {pkgItems.map((r) => (
                                      <tr key={r.id} className="hover:bg-violet-50 dark:hover:bg-violet-950/20">
                                        <td className="px-3 py-2 text-slate-600 dark:text-slate-300">{r.item_name}</td>
                                        <td className="px-3 py-2 text-slate-500">{formatMarketplaceSource(r.marketplace)}</td>
                                        <td className="px-3 py-2"><div className="flex flex-wrap gap-1">{r.conditions.slice(0,2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                                        <td className="px-3 py-2"><StatusBadge status={r.status} /></td>
                                        <td className="px-3 py-2 capitalize text-slate-400">{r.created_by ?? "—"}</td>
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
        {rows.length === 0 && <p className="py-10 text-center text-sm text-slate-400">No packages match your filters.</p>}
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

export function PalletsDataTable({ pallets, packages: allPackages = [], returns: allReturns = [], role, actor, onRowClick, onRowEdit, onBulkDeleted, onNewPallet, externalSearch = "" }: {
  pallets: PalletRecord[]; packages?: PackageRecord[]; returns?: ReturnRecord[]; role: UserRole; actor: string;
  onRowClick: (p: PalletRecord) => void; onRowEdit: (p: PalletRecord) => void;
  onBulkDeleted: (ids: string[]) => void; onNewPallet: () => void;
  externalSearch?: string;
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
                    <tr onClick={() => onRowClick(p)} className="cursor-pointer transition hover:bg-accent hover:text-accent-foreground/50">
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
                      <td className="px-4 py-3 font-mono text-xs font-bold text-foreground">{p.pallet_number}</td>
                      <td className="px-4 py-3"><span className="font-bold text-slate-700 dark:text-slate-300">{p._rollupPkgs}</span><span className="mx-1 text-slate-300 dark:text-slate-600">pkgs</span><span className="font-bold text-slate-500">{p._rollupItems}</span><span className="ml-1 text-slate-300 dark:text-slate-600">items</span></td>
                      <td className="px-4 py-3"><PalletStatusBadge status={p.status} /></td>
                      <td className="hidden px-4 py-3 text-xs capitalize text-slate-400 md:table-cell">{p.created_by}</td>
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
                        <td colSpan={8} className="px-6 py-3">
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
                                          <tr className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                                            <td className={TD_EXP} onClick={(e) => toggleNestedPkgExpand(pk.id, e)}>
                                              <button type="button" className="flex h-6 w-6 items-center justify-center rounded-lg text-slate-400 hover:bg-slate-200 hover:text-slate-600 dark:hover:bg-slate-700 dark:hover:text-slate-300" title={nestedOpen ? "Collapse items" : `Show ${pkgItems.length} item(s)`}>
                                                {nestedOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                                              </button>
                                            </td>
                                            <td className="px-3 py-2 font-mono font-semibold text-slate-700 dark:text-slate-300">{pk.package_number}</td>
                                            <td className="px-3 py-2 text-slate-500">{pk.carrier_name ?? "—"}</td>
                                            <td className="px-3 py-2 font-mono text-slate-400">{pk.tracking_number ?? "—"}</td>
                                            <td className="px-3 py-2">
                                              <div className="flex items-center gap-2">
                                                <span className="font-bold text-slate-600 dark:text-slate-300">{pkItemCount}/{pk.expected_item_count > 0 ? pk.expected_item_count : "?"}</span>
                                                {pct !== null && <div className="hidden h-1.5 w-10 overflow-hidden rounded-full bg-muted sm:block"><div className={`h-full rounded-full ${pct >= 100 ? "bg-emerald-500" : "bg-sky-500"}`} style={{ width: `${pct}%` }} /></div>}
                                              </div>
                                            </td>
                                            <td className="px-3 py-2"><PkgStatusBadge status={pk.status} /></td>
                                            <td className="px-3 py-2 capitalize text-slate-400">{pk.created_by ?? "—"}</td>
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
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Source</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Condition</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Status</th>
                                                          <th className="px-2 py-1.5 text-left font-bold uppercase tracking-wide text-slate-500">Operator</th>
                                                        </tr></thead>
                                                        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                                          {pkgItems.map((r) => (
                                                            <tr key={r.id} className="hover:bg-slate-50 dark:hover:bg-slate-950/30">
                                                              <td className="px-2 py-1.5 text-slate-600 dark:text-slate-300">{r.item_name}</td>
                                                              <td className="px-2 py-1.5 text-slate-500">{formatMarketplaceSource(r.marketplace)}</td>
                                                              <td className="px-2 py-1.5"><div className="flex flex-wrap gap-1">{r.conditions.slice(0, 2).map((c) => <ConditionBadge key={c} value={c} />)}</div></td>
                                                              <td className="px-2 py-1.5"><StatusBadge status={r.status} /></td>
                                                              <td className="px-2 py-1.5 capitalize text-slate-400">{r.created_by ?? "—"}</td>
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
        {rows.length === 0 && <p className="py-10 text-center text-sm text-slate-400">No pallets match your filters.</p>}
      </div>
      {total > 1 && <div className="flex items-center justify-between text-sm text-slate-500"><p>Page {page} of {total}</p><div className="flex gap-2"><button disabled={page<=1} onClick={() => setPage((p)=>p-1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">← Prev</button><button disabled={page>=total} onClick={() => setPage((p)=>p+1)} className="flex h-9 items-center gap-1 rounded-xl border border-slate-200 bg-white px-3 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800">Next →</button></div></div>}
    </div>
  );
}
