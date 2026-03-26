"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Boxes, Package2, ScanLine } from "lucide-react";
import { useGlobalSearch } from "../../components/GlobalSearchContext";
import { useUserRole } from "../../components/UserRoleContext";
import {
  type PackageRecord, type PalletRecord, type ReturnRecord,
  type OrgSettings,
  listReturns, listPackages, listPallets, getOrgSettings,
} from "./actions";
import { getFefoSettings } from "../settings/workspace-settings-actions";
import {
  DEFAULT_FEFO,
  type InventoryModuleConfig,
} from "../settings/workspace-settings-types";
import {
  DEFAULT_ORG_SETTINGS,
  type DrawerContent, type WizardInheritedContext,
  type ToastKind,
  ToastStack, RightDrawer,
  ItemDrawerContent, PackageDrawerContent, PalletDrawerContent,
  ItemsDataTable, PackagesDataTable, PalletsDataTable,
  SingleItemWizardModal, CreatePackageModal, CreatePalletModal,
  useToast,
} from "./_components";

// ─── Root Page ─────────────────────────────────────────────────────────────────

type ActiveTab = "items" | "packages" | "pallets";

export default function ReturnsPage() {
  // ── Data State ──────────────────────────────────────────────────────────────
  const [returns,      setReturns]      = useState<ReturnRecord[]>([]);
  const [packages,     setPackages]     = useState<PackageRecord[]>([]);
  const [pallets,      setPallets]      = useState<PalletRecord[]>([]);
  const [loading,      setLoading]      = useState(true);
  const [fetchErrors,  setFetchErrors]  = useState<string[]>([]);
  const [orgSettings,  setOrgSettings]  = useState<OrgSettings>(DEFAULT_ORG_SETTINGS);
  const [fefoSettings, setFefoSettings] = useState<InventoryModuleConfig>(DEFAULT_FEFO);
  /** In-session File objects keyed by returnId — enables live photo gallery in the drawer. */
  const [sessionPhotos, setSessionPhotos] = useState<Map<string, Record<string, File[]>>>(new Map());

  // ── UI State ────────────────────────────────────────────────────────────────
  const [activeTab, setActiveTab] = useState<ActiveTab>("items");
  const { role, actorName: actor } = useUserRole();

  // ── Drawer Stack ─────────────────────────────────────────────────────────────
  // Stack allows drilling down: Pallet → Package → Item and going back.
  const [drawerStack, setDrawerStack] = useState<DrawerContent[]>([]);
  const activeDrawer = drawerStack[drawerStack.length - 1] ?? null;
  const canGoBack    = drawerStack.length > 1;

  function openDrawer(c: DrawerContent) { setDrawerStack([c]); }
  function pushDrawer(c: DrawerContent) { setDrawerStack((p) => [...p, c]); }
  function popDrawer()                  { setDrawerStack((p) => p.slice(0, -1)); }
  function closeDrawer()                { setDrawerStack([]); }

  // ── Modal State ──────────────────────────────────────────────────────────────
  const [wizardOpen,         setWizardOpen]         = useState(false);
  const [wizardInherited,    setWizardInherited]    = useState<WizardInheritedContext | undefined>(undefined);
  const [createPackageOpen,  setCreatePackageOpen]  = useState(false);
  const [createPalletOpen,   setCreatePalletOpen]   = useState(false);

  const { toasts, show: showToast } = useToast();
  const { query: globalSearchQuery } = useGlobalSearch();

  // ── Data Loading ─────────────────────────────────────────────────────────────
  useEffect(() => {
    async function load() {
      setLoading(true);
      setFetchErrors([]);
      const [r, p, pl, settings, fefo] = await Promise.all([
        listReturns(), listPackages(), listPallets(), getOrgSettings(), getFefoSettings(),
      ]);
      const errs: string[] = [];
      if (r.ok)  setReturns(r.data   ?? []);
      else       errs.push(`Items: ${r.error ?? "unknown error"}`);
      if (p.ok)  setPackages(p.data  ?? []);
      else       errs.push(`Packages: ${p.error ?? "unknown error"}`);
      if (pl.ok) setPallets(pl.data  ?? []);
      else       errs.push(`Pallets: ${pl.error ?? "unknown error"}`);
      if (errs.length) setFetchErrors(errs);
      setOrgSettings(settings);
      setFefoSettings(fefo);
      setLoading(false);
    }
    load();
  }, []);

  // ── Derived helpers ──────────────────────────────────────────────────────────
  const openPackages = useMemo(() => packages.filter((p) => p.status === "open"), [packages]);
  const openPallets  = useMemo(() => pallets.filter((p)  => p.status === "open"), [pallets]);

  const visibleReturns = returns;

  // ── Mutations ────────────────────────────────────────────────────────────────
  function addReturn(r: ReturnRecord, photos?: Record<string, File[]>) {
    setReturns((p) => [r, ...p]);
    if (photos && Object.keys(photos).length > 0) {
      setSessionPhotos((m) => new Map(m).set(r.id, photos));
    }
    setPackages((prev) => prev.map((pkg) => pkg.id === r.package_id ? { ...pkg, actual_item_count: pkg.actual_item_count + 1 } : pkg));
  }
  function updateReturn_(r: ReturnRecord) { setReturns((p) => p.map((x) => x.id === r.id ? r : x)); }
  function removeReturn(id: string) {
    const removed = returns.find((r) => r.id === id);
    setReturns((p) => p.filter((x) => x.id !== id));
    if (removed?.package_id) setPackages((prev) => prev.map((pkg) => pkg.id === removed.package_id ? { ...pkg, actual_item_count: Math.max(0, pkg.actual_item_count - 1) } : pkg));
  }
  function bulkRemoveReturns(ids: string[]) { const set = new Set(ids); setReturns((p) => p.filter((r) => !set.has(r.id))); }
  function bulkUpdateReturns(updated: ReturnRecord[]) { const map = new Map(updated.map((r) => [r.id, r])); setReturns((p) => p.map((r) => map.get(r.id) ?? r)); }

  function addPackage(p: PackageRecord)  { setPackages((prev) => [p, ...prev]); }
  function updatePackage_(p: PackageRecord) {
    setPackages((prev) => prev.map((x) => x.id === p.id ? p : x));
    setReturns((prev) => prev.map((r) => (r.package_id === p.id ? { ...r, pallet_id: p.pallet_id } : r)));
  }
  function removePackage(id: string)     { setPackages((p) => p.filter((x) => x.id !== id)); }
  function bulkRemovePackages(ids: string[]) { const s = new Set(ids); setPackages((p) => p.filter((x) => !s.has(x.id))); }
  /** After bulk assign packages → pallet: merge package rows and sync items' denormalized pallet_id */
  function bulkUpdatePackages(updated: PackageRecord[]) {
    const map = new Map(updated.map((p) => [p.id, p]));
    setPackages((prev) => prev.map((p) => map.get(p.id) ?? p));
    const touched = new Set(updated.map((p) => p.id));
    setReturns((prev) => prev.map((r) => {
      if (!r.package_id || !touched.has(r.package_id)) return r;
      const pkg = map.get(r.package_id);
      if (!pkg) return r;
      return { ...r, pallet_id: pkg.pallet_id };
    }));
  }

  function addPallet(p: PalletRecord)    { setPallets((prev) => [p, ...prev]); }
  function updatePallet_(p: PalletRecord) { setPallets((prev) => prev.map((x) => x.id === p.id ? p : x)); }
  function removePallet(id: string)      { setPallets((p) => p.filter((x) => x.id !== id)); }
  function bulkRemovePallets(ids: string[]) { const s = new Set(ids); setPallets((p) => p.filter((x) => !s.has(x.id))); }

  // ── Open wizard with optional inherited context ───────────────────────────────
  function openWizard(ctx?: WizardInheritedContext) {
    setWizardInherited(ctx);
    setWizardOpen(true);
  }

  // ── Derived drawer title ─────────────────────────────────────────────────────
  function drawerTitle() {
    if (!activeDrawer) return "";
    if (activeDrawer.type === "item")    return activeDrawer.record.item_name;
    if (activeDrawer.type === "package") return activeDrawer.record.package_number;
    if (activeDrawer.type === "pallet")  return activeDrawer.record.pallet_number;
    return "";
  }
  function drawerSubtitle() {
    if (!activeDrawer) return "";
    if (activeDrawer.type === "item")    return "Return Item";
    if (activeDrawer.type === "package") return "Package";
    if (activeDrawer.type === "pallet")  return "Pallet";
    return "";
  }

  // ── Tab config ───────────────────────────────────────────────────────────────
  const tabs: { id: ActiveTab; label: string; icon: React.ElementType; count: number; accent: string }[] = [
    { id: "items",    label: "Items",    icon: ScanLine,  count: returns.length,  accent: "text-sky-600 border-sky-500 dark:text-sky-400 dark:border-sky-400" },
    { id: "packages", label: "Packages", icon: Package2,  count: packages.length, accent: "text-violet-600 border-violet-500 dark:text-violet-400 dark:border-violet-400" },
    { id: "pallets",  label: "Pallets",  icon: Boxes,     count: pallets.length,  accent: "text-slate-700 border-slate-600 dark:text-slate-300 dark:border-slate-400" },
  ];

  return (
    <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col">
      {/* Top Bar */}
      {/* Page title row — global TopHeader (theme/profile) is rendered by AppShell above */}
      <header className="sticky top-0 z-[100] flex items-center gap-3 border-b border-border bg-card/90 px-4 py-3 backdrop-blur-sm">
        <div className="flex-1">
          <h1 className="font-bold text-foreground">Returns & Logistics</h1>
          <p className="text-xs text-slate-400">FBA Reimbursement ERP · role toggle in top-bar</p>
        </div>
      </header>

      {/* Tab Bar */}
      <div className="sticky top-[57px] z-[90] border-b border-border bg-card">
        <nav className="flex gap-0 overflow-x-auto px-4" role="tablist">
          {tabs.map((t) => {
            const Icon = t.icon; const active = activeTab === t.id;
            return (
              <button key={t.id} role="tab" aria-selected={active} onClick={() => setActiveTab(t.id)}
                className={`flex items-center gap-2 border-b-2 px-5 py-4 text-sm font-semibold transition whitespace-nowrap ${active ? t.accent : "border-transparent text-slate-400 hover:text-slate-600 dark:hover:text-slate-300"}`}>
                <Icon className="h-4 w-4" />
                {t.label}
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${active ? "bg-sky-100 text-sky-700 dark:bg-sky-900/60 dark:text-sky-300" : "bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400"}`}>{t.count}</span>
              </button>
            );
          })}
        </nav>
      </div>

      {/* Fetch error banner — shown when any list query fails */}
      {fetchErrors.length > 0 && (
        <div className="border-b border-red-200 bg-red-50 px-4 py-3 dark:border-red-700/50 dark:bg-red-950/30">
          <p className="mb-1 text-sm font-semibold text-red-700 dark:text-red-400">Data failed to load — check your browser console and server logs for details.</p>
          <ul className="list-inside list-disc space-y-0.5">
            {fetchErrors.map((e, i) => (
              <li key={i} className="text-xs text-red-600 dark:text-red-300">{e}</li>
            ))}
          </ul>
          <p className="mt-1.5 text-xs text-red-500 dark:text-red-400">
            Common cause: a database migration has not been applied yet. Run the pending SQL migration files in Supabase → SQL Editor.
          </p>
        </div>
      )}

      {/* Main Content */}
      <main className="flex-1 p-4 sm:p-6">
        {loading ? (
          <div className="flex items-center justify-center py-24">
            <div className="flex flex-col items-center gap-3"><div className="h-10 w-10 animate-spin rounded-full border-4 border-sky-200 border-t-sky-500" /><p className="text-sm text-slate-400">Loading…</p></div>
          </div>
        ) : (
          <>
            {activeTab === "items" && (
              <ItemsDataTable
                items={visibleReturns}
                packages={packages}
                pallets={pallets}
                role={role}
                actor={actor}
                fefoSettings={fefoSettings}
                externalSearch={globalSearchQuery}
                onToast={showToast}
                onRowClick={(r) => openDrawer({ type: "item", record: r })}
                onRowEdit={(r)  => openDrawer({ type: "item", record: r })}
                onBulkDeleted={bulkRemoveReturns}
                onBulkMoved={bulkUpdateReturns}
                onNewItem={() => openWizard()}
              />
            )}

            {activeTab === "packages" && (
              <PackagesDataTable
                packages={packages}
                returns={visibleReturns}
                pallets={pallets}
                role={role}
                actor={actor}
                externalSearch={globalSearchQuery}
                onToast={showToast}
                onRowClick={(p) => openDrawer({ type: "package", record: p })}
                onRowEdit={(p)  => openDrawer({ type: "package", record: p })}
                onBulkDeleted={bulkRemovePackages}
                onBulkPackagesUpdated={bulkUpdatePackages}
                onNewPackage={() => setCreatePackageOpen(true)}
              />
            )}

            {activeTab === "pallets" && (
              <PalletsDataTable
                pallets={pallets}
                packages={packages}
                returns={visibleReturns}
                role={role}
                actor={actor}
                externalSearch={globalSearchQuery}
                onToast={showToast}
                onRowClick={(p) => openDrawer({ type: "pallet", record: p })}
                onRowEdit={(p)  => openDrawer({ type: "pallet", record: p })}
                onBulkDeleted={bulkRemovePallets}
                onNewPallet={() => setCreatePalletOpen(true)}
              />
            )}
          </>
        )}
      </main>

      {/* ── Right Drawer (portal-based — sidebar is ALWAYS visible) ── */}
      <RightDrawer
        open={!!activeDrawer}
        onClose={closeDrawer}
        onBack={canGoBack ? popDrawer : undefined}
        title={drawerTitle()}
        subtitle={drawerSubtitle()}
      >
        {activeDrawer?.type === "item" && (
          <ItemDrawerContent
            record={activeDrawer.record}
            role={role}
            actor={actor}
            packages={packages}
            pallets={pallets}
            sessionPhotos={sessionPhotos.get(activeDrawer.record.id)}
            onToast={showToast}
            onUpdated={(r) => { updateReturn_(r); openDrawer({ type: "item", record: r }); }}
            onDeleted={(id) => { removeReturn(id); closeDrawer(); showToast("Return deleted.", "warning"); }}
          />
        )}

        {activeDrawer?.type === "package" && (
          <PackageDrawerContent
            pkg={activeDrawer.record}
            role={role}
            actor={actor}
            openPallets={openPallets}
            allReturns={visibleReturns}
            onClose={closeDrawer}
            onPackageUpdated={(p) => { updatePackage_(p); setDrawerStack((prev) => prev.map((d) => d.type === "package" && d.record.id === p.id ? { type: "package", record: p } : d)); }}
            onItemAdded={(r) => { addReturn(r); showToast(`✓ Item logged — ${r.product_identifier ?? r.item_name}`); }}
            onPackageDeleted={(id) => { removePackage(id); closeDrawer(); showToast("Package deleted.", "warning"); }}
            onOpenItem={(r) => pushDrawer({ type: "item", record: r })}
            showToast={showToast as (msg: string, kind?: ToastKind) => void}
          />
        )}

        {activeDrawer?.type === "pallet" && (
          <PalletDrawerContent
            pallet={activeDrawer.record}
            role={role}
            actor={actor}
            packages={packages}
            onClose={closeDrawer}
            onPalletUpdated={updatePallet_}
            onPalletDeleted={(id) => { removePallet(id); closeDrawer(); showToast("Pallet deleted.", "warning"); }}
            onOpenPackage={(p) => pushDrawer({ type: "package", record: p })}
            showToast={showToast as (msg: string, kind?: ToastKind) => void}
          />
        )}
      </RightDrawer>

      {/* ── Modals ── */}
      {wizardOpen && (
        <SingleItemWizardModal
          onClose={() => { setWizardOpen(false); setWizardInherited(undefined); }}
          onSuccess={(r, photos) => { addReturn(r, photos); showToast(`✓ Return logged — ${r.product_identifier ?? r.item_name}`); }}
          actor={actor}
          openPackages={openPackages}
          openPallets={openPallets}
          onCreatePackage={() => { setWizardOpen(false); setCreatePackageOpen(true); }}
          onCreatePallet={() => { setWizardOpen(false); setCreatePalletOpen(true); }}
          inheritedContext={wizardInherited}
          aiLabelEnabled={orgSettings.is_ai_label_ocr_enabled}
          onSoftPackageWarning={() => showToast("Warning: This item is not on the package's expected list.", "warning")}
        />
      )}

      {createPackageOpen && (
        <CreatePackageModal
          onClose={() => setCreatePackageOpen(false)}
          onCreated={(p) => { addPackage(p); setCreatePackageOpen(false); showToast(`Package ${p.package_number} created.`); }}
          actor={actor}
          openPallets={openPallets}
          aiPackingSlipEnabled={orgSettings.is_ai_packing_slip_ocr_enabled}
        />
      )}

      {createPalletOpen && (
        <CreatePalletModal
          onClose={() => setCreatePalletOpen(false)}
          onCreated={(p) => { addPallet(p); setCreatePalletOpen(false); showToast(`Pallet ${p.pallet_number} created.`); }}
          actor={actor}
          aiManifestEnabled={orgSettings.is_ai_packing_slip_ocr_enabled}
        />
      )}

      <ToastStack toasts={toasts} />
    </div>
  );
}
