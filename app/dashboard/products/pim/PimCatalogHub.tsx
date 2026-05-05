"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { LayoutGrid, ListTree, Plus, RefreshCw, Search } from "lucide-react";
import { getPimManualProductFormDefaults, type PimStoreOption } from "../pim-actions";
import { CatalogDataGrid, type PimCatalogRow } from "./CatalogDataGrid";
import { VendorTreeView, type VendorAggRow } from "./VendorTreeView";
import { ProductDetailDrawer } from "./ProductDetailDrawer";
import { ManualProductForm } from "./ManualProductForm";

type ViewMode = "grid" | "vendor";

type Facets = {
  brands: string[];
  statuses: string[];
  match_sources: string[];
  source_report_types: string[];
};

export function PimCatalogHub({ organizationId }: { organizationId: string | null }) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [stores, setStores] = useState<PimStoreOption[]>([]);
  const [storeLoading, setStoreLoading] = useState(false);
  const [storeId, setStoreId] = useState("");
  const [view, setView] = useState<ViewMode>("grid");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [sort, setSort] = useState("updated_at");
  const [dir, setDir] = useState<"asc" | "desc">("desc");
  const [q, setQ] = useState("");
  const [qDebounced, setQDebounced] = useState("");
  const [vendorFilter, setVendorFilter] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [brandFilter, setBrandFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [matchSourceFilter, setMatchSourceFilter] = useState("");
  const [reportTypeFilter, setReportTypeFilter] = useState("");
  const [missingImage, setMissingImage] = useState(false);
  const [missingAsin, setMissingAsin] = useState(false);
  const [missingFnsku, setMissingFnsku] = useState(false);
  const [rows, setRows] = useState<PimCatalogRow[]>([]);
  const [total, setTotal] = useState(0);
  const [gridLoading, setGridLoading] = useState(false);
  const [gridErr, setGridErr] = useState<string | null>(null);
  const [facets, setFacets] = useState<Facets | null>(null);
  const [vendorsAgg, setVendorsAgg] = useState<VendorAggRow[]>([]);
  const [vendorProducts, setVendorProducts] = useState<Map<string, PimCatalogRow[]>>(new Map());
  const vendorProductsLoaded = useRef(new Set<string>());
  const [treeSearch, setTreeSearch] = useState("");
  const [drawerId, setDrawerId] = useState<string | null>(null);
  const [formOpen, setFormOpen] = useState(false);
  const [editId, setEditId] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);

  const oid = organizationId?.trim() ?? "";

  useEffect(() => {
    const t = window.setTimeout(() => setQDebounced(q.trim()), 350);
    return () => window.clearTimeout(t);
  }, [q]);

  useEffect(() => {
    if (!oid) {
      setStores([]);
      setStoreId("");
      return;
    }
    setStoreLoading(true);
    void getPimManualProductFormDefaults(oid)
      .then((r) => {
        if (!r.ok) {
          setStores([]);
          return;
        }
        setStores(r.stores);
        const fromUrl = searchParams.get("store");
        const preferred =
          fromUrl && r.stores.some((s) => s.id === fromUrl)
            ? fromUrl
            : r.defaultStoreId && r.stores.some((s) => s.id === r.defaultStoreId)
              ? r.defaultStoreId
              : r.stores[0]?.id ?? "";
        setStoreId((prev) => (prev && r.stores.some((s) => s.id === prev) ? prev : preferred));
      })
      .finally(() => setStoreLoading(false));
  }, [oid, searchParams]);

  const syncStoreUrl = useCallback(
    (sid: string) => {
      if (!pathname) return;
      const p = new URLSearchParams(searchParams.toString());
      if (sid) p.set("store", sid);
      else p.delete("store");
      router.replace(`${pathname}?${p.toString()}`, { scroll: false });
    },
    [pathname, router, searchParams],
  );

  const loadFacets = useCallback(async () => {
    if (!oid || !storeId) return;
    const u = new URL("/api/dashboard/products/catalog/facets", window.location.origin);
    u.searchParams.set("organization_id", oid);
    u.searchParams.set("store_id", storeId);
    const res = await fetch(u.toString());
    const data = (await res.json()) as { ok?: boolean; facets?: Facets };
    if (data.ok && data.facets) setFacets(data.facets);
  }, [oid, storeId]);

  const refreshVendorsAgg = useCallback(async () => {
    if (!oid || !storeId) return;
    const u = new URL("/api/dashboard/vendors", window.location.origin);
    u.searchParams.set("organization_id", oid);
    u.searchParams.set("store_id", storeId);
    u.searchParams.set("include_counts", "1");
    const res = await fetch(u.toString());
    const data = (await res.json()) as { ok?: boolean; vendors?: VendorAggRow[] };
    if (data.ok && data.vendors) setVendorsAgg(data.vendors);
  }, [oid, storeId]);

  const loadGrid = useCallback(async () => {
    if (!oid || !storeId) return;
    setGridLoading(true);
    setGridErr(null);
    try {
      const u = new URL("/api/dashboard/products/catalog", window.location.origin);
      u.searchParams.set("organization_id", oid);
      u.searchParams.set("store_id", storeId);
      u.searchParams.set("page", String(page));
      u.searchParams.set("page_size", String(pageSize));
      u.searchParams.set("sort", sort);
      u.searchParams.set("dir", dir);
      if (qDebounced) u.searchParams.set("q", qDebounced);
      if (vendorFilter && /^[0-9a-f-]{36}$/i.test(vendorFilter)) u.searchParams.set("vendor_id", vendorFilter);
      if (categoryFilter && /^[0-9a-f-]{36}$/i.test(categoryFilter)) u.searchParams.set("category_id", categoryFilter);
      if (brandFilter.trim()) u.searchParams.set("brand", brandFilter.trim());
      if (statusFilter.trim()) u.searchParams.set("status", statusFilter.trim());
      if (matchSourceFilter.trim()) u.searchParams.set("match_source", matchSourceFilter.trim());
      if (reportTypeFilter.trim()) u.searchParams.set("source_report_type", reportTypeFilter.trim());
      if (missingImage) u.searchParams.set("missing_image", "true");
      if (missingAsin) u.searchParams.set("missing_asin", "true");
      if (missingFnsku) u.searchParams.set("missing_fnsku", "true");
      const res = await fetch(u.toString());
      const data = (await res.json()) as {
        ok?: boolean;
        rows?: PimCatalogRow[];
        total?: number;
        error?: string;
        details?: string;
      };
      if (!res.ok || !data.ok) {
        setRows([]);
        setTotal(0);
        const base = data.error ?? "Catalog failed.";
        const detail = data.details && data.details !== base ? data.details : "";
        setGridErr(detail ? `${base}\n\n${detail}` : base);
        return;
      }
      setRows((data.rows ?? []) as PimCatalogRow[]);
      setTotal(Number(data.total ?? 0));
    } catch {
      setGridErr("Network error.");
      setRows([]);
      setTotal(0);
    } finally {
      setGridLoading(false);
    }
  }, [
    oid,
    storeId,
    page,
    pageSize,
    sort,
    dir,
    qDebounced,
    vendorFilter,
    categoryFilter,
    brandFilter,
    statusFilter,
    matchSourceFilter,
    reportTypeFilter,
    missingImage,
    missingAsin,
    missingFnsku,
  ]);

  useEffect(() => {
    void loadFacets();
  }, [loadFacets]);

  useEffect(() => {
    void refreshVendorsAgg();
  }, [refreshVendorsAgg]);

  useEffect(() => {
    if (view === "grid") void loadGrid();
  }, [loadGrid, view]);

  useEffect(() => {
    vendorProductsLoaded.current = new Set();
    setVendorProducts(new Map());
  }, [oid, storeId]);

  const loadVendorProducts = useCallback(async (vendorId: string) => {
    if (!oid || !storeId || vendorProductsLoaded.current.has(vendorId)) return;
    vendorProductsLoaded.current.add(vendorId);
    const u = new URL("/api/dashboard/products/catalog", window.location.origin);
    u.searchParams.set("organization_id", oid);
    u.searchParams.set("store_id", storeId);
    u.searchParams.set("vendor_id", vendorId);
    u.searchParams.set("page", "1");
    u.searchParams.set("page_size", "100");
    const res = await fetch(u.toString());
    const data = (await res.json()) as { ok?: boolean; rows?: PimCatalogRow[] };
    if (data.ok && data.rows) {
      setVendorProducts((m) => new Map(m).set(vendorId, data.rows ?? []));
    }
  }, [oid, storeId]);

  const onSort = useCallback(
    (col: string) => {
      setPage(1);
      if (sort === col) setDir((d) => (d === "asc" ? "desc" : "asc"));
      else {
        setSort(col);
        setDir("asc");
      }
    },
    [sort],
  );

  const vendorOptions = useMemo(() => vendorsAgg.map((v) => ({ id: v.id, name: v.name })), [vendorsAgg]);
  const categoryListUrl = useMemo(() => {
    if (!oid) return "";
    return `/api/dashboard/product-categories?organization_id=${encodeURIComponent(oid)}${storeId ? `&store_id=${encodeURIComponent(storeId)}&include_counts=1` : ""}`;
  }, [oid, storeId]);

  const [categoryOptions, setCategoryOptions] = useState<{ id: string; name: string }[]>([]);

  useEffect(() => {
    if (!categoryListUrl) return;
    void fetch(categoryListUrl)
      .then((r) => r.json())
      .then((d: { categories?: { id: string; name: string }[] }) => {
        setCategoryOptions(d.categories ?? []);
      });
  }, [categoryListUrl]);

  if (!oid) {
    return (
      <section className="rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl">
        <p className="text-sm text-muted-foreground">Select a workspace organization to use the catalog.</p>
      </section>
    );
  }

  return (
    <section className="rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl backdrop-blur-md dark:bg-card/50 sm:p-8">
      {toast ? (
        <div className="mb-4 rounded-lg border border-border bg-muted/40 px-3 py-2 text-sm text-foreground" role="status">
          {toast}
        </div>
      ) : null}

      <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Catalog Hub</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Store-scoped catalog with server pagination. Apply migration <code className="rounded bg-muted px-1 text-xs">20260710120000_pim_catalog_products_page</code> for full query support.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            disabled={!storeId || gridLoading}
            title="No image enrichment job is configured."
            className="h-10 cursor-not-allowed rounded-lg border border-dashed border-border px-3 text-xs font-medium text-muted-foreground opacity-70"
          >
            Enrich missing images
          </button>
          <button
            type="button"
            onClick={() => {
              setEditId(null);
              setFormOpen(true);
            }}
            disabled={!storeId}
            className="inline-flex h-10 items-center gap-2 rounded-lg bg-primary px-4 text-sm font-semibold text-primary-foreground disabled:opacity-50"
          >
            <Plus className="h-4 w-4" />
            Add product
          </button>
          <button
            type="button"
            disabled={!storeId || gridLoading}
            onClick={() => void loadGrid()}
            className="inline-flex h-10 items-center gap-2 rounded-lg border border-border bg-background px-3 text-sm font-medium hover:bg-muted"
          >
            <RefreshCw className={`h-4 w-4 ${gridLoading ? "animate-spin" : ""}`} />
            Refresh
          </button>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-end gap-3">
        <label className="text-sm font-medium text-foreground">
          Store *
          <select
            value={storeId}
            disabled={storeLoading || !stores.length}
            onChange={(e) => {
              const v = e.target.value;
              setStoreId(v);
              setPage(1);
              syncStoreUrl(v);
            }}
            className="mt-1 block h-10 min-w-[200px] rounded-lg border border-border bg-background px-3 text-sm"
          >
            {stores.length === 0 ? <option value="">No stores</option> : null}
            {stores.map((s) => (
              <option key={s.id} value={s.id}>
                {s.display_name}
              </option>
            ))}
          </select>
        </label>
        <div className="flex rounded-lg border border-border p-0.5">
          <button
            type="button"
            onClick={() => setView("grid")}
            className={[
              "inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-xs font-medium",
              view === "grid" ? "bg-muted text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground",
            ].join(" ")}
          >
            <LayoutGrid className="h-3.5 w-3.5" />
            Grid
          </button>
          <button
            type="button"
            onClick={() => setView("vendor")}
            className={[
              "inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-xs font-medium",
              view === "vendor" ? "bg-muted text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground",
            ].join(" ")}
          >
            <ListTree className="h-3.5 w-3.5" />
            By vendor
          </button>
        </div>
      </div>

      {!storeId ? (
        <p className="text-sm text-amber-800 dark:text-amber-200">Pick a store to load the catalog.</p>
      ) : (
        <>
          <div className="mb-4 flex flex-wrap items-end gap-3">
            <label className="min-w-[200px] flex-1 text-sm font-medium">
              Search
              <span className="relative mt-1 block">
                <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <input
                  value={q}
                  onChange={(e) => {
                    setQ(e.target.value);
                    setPage(1);
                  }}
                  placeholder="Name, SKU, ASIN, FNSKU, UPC, brand, vendor…"
                  className="h-10 w-full rounded-lg border border-border bg-background py-2 pl-9 pr-3 text-sm"
                />
              </span>
            </label>
          </div>

          <div className="mb-4 grid gap-3 rounded-xl border border-border/50 bg-muted/10 p-4 sm:grid-cols-2 lg:grid-cols-4">
            <label className="text-xs font-medium text-muted-foreground">
              Vendor
              <select
                value={vendorFilter}
                onChange={(e) => {
                  setVendorFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {vendorOptions.map((v) => (
                  <option key={v.id} value={v.id}>
                    {v.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium text-muted-foreground">
              Category
              <select
                value={categoryFilter}
                onChange={(e) => {
                  setCategoryFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {categoryOptions.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium text-muted-foreground">
              Brand
              <select
                value={brandFilter}
                onChange={(e) => {
                  setBrandFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {(facets?.brands ?? []).map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium text-muted-foreground">
              Status
              <select
                value={statusFilter}
                onChange={(e) => {
                  setStatusFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {(facets?.statuses ?? []).map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium text-muted-foreground">
              match_source
              <select
                value={matchSourceFilter}
                onChange={(e) => {
                  setMatchSourceFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {(facets?.match_sources ?? []).map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium text-muted-foreground">
              source_report_type
              <select
                value={reportTypeFilter}
                onChange={(e) => {
                  setReportTypeFilter(e.target.value);
                  setPage(1);
                }}
                className="mt-1 h-9 w-full rounded-lg border border-border bg-background text-sm"
              >
                <option value="">All</option>
                {(facets?.source_report_types ?? []).map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex items-center gap-2 pt-6 text-xs text-muted-foreground">
              <input type="checkbox" checked={missingImage} onChange={(e) => { setMissingImage(e.target.checked); setPage(1); }} />
              Missing image
            </label>
            <label className="flex items-center gap-2 pt-6 text-xs text-muted-foreground">
              <input type="checkbox" checked={missingAsin} onChange={(e) => { setMissingAsin(e.target.checked); setPage(1); }} />
              Missing ASIN
            </label>
            <label className="flex items-center gap-2 pt-6 text-xs text-muted-foreground">
              <input type="checkbox" checked={missingFnsku} onChange={(e) => { setMissingFnsku(e.target.checked); setPage(1); }} />
              Missing FNSKU
            </label>
          </div>

          {gridErr ? (
            <div className="mb-4 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
              {gridErr}
            </div>
          ) : null}

          {view === "grid" ? (
            <CatalogDataGrid
              rows={rows}
              total={total}
              loading={gridLoading}
              page={page}
              pageSize={pageSize}
              sort={sort}
              dir={dir}
              onSort={onSort}
              onPageChange={(p) => setPage(p)}
              onPageSizeChange={(n) => {
                setPageSize(n);
                setPage(1);
              }}
              onOpenProduct={(id) => setDrawerId(id)}
              onEditProduct={(id) => {
                setEditId(id);
                setFormOpen(true);
              }}
            />
          ) : (
            <VendorTreeView
              vendors={vendorsAgg}
              productsByVendor={vendorProducts}
              treeSearch={treeSearch}
              onTreeSearchChange={setTreeSearch}
              onOpenProduct={(id) => setDrawerId(id)}
              onViewVendorInGrid={(vid) => {
                setVendorFilter(vid);
                setView("grid");
                setPage(1);
              }}
              onVendorOpened={(vid) => void loadVendorProducts(vid)}
            />
          )}
        </>
      )}

      {drawerId && storeId ? (
        <ProductDetailDrawer
          organizationId={oid}
          storeId={storeId}
          productId={drawerId}
          onClose={() => setDrawerId(null)}
          onEdit={(id) => {
            setDrawerId(null);
            setEditId(id);
            setFormOpen(true);
          }}
        />
      ) : null}

      <ManualProductForm
        open={formOpen}
        onClose={() => {
          setFormOpen(false);
          setEditId(null);
        }}
        organizationId={oid}
        storeId={storeId}
        stores={stores}
        storesLoading={storeLoading}
        editingProductId={editId}
        onSaved={() => {
          setToast("Saved.");
          window.setTimeout(() => setToast(null), 3000);
          void loadGrid();
          void loadFacets();
          void refreshVendorsAgg();
          window.dispatchEvent(new Event("pim-catalog-refresh"));
        }}
      />
    </section>
  );
}
