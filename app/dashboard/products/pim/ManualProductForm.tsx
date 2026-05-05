"use client";

import React, { useEffect, useState } from "react";
import { Loader2, X } from "lucide-react";
import type { PimStoreOption } from "../pim-actions";

type PimVendorRow = { id: string; name: string };
type PimCategoryRow = { id: string; name: string };

export function ManualProductForm({
  open,
  onClose,
  organizationId,
  storeId,
  stores,
  storesLoading,
  editingProductId,
  onSaved,
}: {
  open: boolean;
  onClose: () => void;
  organizationId: string;
  storeId: string;
  stores: PimStoreOption[];
  storesLoading: boolean;
  editingProductId: string | null;
  onSaved: () => void;
}) {
  const [vendors, setVendors] = useState<PimVendorRow[]>([]);
  const [categories, setCategories] = useState<PimCategoryRow[]>([]);
  const [listsLoading, setListsLoading] = useState(false);
  const [productName, setProductName] = useState("");
  const [localStoreId, setLocalStoreId] = useState(storeId);
  const [vendorId, setVendorId] = useState("");
  const [categoryId, setCategoryId] = useState("");
  const [brand, setBrand] = useState("");
  const [vendorName, setVendorName] = useState("");
  const [sku, setSku] = useState("");
  const [asin, setAsin] = useState("");
  const [fnsku, setFnsku] = useState("");
  const [upc, setUpc] = useState("");
  const [mpn, setMpn] = useState("");
  const [status, setStatus] = useState("");
  const [condition, setCondition] = useState("");
  const [mainImageUrl, setMainImageUrl] = useState("");
  const [notes, setNotes] = useState("");
  const [newVendor, setNewVendor] = useState("");
  const [newCategory, setNewCategory] = useState("");
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    if (!editingProductId) setLocalStoreId(storeId);
    setListsLoading(true);
    const vq = new URLSearchParams({ organization_id: organizationId });
    const cq = new URLSearchParams({ organization_id: organizationId });
    if (storeId.trim()) {
      vq.set("store_id", storeId.trim());
      cq.set("store_id", storeId.trim());
    }
    void Promise.all([
      fetch(`/api/dashboard/vendors?${vq.toString()}`).then((r) => r.json()),
      fetch(`/api/dashboard/product-categories?${cq.toString()}`).then((r) => r.json()),
    ])
      .then(([v, c]) => {
        if (v?.ok) setVendors(v.vendors ?? []);
        if (c?.ok) setCategories(c.categories ?? []);
      })
      .finally(() => setListsLoading(false));
  }, [open, organizationId, storeId, editingProductId]);

  useEffect(() => {
    if (!open) return;
    if (editingProductId) {
      setListsLoading(true);
      const u = new URL(`/api/dashboard/products/${encodeURIComponent(editingProductId)}`, window.location.origin);
      u.searchParams.set("organization_id", organizationId);
      u.searchParams.set("store_id", storeId);
      void fetch(u.toString())
        .then((r) => r.json())
        .then((data: { ok?: boolean; product?: Record<string, unknown> }) => {
          if (!data.ok || !data.product) return;
          const pr = data.product;
          setProductName(String(pr.product_name ?? ""));
          setLocalStoreId(String(pr.store_id ?? storeId));
          setVendorId(String(pr.vendor_id ?? ""));
          setCategoryId(String(pr.category_id ?? ""));
          setBrand(String(pr.brand ?? ""));
          setVendorName(String(pr.vendor_name ?? ""));
          setSku(String(pr.sku ?? ""));
          setAsin(String(pr.asin ?? ""));
          setFnsku(String(pr.fnsku ?? ""));
          setUpc(String(pr.upc_code ?? ""));
          setMpn(String(pr.mfg_part_number ?? ""));
          setStatus(String(pr.status ?? ""));
          setCondition(String(pr.condition ?? ""));
          setMainImageUrl(String(pr.main_image_url ?? ""));
          const meta = pr.metadata as { pim_ui?: { notes?: string } } | undefined;
          setNotes(typeof meta?.pim_ui?.notes === "string" ? meta.pim_ui.notes : "");
        })
        .finally(() => setListsLoading(false));
    } else {
      setProductName("");
      setVendorId("");
      setCategoryId("");
      setBrand("");
      setVendorName("");
      setSku("");
      setAsin("");
      setFnsku("");
      setUpc("");
      setMpn("");
      setStatus("");
      setCondition("");
      setMainImageUrl("");
      setNotes("");
      setNewVendor("");
      setNewCategory("");
      setErr(null);
    }
  }, [open, editingProductId, organizationId, storeId]);

  async function addVendorInline() {
    const n = newVendor.trim();
    if (!n) return;
    const res = await fetch("/api/dashboard/vendors", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ organization_id: organizationId, name: n }),
    });
    const data = (await res.json()) as { ok?: boolean; vendor?: { id: string }; error?: string };
    if (!res.ok || !data.ok) {
      setErr(data.error ?? "Vendor create failed.");
      return;
    }
    setNewVendor("");
    if (data.vendor?.id) setVendorId(data.vendor.id);
    const vq = new URLSearchParams({ organization_id: organizationId });
    if (storeId.trim()) vq.set("store_id", storeId.trim());
    const r = await fetch(`/api/dashboard/vendors?${vq.toString()}`);
    const j = (await r.json()) as { vendors?: PimVendorRow[] };
    setVendors(j.vendors ?? []);
  }

  async function addCategoryInline() {
    const n = newCategory.trim();
    if (!n) return;
    const res = await fetch("/api/dashboard/product-categories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ organization_id: organizationId, name: n }),
    });
    const data = (await res.json()) as { ok?: boolean; category?: { id: string }; error?: string };
    if (!res.ok || !data.ok) {
      setErr(data.error ?? "Category create failed.");
      return;
    }
    setNewCategory("");
    if (data.category?.id) setCategoryId(data.category.id);
    const cq = new URLSearchParams({ organization_id: organizationId });
    if (storeId.trim()) cq.set("store_id", storeId.trim());
    const r = await fetch(`/api/dashboard/product-categories?${cq.toString()}`);
    const j = (await r.json()) as { categories?: PimCategoryRow[] };
    setCategories(j.categories ?? []);
  }

  async function submit() {
    const sid = (localStoreId || storeId).trim();
    if (!productName.trim() || !sku.trim() || !sid) {
      setErr("Product name, SKU, and store are required.");
      return;
    }
    setSaving(true);
    setErr(null);
    try {
      const body = {
        organization_id: organizationId,
        store_id: sid,
        product_name: productName.trim(),
        brand: brand.trim() || null,
        vendor_name: vendorName.trim() || null,
        vendor_id: vendorId || null,
        category_id: categoryId || null,
        sku: sku.trim(),
        asin: asin.trim() || null,
        fnsku: fnsku.trim() || null,
        upc_code: upc.trim() || null,
        mfg_part_number: mpn.trim() || null,
        status: status.trim() || null,
        condition: condition.trim() || null,
        main_image_url: mainImageUrl.trim() || null,
        notes: notes.trim() || null,
      };
      const url = editingProductId
        ? `/api/dashboard/products/${encodeURIComponent(editingProductId)}`
        : "/api/dashboard/products";
      const res = await fetch(url, {
        method: editingProductId ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = (await res.json()) as { ok?: boolean; error?: string };
      if (!res.ok || !data.ok) {
        setErr(data.error ?? "Save failed.");
        return;
      }
      onSaved();
      onClose();
    } finally {
      setSaving(false);
    }
  }

  if (!open) return null;

  const field =
    "mt-1 h-10 w-full rounded-lg border border-border bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-60";

  return (
    <div className="fixed inset-0 z-[350] flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true">
      <div className="max-h-[92vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-border bg-card p-6 shadow-2xl">
        <div className="flex items-start justify-between gap-2">
          <h2 className="text-lg font-semibold text-foreground">{editingProductId ? "Edit product" : "Add product"}</h2>
          <button type="button" onClick={onClose} className="rounded-lg p-1 text-muted-foreground hover:bg-muted" aria-label="Close">
            <X className="h-5 w-5" />
          </button>
        </div>
        {err ? <p className="mt-2 text-sm text-destructive">{err}</p> : null}
        <div className="mt-4 grid gap-3 sm:grid-cols-2">
          <label className="sm:col-span-2 block text-sm font-medium">
            Product name *
            <input value={productName} onChange={(e) => setProductName(e.target.value)} className={field} />
          </label>
          <label className="block text-sm font-medium">
            Store *
            <select
              value={localStoreId}
              disabled={storesLoading || !!editingProductId}
              onChange={(e) => setLocalStoreId(e.target.value)}
              className={field}
            >
              {stores.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.display_name}
                </option>
              ))}
            </select>
          </label>
          <label className="block text-sm font-medium">
            Status
            <input value={status} onChange={(e) => setStatus(e.target.value)} className={field} placeholder="e.g. active" />
          </label>
          <label className="block text-sm font-medium">
            Vendor
            <select value={vendorId} disabled={listsLoading} onChange={(e) => setVendorId(e.target.value)} className={field}>
              <option value="">—</option>
              {vendors.map((v) => (
                <option key={v.id} value={v.id}>
                  {v.name}
                </option>
              ))}
            </select>
          </label>
          <div className="flex items-end gap-2">
            <label className="min-w-0 flex-1 text-sm font-medium">
              New vendor
              <input value={newVendor} onChange={(e) => setNewVendor(e.target.value)} className={field} placeholder="Name" />
            </label>
            <button
              type="button"
              disabled={!newVendor.trim()}
              onClick={() => void addVendorInline()}
              className="mb-0.5 h-10 shrink-0 rounded-lg bg-primary px-3 text-xs font-semibold text-primary-foreground disabled:opacity-50"
            >
              Add
            </button>
          </div>
          <label className="block text-sm font-medium">
            Category
            <select value={categoryId} disabled={listsLoading} onChange={(e) => setCategoryId(e.target.value)} className={field}>
              <option value="">—</option>
              {categories.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          </label>
          <div className="flex items-end gap-2">
            <label className="min-w-0 flex-1 text-sm font-medium">
              New category
              <input value={newCategory} onChange={(e) => setNewCategory(e.target.value)} className={field} placeholder="Name" />
            </label>
            <button
              type="button"
              disabled={!newCategory.trim()}
              onClick={() => void addCategoryInline()}
              className="mb-0.5 h-10 shrink-0 rounded-lg bg-primary px-3 text-xs font-semibold text-primary-foreground disabled:opacity-50"
            >
              Add
            </button>
          </div>
          <label className="block text-sm font-medium">
            Vendor name (display)
            <input value={vendorName} onChange={(e) => setVendorName(e.target.value)} className={field} />
          </label>
          <label className="block text-sm font-medium">
            Brand
            <input value={brand} onChange={(e) => setBrand(e.target.value)} className={field} />
          </label>
          <label className="block text-sm font-medium">
            SKU *
            <input value={sku} onChange={(e) => setSku(e.target.value)} disabled={!!editingProductId} className={`${field} font-mono`} />
          </label>
          <label className="block text-sm font-medium">
            ASIN
            <input value={asin} onChange={(e) => setAsin(e.target.value)} className={`${field} font-mono`} />
          </label>
          <label className="block text-sm font-medium">
            FNSKU
            <input value={fnsku} onChange={(e) => setFnsku(e.target.value)} className={`${field} font-mono`} />
          </label>
          <label className="block text-sm font-medium">
            UPC
            <input value={upc} onChange={(e) => setUpc(e.target.value)} className={`${field} font-mono`} />
          </label>
          <label className="block text-sm font-medium">
            MPN
            <input value={mpn} onChange={(e) => setMpn(e.target.value)} className={field} />
          </label>
          <label className="block text-sm font-medium">
            Condition
            <input value={condition} onChange={(e) => setCondition(e.target.value)} className={field} />
          </label>
          <label className="sm:col-span-2 block text-sm font-medium">
            Main image URL
            <input value={mainImageUrl} onChange={(e) => setMainImageUrl(e.target.value)} className={field} />
          </label>
          <label className="sm:col-span-2 block text-sm font-medium">
            Notes (stored in metadata.pim_ui.notes)
            <textarea value={notes} onChange={(e) => setNotes(e.target.value)} rows={3} className={`${field} h-auto py-2`} />
          </label>
        </div>
        <div className="mt-6 flex justify-end gap-2">
          <button type="button" onClick={onClose} className="h-10 rounded-lg border border-border px-4 text-sm hover:bg-muted">
            Cancel
          </button>
          <button
            type="button"
            disabled={saving || storesLoading || !productName.trim() || !sku.trim()}
            onClick={() => void submit()}
            className="inline-flex h-10 items-center gap-2 rounded-lg bg-primary px-4 text-sm font-semibold text-primary-foreground disabled:opacity-50"
          >
            {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
