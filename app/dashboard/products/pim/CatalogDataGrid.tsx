"use client";

import React from "react";
import { Image as ImageIcon, Loader2 } from "lucide-react";
import { resolvePimDisplayImageUrl } from "../../../../lib/pim-display-image";
import { IdentifierValue } from "./IdentifierValue";

export type PimCatalogRow = Record<string, unknown>;

const SORTABLE = new Set([
  "product_name",
  "sku",
  "asin",
  "fnsku",
  "brand",
  "status",
  "last_seen_at",
  "updated_at",
  "vendor",
  "category",
  "latest_price",
]);

function formatPrice(row: PimCatalogRow): string {
  const amt = row.latest_price_amount;
  const cur = typeof row.latest_price_currency === "string" ? row.latest_price_currency : "USD";
  const n = typeof amt === "number" ? amt : typeof amt === "string" ? Number.parseFloat(amt) : Number.NaN;
  if (!Number.isFinite(n)) return "—";
  try {
    return new Intl.NumberFormat(undefined, { style: "currency", currency: cur.length === 3 ? cur : "USD" }).format(n);
  } catch {
    return `${cur} ${n.toFixed(2)}`;
  }
}

function formatTs(iso: unknown): string {
  if (typeof iso !== "string" || !iso.trim()) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

function displaySku(row: PimCatalogRow): string {
  const s = typeof row.sku === "string" ? row.sku.trim() : "";
  if (s) return s;
  const m = typeof row.map_seller_sku === "string" ? row.map_seller_sku.trim() : "";
  return m || "";
}

function displayAsin(row: PimCatalogRow): string {
  const a = typeof row.asin === "string" ? row.asin.trim() : "";
  if (a) return a;
  const m = typeof row.map_asin === "string" ? row.map_asin.trim() : "";
  return m || "";
}

function displayFnsku(row: PimCatalogRow): string {
  const f = typeof row.fnsku === "string" ? row.fnsku.trim() : "";
  if (f) return f;
  const m = typeof row.map_fnsku === "string" ? row.map_fnsku.trim() : "";
  return m || "";
}

function displayUpc(row: PimCatalogRow): string {
  const u = typeof row.upc_code === "string" ? row.upc_code.trim() : "";
  if (u) return u;
  const m = typeof row.map_upc === "string" ? row.map_upc.trim() : "";
  return m || "";
}

export function CatalogDataGrid({
  rows,
  total,
  loading,
  page,
  pageSize,
  sort,
  dir,
  onSort,
  onPageChange,
  onPageSizeChange,
  onOpenProduct,
  onEditProduct,
}: {
  rows: PimCatalogRow[];
  total: number;
  loading: boolean;
  page: number;
  pageSize: number;
  sort: string;
  dir: string;
  onSort: (col: string) => void;
  onPageChange: (p: number) => void;
  onPageSizeChange: (n: number) => void;
  onOpenProduct: (id: string) => void;
  onEditProduct?: (id: string) => void;
}) {
  const th = (id: string, label: string) => {
    const active = sort === id;
    return (
      <th className="sticky top-0 z-20 whitespace-nowrap border-b border-border bg-muted/95 px-3 py-2.5 text-left text-xs font-medium text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-muted/80">
        {SORTABLE.has(id) ? (
          <button
            type="button"
            className={[
              "inline-flex items-center gap-1 rounded px-1 py-0.5 hover:bg-muted",
              active ? "text-foreground" : "",
            ].join(" ")}
            onClick={() => onSort(id)}
          >
            {label}
            {active ? (dir === "asc" ? " ↑" : " ↓") : ""}
          </button>
        ) : (
          label
        )}
      </th>
    );
  };

  const pages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto rounded-xl border border-border/60 shadow-sm">
        <table className="w-full min-w-[1400px] border-collapse text-left text-sm">
          <thead className="sticky top-0 z-10">
            <tr>
              {th("image", "Image")}
              {th("product_name", "Product name")}
              {th("vendor", "Vendor")}
              {th("category", "Category")}
              {th("brand", "Brand")}
              {th("sku", "SKU")}
              {th("asin", "ASIN")}
              {th("fnsku", "FNSKU")}
              <th className="sticky top-0 z-20 whitespace-nowrap border-b border-border bg-muted/95 px-3 py-2.5 text-xs font-medium text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-muted/80">
                UPC
              </th>
              {th("status", "Status")}
              {th("latest_price", "Latest price")}
              {th("last_seen_at", "Last activity")}
              <th className="sticky top-0 z-20 whitespace-nowrap border-b border-border bg-muted/95 px-3 py-2.5 text-xs font-medium text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-muted/80">
                Actions
              </th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={13} className="px-4 py-16 text-center text-muted-foreground">
                  <Loader2 className="mx-auto h-8 w-8 animate-spin text-primary" aria-hidden />
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={13} className="px-4 py-12 text-center text-muted-foreground">
                  No products match the current filters for this store.
                </td>
              </tr>
            ) : (
              rows.map((row) => {
                const id = String(row.id ?? "");
                const img = resolvePimDisplayImageUrl(row.main_image_url, row.amazon_raw);
                const name = String(row.product_name ?? "—");
                const vendor = String(row.vendor_name ?? "—");
                const cat = String(row.category_name ?? "—");
                const brand = String(row.brand ?? "—");
                const status = String(row.status ?? "—");
                const last = row.last_seen_at ?? row.updated_at;
                return (
                  <tr key={id} className="border-b border-border/50 hover:bg-muted/20">
                    <td className="px-3 py-2">
                      {img ? (
                        // eslint-disable-next-line @next/next/no-img-element
                        <img src={img} alt="" className="h-10 w-10 rounded-md border border-border object-cover" />
                      ) : (
                        <div className="flex h-10 w-10 items-center justify-center rounded-md bg-muted text-muted-foreground">
                          <ImageIcon className="h-4 w-4" />
                        </div>
                      )}
                    </td>
                    <td className="max-w-[220px] truncate px-3 py-2 font-medium text-foreground" title={name}>
                      {name}
                    </td>
                    <td className="max-w-[140px] truncate px-3 py-2 text-muted-foreground" title={vendor}>
                      {vendor}
                    </td>
                    <td className="max-w-[140px] truncate px-3 py-2 text-muted-foreground" title={cat}>
                      {cat}
                    </td>
                    <td className="max-w-[120px] truncate px-3 py-2 text-muted-foreground">{brand}</td>
                    <td className="min-w-[140px] px-3 py-2">
                      <IdentifierValue value={displaySku(row)} kind="sku" />
                    </td>
                    <td className="min-w-[140px] px-3 py-2">
                      <IdentifierValue value={displayAsin(row) || null} kind="asin" />
                    </td>
                    <td className="min-w-[140px] px-3 py-2">
                      <IdentifierValue value={displayFnsku(row) || null} kind="fnsku" />
                    </td>
                    <td className="min-w-[120px] px-3 py-2">
                      <IdentifierValue value={displayUpc(row) || null} kind="upc" />
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{status}</td>
                    <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{formatPrice(row)}</td>
                    <td className="whitespace-nowrap px-3 py-2 text-xs text-muted-foreground">{formatTs(last)}</td>
                    <td className="whitespace-nowrap px-3 py-2">
                      <div className="flex flex-wrap gap-1">
                        <button
                          type="button"
                          onClick={() => onOpenProduct(id)}
                          className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-muted"
                        >
                          Details
                        </button>
                        {onEditProduct ? (
                          <button
                            type="button"
                            onClick={() => onEditProduct(id)}
                            className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-muted"
                          >
                            Edit
                          </button>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 text-sm">
        <p className="text-muted-foreground">
          {total.toLocaleString()} product{total === 1 ? "" : "s"} · Page {page} of {pages}
        </p>
        <div className="flex flex-wrap items-center gap-2">
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            Per page
            <select
              value={pageSize}
              onChange={(e) => onPageSizeChange(Number(e.target.value))}
              className="h-9 rounded-lg border border-border bg-background px-2 text-sm"
            >
              {[25, 50, 100].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            disabled={page <= 1 || loading}
            onClick={() => onPageChange(page - 1)}
            className="h-9 rounded-lg border border-border px-3 text-sm hover:bg-muted disabled:opacity-40"
          >
            Previous
          </button>
          <button
            type="button"
            disabled={page >= pages || loading}
            onClick={() => onPageChange(page + 1)}
            className="h-9 rounded-lg border border-border px-3 text-sm hover:bg-muted disabled:opacity-40"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
