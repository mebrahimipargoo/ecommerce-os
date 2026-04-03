"use client";

import React, { useCallback, useEffect, useState } from "react";
import {
  ArrowRight,
  Loader2,
  Plus,
  Trash2,
} from "lucide-react";
import Link from "next/link";
import { useUserRole } from "../../../../components/UserRoleContext";
import {
  deletePlatformMarketplace,
  listPlatformMarketplaces,
  type PlatformMarketplaceRow,
  upsertPlatformMarketplace,
} from "../../lib/platform-actions";
import {
  listOrganizationFeatures,
  saveOrganizationFeatures,
  type OrganizationFeatureRow,
} from "../../lib/organization-features-actions";

function CompanyFeatureRow({
  row,
  actorUserId,
  onSaved,
}: {
  row: OrganizationFeatureRow;
  actorUserId: string | null;
  onSaved: () => void | Promise<void>;
}) {
  const [debug, setDebug] = useState(row.debug_mode);
  const [labelOcr, setLabelOcr] = useState(row.is_ai_label_ocr_enabled);
  const [packOcr, setPackOcr] = useState(row.is_ai_packing_slip_ocr_enabled);
  const [saving, setSaving] = useState(false);
  const [localErr, setLocalErr] = useState<string | null>(null);

  useEffect(() => {
    setDebug(row.debug_mode);
    setLabelOcr(row.is_ai_label_ocr_enabled);
    setPackOcr(row.is_ai_packing_slip_ocr_enabled);
  }, [row]);

  const dirty =
    debug !== row.debug_mode ||
    labelOcr !== row.is_ai_label_ocr_enabled ||
    packOcr !== row.is_ai_packing_slip_ocr_enabled;

  async function handleSave() {
    setSaving(true);
    setLocalErr(null);
    const res = await saveOrganizationFeatures({
      actorProfileId: actorUserId,
      organizationId: row.organization_id,
      debug_mode: debug,
      is_ai_label_ocr_enabled: labelOcr,
      is_ai_packing_slip_ocr_enabled: packOcr,
    });
    setSaving(false);
    if (!res.ok) {
      setLocalErr(res.error);
      return;
    }
    await onSaved();
  }

  return (
    <li className="flex flex-col gap-3 border-b border-slate-100 py-4 last:border-0 dark:border-slate-800">
      <div className="flex flex-wrap items-center gap-2">
        <p className="min-w-0 flex-1 font-medium text-slate-900 dark:text-slate-100">{row.display_name}</p>
        <code className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-500 dark:bg-slate-800 dark:text-slate-400">
          {row.organization_id}
        </code>
      </div>
      {localErr && (
        <p className="text-xs text-red-600 dark:text-red-400">{localErr}</p>
      )}
      <div className="flex flex-wrap gap-4 text-sm text-slate-700 dark:text-slate-300">
        <label className="inline-flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={debug}
            onChange={(e) => setDebug(e.target.checked)}
            className="rounded border-slate-300"
          />
          <span>Technical debug UI</span>
        </label>
        <label className="inline-flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={labelOcr}
            onChange={(e) => setLabelOcr(e.target.checked)}
            className="rounded border-slate-300"
          />
          <span>AI label OCR</span>
        </label>
        <label className="inline-flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={packOcr}
            onChange={(e) => setPackOcr(e.target.checked)}
            className="rounded border-slate-300"
          />
          <span>AI packing slip OCR</span>
        </label>
      </div>
      <div>
        <button
          type="button"
          disabled={saving || !dirty}
          onClick={() => void handleSave()}
          className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
          Save for this company
        </button>
      </div>
    </li>
  );
}

export default function AdminSettingsPage() {
  const { role, actorUserId } = useUserRole();

  const [rows, setRows] = useState<PlatformMarketplaceRow[]>([]);
  const [featureRows, setFeatureRows] = useState<OrganizationFeatureRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [featuresLoading, setFeaturesLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [iconUrl, setIconUrl] = useState("");
  const [editId, setEditId] = useState<string | null>(null);

  const loadMarketplaces = useCallback(async () => {
    const res = await listPlatformMarketplaces(actorUserId);
    if (!res.ok) {
      setErr(res.error ?? "Could not load platforms.");
      setRows([]);
    } else {
      setRows(res.rows);
    }
  }, [actorUserId]);

  const loadFeatures = useCallback(async () => {
    const res = await listOrganizationFeatures(actorUserId);
    if (!res.ok) {
      setErr(res.error ?? "Could not load company features.");
      setFeatureRows([]);
    } else {
      setFeatureRows(res.rows);
    }
  }, [actorUserId]);

  const load = useCallback(async () => {
    setLoading(true);
    setFeaturesLoading(true);
    setErr(null);
    await Promise.all([loadMarketplaces(), loadFeatures()]);
    setLoading(false);
    setFeaturesLoading(false);
  }, [loadMarketplaces, loadFeatures]);

  useEffect(() => {
    if (role !== "super_admin") return;
    void load();
  }, [role, load]);

  async function handleSavePlatform(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setErr(null);
    const res = await upsertPlatformMarketplace({
      actorProfileId: actorUserId,
      id: editId,
      name,
      slug,
      icon_url: iconUrl || null,
    });
    setSaving(false);
    if (!res.ok) {
      setErr(res.error ?? "Save failed.");
      return;
    }
    setName(""); setSlug(""); setIconUrl(""); setEditId(null);
    await loadMarketplaces();
  }

  function startEdit(r: PlatformMarketplaceRow) {
    setEditId(r.id); setName(r.name); setSlug(r.slug); setIconUrl(r.icon_url ?? "");
  }

  async function handleDelete(id: string) {
    if (!window.confirm("Delete this marketplace entry?")) return;
    setErr(null);
    const res = await deletePlatformMarketplace(actorUserId, id);
    if (!res.ok) { setErr(res.error ?? "Delete failed."); return; }
    await loadMarketplaces();
  }

  if (role !== "super_admin") {
    return (
      <div className="mx-auto max-w-lg p-8">
        <h1 className="text-lg font-semibold text-slate-800 dark:text-slate-100">Admin Settings</h1>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-400">
          This page is only available to Super Admins.
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl space-y-10 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-slate-900 dark:text-slate-50">Admin Settings</h1>
        <p className="mt-1 text-sm text-slate-600 dark:text-slate-400">
          Global marketplace catalog and per-company feature flags. Amazon ledger uploads are on the Imports page.
        </p>
      </div>

      {err && (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
          {err}
        </div>
      )}

      <section className="rounded-2xl border border-violet-200 bg-white p-6 shadow-sm dark:border-violet-900/40 dark:bg-slate-950">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Company feature flags</h2>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Toggle debug overlays and AI OCR defaults per company. Persists to{" "}
          <code className="rounded bg-slate-100 px-1 dark:bg-slate-800">organization_settings</code>.
        </p>
        {featuresLoading ? (
          <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="h-5 w-5 animate-spin" /> Loading companies…
          </div>
        ) : (
          <ul className="mt-2">
            {featureRows.map((r) => (
              <CompanyFeatureRow
                key={r.organization_id}
                row={r}
                actorUserId={actorUserId}
                onSaved={loadFeatures}
              />
            ))}
          </ul>
        )}
      </section>

      <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Marketplaces</h2>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Name, slug, and icon URL. Icons appear on the Returns list when linked by ID or matching slug.
        </p>

        {loading ? (
          <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="h-5 w-5 animate-spin" /> Loading…
          </div>
        ) : (
          <ul className="mt-4 divide-y divide-slate-100 dark:divide-slate-800">
            {rows.map((r) => (
              <li key={r.id} className="flex flex-wrap items-center gap-3 py-3">
                {r.icon_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={r.icon_url} alt="" className="h-8 w-8 object-contain" />
                ) : (
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-slate-100 text-xs text-slate-400 dark:bg-slate-800">—</div>
                )}
                <div className="min-w-0 flex-1">
                  <p className="font-medium text-slate-900 dark:text-slate-100">{r.name}</p>
                  <p className="text-xs text-slate-500">{r.slug}</p>
                </div>
                <button
                  type="button"
                  onClick={() => startEdit(r)}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-xs font-medium hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-900"
                >
                  Edit
                </button>
                <button
                  type="button"
                  onClick={() => void handleDelete(r.id)}
                  className="rounded-lg p-2 text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950/40"
                  title="Delete"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </li>
            ))}
          </ul>
        )}

        <form onSubmit={handleSavePlatform} className="mt-6 space-y-3 rounded-xl border border-dashed border-slate-200 p-4 dark:border-slate-700">
          <p className="text-sm font-medium text-slate-800 dark:text-slate-200">
            {editId ? "Update marketplace" : "Add marketplace"}
          </p>
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              Name
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
                required
              />
            </label>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              Slug
              <input
                value={slug}
                onChange={(e) => setSlug(e.target.value)}
                className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
                placeholder="amazon"
                required
              />
            </label>
          </div>
          <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
            Icon URL
            <input
              value={iconUrl}
              onChange={(e) => setIconUrl(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
              placeholder="https://…"
            />
          </label>
          <div className="flex flex-wrap gap-2">
            <button
              type="submit"
              disabled={saving}
              className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50"
            >
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
              {editId ? "Save changes" : "Add"}
            </button>
            {editId && (
              <button
                type="button"
                onClick={() => { setEditId(null); setName(""); setSlug(""); setIconUrl(""); }}
                className="rounded-xl border border-slate-200 px-4 py-2 text-sm dark:border-slate-700"
              >
                Cancel edit
              </button>
            )}
          </div>
        </form>
      </section>

      <section className="rounded-2xl border border-sky-200 bg-sky-50/60 p-6 dark:border-sky-800/50 dark:bg-sky-950/20">
        <p className="text-xs font-semibold uppercase tracking-wide text-sky-700 dark:text-sky-400">
          Amazon Inventory Ledger
        </p>
        <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
          Filtered or full-file CSV import is on{" "}
          <strong>System Admin → Imports</strong>.
        </p>
        <Link
          href="/imports"
          className="mt-4 inline-flex items-center gap-2 rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700"
        >
          Go to Imports
          <ArrowRight className="h-4 w-4" />
        </Link>
      </section>
    </div>
  );
}
