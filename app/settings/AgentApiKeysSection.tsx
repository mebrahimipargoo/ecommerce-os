"use client";

import React, { useCallback, useEffect, useState } from "react";
import { Bot, Check, Copy, KeyRound, Loader2, Plus, ShieldAlert, Trash2 } from "lucide-react";
import {
  createOrganizationApiKey,
  listOrganizationApiKeys,
  revokeOrganizationApiKey,
  type OrganizationApiKeyRow,
} from "./organization-api-keys-actions";
import { RoleTagCombobox } from "./RoleTagCombobox";
import { normalizeAIRoleTag } from "../../lib/openai-settings";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
const LABEL = "mb-2 block text-sm font-medium leading-none";

type ToastFn = (msg: string, ok: boolean) => void;

export function AgentApiKeysSection({ showToast }: { showToast: ToastFn }) {
  const [rows, setRows] = useState<OrganizationApiKeyRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [label, setLabel] = useState("");
  const [roleTag, setRoleTag] = useState("integration");
  const [reveal, setReveal] = useState<{ key: string; id: string } | null>(null);
  const [copied, setCopied] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    const res = await listOrganizationApiKeys();
    setLoading(false);
    if (!res.ok) {
      showToast(res.error, false);
      return;
    }
    setRows(res.rows);
  }, [showToast]);

  useEffect(() => {
    void load();
  }, [load]);

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    setCreating(true);
    try {
      const res = await createOrganizationApiKey({
        label: label.trim() || "Agent / integration",
        roleTag: normalizeAIRoleTag(roleTag),
      });
      if (!res.ok) throw new Error(res.error);
      setReveal({ key: res.plaintextKey, id: res.id });
      setCopied(false);
      setLabel("");
      setRoleTag("integration");
      showToast("API key created — copy it now; it will not be shown again.", true);
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Failed to create key.", false);
    } finally {
      setCreating(false);
    }
  }

  async function copyKey(text: string) {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2500);
    } catch {
      showToast("Could not copy to clipboard.", false);
    }
  }

  async function handleRevoke(id: string) {
    if (!window.confirm("Revoke this key? Integrations using it will stop working.")) return;
    const res = await revokeOrganizationApiKey(id);
    if (!res.ok) {
      showToast(res.error ?? "Revoke failed.", false);
      return;
    }
    showToast("Key revoked.", true);
    await load();
  }

  return (
    <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
      <div className="mb-4 flex items-start gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-amber-100 dark:bg-amber-950/50">
          <Bot className="h-5 w-5 text-amber-700 dark:text-amber-400" />
        </div>
        <div>
          <h2 className="text-base font-bold">Workspace API keys (agents &amp; bots)</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Keys for <strong>external</strong> systems calling your workspace APIs. Saved to{" "}
            <code className="rounded bg-muted px-1 font-mono text-[10px]">organization_api_keys</code>{" "}
            (digest in <code className="rounded bg-muted px-1 font-mono text-[10px]">api_key</code>).
            Different from LLM provider keys above.
          </p>
        </div>
      </div>

      <form onSubmit={(e) => void handleCreate(e)} className="mb-6 space-y-4 rounded-xl border border-dashed border-border bg-muted/20 p-4">
        <p className="text-xs font-medium text-muted-foreground">
          <KeyRound className="mr-1 inline h-3.5 w-3.5" />
          Generate a new secret — you will see it once.
        </p>
        <div className="grid gap-4 sm:grid-cols-2 sm:items-start">
          <div className="flex min-w-0 flex-col">
            <label className={LABEL} htmlFor="agent-key-label">Label</label>
            <div className="min-h-[2.5rem] text-xs text-muted-foreground">
              <span className="invisible select-none" aria-hidden>
                .
              </span>
            </div>
            <input
              id="agent-key-label"
              className={INPUT}
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="e.g. Nightly sync bot"
            />
          </div>
          <div className="flex min-w-0 flex-col">
            <label className={LABEL} htmlFor="agent-key-role">Role / tag</label>
            <div className="min-h-[2.5rem] text-xs text-muted-foreground">
              Presets for bots and integrations — or pick a custom tag below.
            </div>
            <RoleTagCombobox
              id="agent-key-role"
              value={roleTag}
              onChange={setRoleTag}
              className={INPUT}
              placeholder="integration"
              aria-label="Role tag for API key"
            />
          </div>
        </div>
        <button
          type="submit"
          disabled={creating}
          className="inline-flex items-center gap-2 rounded-md bg-amber-600 px-4 py-2 text-sm font-semibold text-white shadow transition hover:bg-amber-700 disabled:opacity-50"
        >
          {creating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
          Generate API key
        </button>
      </form>

      {reveal && (
        <div
          role="alert"
          className="mb-6 rounded-xl border-2 border-amber-400 bg-amber-50 p-4 dark:border-amber-600 dark:bg-amber-950/40"
        >
          <div className="flex items-start gap-2">
            <ShieldAlert className="mt-0.5 h-5 w-5 shrink-0 text-amber-700 dark:text-amber-400" />
            <div className="min-w-0 flex-1">
              <p className="text-sm font-bold text-amber-900 dark:text-amber-100">
                Copy this key now — it will not be shown again
              </p>
              <p className="mt-1 break-all font-mono text-xs text-amber-950 dark:text-amber-50">
                {reveal.key}
              </p>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => void copyKey(reveal.key)}
                  className="inline-flex items-center gap-2 rounded-md bg-amber-600 px-3 py-2 text-xs font-semibold text-white hover:bg-amber-700"
                >
                  {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                  {copied ? "Copied" : "Copy to clipboard"}
                </button>
                <button
                  type="button"
                  onClick={() => setReveal(null)}
                  className="rounded-md border border-amber-700/30 bg-white px-3 py-2 text-xs font-semibold text-amber-900 hover:bg-amber-100 dark:bg-amber-900/50 dark:text-amber-100 dark:hover:bg-amber-900"
                >
                  I have stored it safely
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <div>
        <h3 className="mb-2 text-sm font-semibold">Active keys</h3>
        {loading ? (
          <p className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading…
          </p>
        ) : rows.length === 0 ? (
          <p className="py-4 text-sm text-muted-foreground">No workspace API keys yet.</p>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/40 text-left text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  <th className="px-3 py-2">Label</th>
                  <th className="px-3 py-2">Role / tag</th>
                  <th className="px-3 py-2">Key</th>
                  <th className="px-3 py-2">Created</th>
                  <th className="px-3 py-2 text-right"> </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id} className="border-b border-border last:border-0">
                    <td className="px-3 py-2 font-medium">{r.name?.trim() || "—"}</td>
                    <td className="px-3 py-2">
                      <span className="rounded-full border border-border bg-muted/50 px-2 py-0.5 text-[11px]">
                        {r.role}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono text-[11px] text-muted-foreground">
                      {(r.api_key ?? "").slice(0, 14)}…
                      <span className="sr-only"> (digest prefix)</span>
                    </td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">
                      {new Date(r.created_at).toLocaleString()}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => void handleRevoke(r.id)}
                        className="inline-flex items-center gap-1 rounded-md border border-rose-200 px-2 py-1 text-xs font-medium text-rose-600 hover:bg-rose-50 dark:border-rose-800 dark:hover:bg-rose-950/40"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                        Revoke
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
