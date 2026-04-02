"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES } from "../lib/constants";

export type CompanyOption = { id: string; display_name: string };

/**
 * Rows from `public.companies` for the ledger target dropdown (Imports).
 */
export async function listCompaniesForImports(): Promise<
  { ok: true; rows: CompanyOption[] } | { ok: false; error: string }
> {
  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.companies)
      .select("*")
      .order("id", { ascending: true });

    if (error) return { ok: false, error: error.message };

    const rows = (data ?? []) as Record<string, unknown>[];
    const out: CompanyOption[] = rows.map((r) => {
      const id = String(r.id ?? "");
      const display_name =
        (typeof r.name === "string" && r.name.trim()) ||
        (typeof r.display_name === "string" && r.display_name.trim()) ||
        (typeof r.company_name === "string" && r.company_name.trim()) ||
        (typeof r.title === "string" && r.title.trim()) ||
        id;
      return { id, display_name };
    });
    out.sort((a, b) => a.display_name.localeCompare(b.display_name));
    return { ok: true, rows: out };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load companies.",
    };
  }
}

/**
 * Persists the selected workspace company on `profiles.company_id` when it was unset.
 * Called from admin Imports when the user picks a target company.
 */
export async function saveHomeCompanyForProfile(
  profileId: string,
  companyId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const pid = profileId.trim();
  const cid = companyId.trim();
  if (!isUuidString(pid) || !isUuidString(cid)) {
    return { ok: false, error: "Invalid profile or company id." };
  }
  try {
    const { data: co, error: coErr } = await supabaseServer
      .from(DB_TABLES.companies)
      .select("id")
      .eq("id", cid)
      .maybeSingle();
    if (coErr) return { ok: false, error: coErr.message };
    if (!co?.id) return { ok: false, error: "Company not found." };

    const { error } = await supabaseServer
      .from(DB_TABLES.profiles)
      .update({ company_id: cid })
      .eq("id", pid);

    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Could not save company.",
    };
  }
}
