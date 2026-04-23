"use server";

import { type ModuleFeatureBucket, type ModuleFeatureTreeNode } from "../../lib/access-entitlements";
import {
  allPermissionIdsInFeatureBucket,
  effectiveLevelFromFeatureBucket,
  findFeatureBucket,
  type UiAccessLevel,
  permissionIdsForUiLevel,
} from "../../lib/access-level";
import { isUserOverrideLevel } from "../../lib/user-feature-access";
import { getPrimaryModuleFeatureTreeForAccess } from "../platform/access/permissions-actions";
import { supabaseServer } from "../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../lib/supabase-server-auth";
import { normalizeRoleKeyForBranding } from "../../lib/tenant-branding-permissions";
import { isUuidString } from "../../lib/uuid";

export type SettingsPageFeature = "company" | "stores" | "users" | "imports";

const FEATURE_LABEL: Record<SettingsPageFeature, string> = {
  company: "Company",
  stores: "Stores & adapters",
  users: "Users",
  imports: "Imports",
};

const FEATURE_MODULE_KEY: Record<SettingsPageFeature, string> = {
  company: "company",
  stores: "stores",
  users: "users",
  imports: "imports",
};

const SETTINGS_MODULE_KEY = "settings";

type GateOk = { ok: true; profileId: string } | { ok: false; error: string };

function denied(p: "not_authenticated" | "forbidden"): string {
  return p === "not_authenticated" ? "Not authenticated." : "Only super_admin can manage page access on Settings.";
}

async function assertSuperAdminForSettingsPage(): Promise<GateOk> {
  const sid = await getSessionUserIdFromCookies();
  if (!sid || !isUuidString(sid)) return { ok: false, error: denied("not_authenticated") };
  const { data: pro, error } = await supabaseServer
    .from("profiles")
    .select("id, role, role_id")
    .eq("id", sid)
    .maybeSingle();
  if (error || !pro) return { ok: false, error: denied("not_authenticated") };
  const pr = pro as { id?: string; role?: string | null; role_id?: string | null };
  let k = "";
  if (pr.role_id && isUuidString(String(pr.role_id).trim())) {
    const { data: r } = await supabaseServer.from("roles").select("key").eq("id", pr.role_id).maybeSingle();
    if (r && typeof (r as { key?: string }).key === "string") {
      k = String((r as { key: string }).key).trim();
    }
  }
  if (!k && pr.role) k = String(pr.role).trim();
  if (normalizeRoleKeyForBranding(k) !== "super_admin") {
    return { ok: false, error: denied("forbidden") };
  }
  return { ok: true, profileId: String(pr.id ?? sid) };
}

function findSettingsModuleId(tree: ModuleFeatureTreeNode[]): string | null {
  const n = tree.find(
    (node) => node.module.key.trim().toLowerCase() === SETTINGS_MODULE_KEY,
  );
  return n ? String(n.module.id) : null;
}

export type SettingsPageAccessEntityRow = {
  id: string;
  name: string;
  key: string;
  level: UiAccessLevel;
};

export type SettingsPageAccessUserOverrideRow = {
  profileId: string;
  fullName: string;
  override: "none" | "read" | "write";
};

export type SettingsPageAccessData = {
  feature: SettingsPageFeature;
  featureLabel: string;
  moduleId: string;
  moduleFeatureId: string;
  featureKey: string;
  moduleFeatureName: string;
  roles: SettingsPageAccessEntityRow[];
  userOverrides: SettingsPageAccessUserOverrideRow[];
};

/**
 * super_admin: catalog snapshot for one Settings `module_feature`, role levels, and per-user override rows.
 */
export async function getSettingsPageAccessDataAction(
  organizationId: string,
  feature: SettingsPageFeature,
): Promise<
  { ok: true; data: SettingsPageAccessData } | { ok: false; error: string }
> {
  const g = await assertSuperAdminForSettingsPage();
  if (!g.ok) return { ok: false, error: g.error };
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  const featureKey = FEATURE_MODULE_KEY[feature];

  const built = await getPrimaryModuleFeatureTreeForAccess();
  if (!built.ok) return { ok: false, error: built.error };
  const moduleId = findSettingsModuleId(built.tree);
  if (!moduleId) {
    return { ok: false, error: 'Settings module not found in access catalog. Run "npm run sync:sidebar".' };
  }
  const bucket = findFeatureBucket(built.tree, moduleId, featureKey);
  if (!bucket) {
    return {
      ok: false,
      error: `Feature "${SETTINGS_MODULE_KEY}.${featureKey}" is not in the catalog. Run "npm run sync:sidebar" or add the feature in the DB.`,
    };
  }
  if (!bucket.feature || String(bucket.feature.id).startsWith("synthetic:")) {
    return {
      ok: false,
      error: `This feature is not linked to a real module_feature row. Run "npm run sync:sidebar" for ${SETTINGS_MODULE_KEY} / ${featureKey}.`,
    };
  }
  const moduleFeatureId = String(bucket.feature.id);

  const { data: roleRows, error: re } = await supabaseServer
    .from("roles")
    .select("id, key, name")
    .order("name", { ascending: true });
  if (re) return { ok: false, error: re.message };

  const roleOut: SettingsPageAccessEntityRow[] = [];
  for (const raw of roleRows ?? []) {
    const r = raw as Record<string, unknown>;
    const id = String(r.id ?? "").trim();
    if (!id) continue;
    const { data: rp } = await supabaseServer
      .from("role_permissions")
      .select("permission_id")
      .eq("role_id", id);
    const have = new Set(
      (rp ?? [])
        .map((x) => String((x as { permission_id?: unknown }).permission_id ?? "").trim())
        .filter(Boolean),
    );
    const level = effectiveLevelFromFeatureBucket(bucket, have);
    roleOut.push({
      id,
      key: String(r.key ?? "").trim() || "—",
      name: String(r.name ?? "").trim() || id,
      level,
    });
  }

  const { data: orgProfiles, error: perr } = await supabaseServer
    .from("profiles")
    .select("id, full_name")
    .eq("organization_id", oid);
  if (perr) return { ok: false, error: perr.message };
  const pids = (orgProfiles ?? [])
    .map((r) => String((r as { id?: unknown }).id ?? "").trim())
    .filter((id) => isUuidString(id));
  if (pids.length === 0) {
    return {
      ok: true,
      data: {
        feature,
        featureLabel: FEATURE_LABEL[feature],
        moduleId,
        moduleFeatureId,
        featureKey,
        moduleFeatureName: String(bucket.feature?.name ?? featureKey).trim() || featureKey,
        roles: roleOut,
        userOverrides: [],
      },
    };
  }
  const { data: ovr, error: ove } = await supabaseServer
    .from("user_feature_access_overrides")
    .select("profile_id, access_level")
    .eq("module_feature_id", moduleFeatureId)
    .in("profile_id", pids);
  if (ove) return { ok: false, error: ove.message };

  const nameById = new Map(
    (orgProfiles ?? []).map((r) => {
      const c = r as { id?: string; full_name?: string | null };
      return [String(c.id ?? "").trim(), String(c.full_name ?? "").trim() || String(c.id)] as const;
    }),
  );
  const userOverrides: SettingsPageAccessUserOverrideRow[] = [];
  for (const row of ovr ?? []) {
    const pr = String((row as { profile_id?: string }).profile_id ?? "").trim();
    const al = String((row as { access_level?: string }).access_level ?? "").trim().toLowerCase();
    if (!pr || !al || !isUserOverrideLevel(al)) continue;
    userOverrides.push({
      profileId: pr,
      fullName: nameById.get(pr) ?? pr,
      override: al as "none" | "read" | "write",
    });
  }
  userOverrides.sort((a, b) => a.fullName.localeCompare(b.fullName));

  return {
    ok: true,
    data: {
      feature,
      featureLabel: FEATURE_LABEL[feature],
      moduleId,
      moduleFeatureId,
      featureKey,
      moduleFeatureName: String(bucket.feature?.name ?? featureKey).trim() || featureKey,
      roles: roleOut,
      userOverrides,
    },
  };
}

/** UI levels for Settings page admin: none, read, write (maps to permission rows, not manage, unless catalog only has r/w). */
export type SettingsPageUiLevel = "none" | "read" | "write";

function uiToAccessLevel(x: SettingsPageUiLevel): UiAccessLevel {
  if (x === "none") return "none";
  if (x === "read") return "read";
  return "write";
}

function syncSettingsPermissionRows(
  bucket: ModuleFeatureBucket,
  level: SettingsPageUiLevel,
): Set<string> {
  const l = uiToAccessLevel(level);
  return permissionIdsForUiLevel(bucket, l);
}

async function featureBucketForSettings(
  feature: SettingsPageFeature,
): Promise<
  { ok: true; bucket: ModuleFeatureBucket; moduleId: string; featureKey: string } | { ok: false; error: string }
> {
  const featureKey = FEATURE_MODULE_KEY[feature];
  const built = await getPrimaryModuleFeatureTreeForAccess();
  if (!built.ok) return { ok: false, error: built.error };
  const moduleId = findSettingsModuleId(built.tree);
  if (!moduleId) {
    return { ok: false, error: "Settings module missing in catalog." };
  }
  const bucket = findFeatureBucket(built.tree, moduleId, featureKey);
  if (!bucket) return { ok: false, error: "Feature not in catalog." };
  return { ok: true, bucket, moduleId, featureKey };
}

async function syncTargetFeature(
  table: "role_permissions" | "group_permissions",
  col: "role_id" | "group_id",
  targetId: string,
  allIn: string[],
  desired: Set<string>,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (allIn.length === 0) return { ok: true };
  const { data: current, error: ce } = await supabaseServer
    .from(table)
    .select("permission_id")
    .eq(col, targetId)
    .in("permission_id", allIn);
  if (ce) return { ok: false, error: ce.message };
  const have = new Set(
    (current ?? [])
      .map((r) => String((r as { permission_id?: unknown }).permission_id ?? "").trim())
      .filter((id) => id),
  );
  for (const pid of allIn) {
    const w = desired.has(pid);
    const h = have.has(pid);
    if (w && !h) {
      const ins: Record<string, string> = { permission_id: pid };
      ins[col] = targetId;
      const { error: ie } = await supabaseServer.from(table).insert(ins);
      if (ie && (ie as { code?: string }).code !== "23505") return { ok: false, error: ie.message };
    } else if (!w && h) {
      const { error: de } = await supabaseServer
        .from(table)
        .delete()
        .eq(col, targetId)
        .eq("permission_id", pid);
      if (de) return { ok: false, error: de.message };
    }
  }
  return { ok: true };
}

export async function setSettingsPageLevelForRoleAction(input: {
  organizationId: string;
  feature: SettingsPageFeature;
  roleId: string;
  level: SettingsPageUiLevel;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertSuperAdminForSettingsPage();
  if (!g.ok) return { ok: false, error: g.error };
  if (!isUuidString(input.organizationId.trim()) || !isUuidString(input.roleId.trim())) {
    return { ok: false, error: "Invalid id." };
  }
  const b = await featureBucketForSettings(input.feature);
  if (!b.ok) return b;
  const allIn = allPermissionIdsInFeatureBucket(b.bucket);
  const desired = syncSettingsPermissionRows(b.bucket, input.level);
  return syncTargetFeature("role_permissions", "role_id", input.roleId.trim(), allIn, desired);
}

export async function setSettingsPageLevelForGroupAction(input: {
  organizationId: string;
  feature: SettingsPageFeature;
  groupId: string;
  level: SettingsPageUiLevel;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertSuperAdminForSettingsPage();
  if (!g.ok) return { ok: false, error: g.error };
  if (!isUuidString(input.organizationId.trim()) || !isUuidString(input.groupId.trim())) {
    return { ok: false, error: "Invalid id." };
  }
  const b = await featureBucketForSettings(input.feature);
  if (!b.ok) return b;
  const allIn = allPermissionIdsInFeatureBucket(b.bucket);
  const desired = syncSettingsPermissionRows(b.bucket, input.level);
  return syncTargetFeature("group_permissions", "group_id", input.groupId.trim(), allIn, desired);
}

export async function setSettingsPageUserOverrideAction(input: {
  organizationId: string;
  feature: SettingsPageFeature;
  profileId: string;
  level: "inherit" | "none" | "read" | "write";
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertSuperAdminForSettingsPage();
  if (!g.ok) return { ok: false, error: g.error };
  if (!isUuidString(input.profileId.trim())) return { ok: false, error: "Invalid user." };
  const b = await featureBucketForSettings(input.feature);
  if (!b.ok) return b;
  if (!b.bucket.feature || String(b.bucket.feature.id).startsWith("synthetic:")) {
    return { ok: false, error: "Invalid feature row." };
  }
  const moduleFeatureId = String(b.bucket.feature.id);
  const level = input.level;
  if (level === "inherit") {
    const { error } = await supabaseServer
      .from("user_feature_access_overrides")
      .delete()
      .eq("profile_id", input.profileId.trim())
      .eq("module_feature_id", moduleFeatureId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  }
  if (!isUserOverrideLevel(level)) {
    return { ok: false, error: "Invalid level." };
  }
  const { error } = await supabaseServer.from("user_feature_access_overrides").upsert(
    {
      profile_id: input.profileId.trim(),
      module_feature_id: moduleFeatureId,
      access_level: level,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "profile_id,module_feature_id" },
  );
  if (error) return { ok: false, error: error.message };
  return { ok: true };
}

export type SettingsPageSearchUserRow = { profileId: string; fullName: string; email: string };

async function emailByUserIdMap(): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  try {
    const { data, error } = await supabaseServer.auth.admin.listUsers({ perPage: 1000 });
    if (error || !data?.users) return map;
    for (const u of data.users) {
      const e = u.email?.trim();
      if (e) map.set(u.id, e);
    }
  } catch {
    /* ignore */
  }
  return map;
}

/**
 * super_admin: search org users by `full_name` or auth email (substring match, min 2 chars, max 20 rows).
 */
export async function searchSettingsPageOrgUsersAction(
  organizationId: string,
  q: string,
): Promise<
  { ok: true; rows: SettingsPageSearchUserRow[] } | { ok: false; error: string }
> {
  const g = await assertSuperAdminForSettingsPage();
  if (!g.ok) return { ok: false, error: g.error };
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  const pat = String(q ?? "").trim().toLowerCase();
  if (pat.length < 2) {
    return { ok: true, rows: [] };
  }
  const { data: orgRows, error: pe } = await supabaseServer
    .from("profiles")
    .select("id, full_name")
    .eq("organization_id", oid);
  if (pe) return { ok: false, error: pe.message };
  const emailMap = await emailByUserIdMap();
  const out: SettingsPageSearchUserRow[] = [];
  const seen = new Set<string>();
  for (const raw of orgRows ?? []) {
    const c = raw as { id?: string; full_name?: string | null };
    const id = String(c.id ?? "").trim();
    if (!id || seen.has(id)) continue;
    const fn = String(c.full_name ?? "").trim() || id;
    const em = (emailMap.get(id) ?? "").trim() || "—";
    if (fn.toLowerCase().includes(pat) || em.toLowerCase().includes(pat)) {
      seen.add(id);
      out.push({ profileId: id, fullName: fn, email: em });
    }
  }
  out.sort((a, b) => a.fullName.localeCompare(b.fullName));
  return { ok: true, rows: out.slice(0, 20) };
}
