"use server";

import {
  allPermissionIdsInFeatureBucket,
  effectiveLevelFromFeatureBucket,
  filterAccessManagementPrimaryTree,
  findAllFeatureBucketsInModule,
  findFeatureBucket,
  type UiAccessLevel,
  permissionIdsForUiLevel,
} from "../../../lib/access-level";
import {
  buildEntitlementModeMapsForTree,
  buildModuleFeatureTree,
  buildOrgEntitlementSnapshot,
  filterModuleFeatureTreeToOrgLicensing,
  mergeModulesWithPermissionKeys,
  orgAllowsPermission,
  orgEntitlementPayloadAllFeaturesUnderModules,
  snapshotToPayload,
  type AccessModuleFeatureRow,
  type AccessModuleRow,
  type AccessPermissionRow,
  type ModuleFeatureBucket,
  type ModuleFeatureTreeNode,
  type OrgEntitlementsPayload,
} from "../../../lib/access-entitlements";
import {
  effectiveUserFeatureAfterOverride,
  isUserOverrideLevel,
  maxAccessLevel,
} from "../../../lib/user-feature-access";
import type { UserFeatureAccessRow, UserOverrideChoice } from "../../../lib/user-feature-access";
export type { UserFeatureAccessRow } from "../../../lib/user-feature-access";
import { supabaseServer } from "../../../lib/supabase-server";
import { normalizeRoleKeyForBranding } from "../../../lib/tenant-branding-permissions";
import { isUuidString } from "../../../lib/uuid";
import { assertManagePlatformAccess } from "./server-gate";
import { rowMatchesReportModuleGroup } from "../../../lib/access-report-filters";
import { sortAndAnnotateModuleFeatureTreeBySidebar } from "../../../lib/sidebar-access-ordering";

function splitJoined<T>(raw: unknown): T | null {
  if (raw == null) return null;
  if (Array.isArray(raw)) return (raw[0] as T | undefined) ?? null;
  return raw as T;
}

function deniedMessage(d: "not_authenticated" | "forbidden"): string {
  return d === "not_authenticated" ? "Not authenticated." : "Forbidden.";
}

export type PermissionCatalogRow = {
  id: string;
  key: string;
  name: string;
  module: string;
  description: string | null;
};

export type AccessInspectorUserRow = {
  id: string;
  full_name: string;
  organization_id: string;
  organization_name: string;
  role_key: string;
  role_display_name: string | null;
};

export type UserEffectiveGroupRow = {
  id: string;
  key: string;
  name: string;
};

export type EffectivePermissionRow = PermissionCatalogRow & {
  fromRole: boolean;
  fromGroup: boolean;
};

export type UserEffectiveAccessResult = {
  profile_id: string;
  organization_id: string;
  full_name: string;
  organization_name: string;
  role: { key: string; display_name: string | null };
  groups: UserEffectiveGroupRow[];
  /** Union of `role_permissions` and `group_permissions` only (user overrides are in `userFeatureAccessByModuleFeatureId`). */
  permissions: EffectivePermissionRow[];
  /** Per licensed module_feature: baseline = max(role, group), override from `user_feature_access_overrides`, effective after org + override. */
  userFeatureAccessByModuleFeatureId: Record<string, UserFeatureAccessRow>;
  orgEntitlements: OrgEntitlementsPayload | null;
};

function toPermissionRow(raw: Record<string, unknown>): PermissionCatalogRow {
  const m = String(raw.module ?? "").trim();
  return {
    id: String(raw.id ?? ""),
    key: String(raw.key ?? "").trim(),
    name: String(raw.name ?? "").trim(),
    module: m || "general",
    description: raw.description != null ? String(raw.description) : null,
  };
}

function numOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function loadModulesCatalog(): Promise<{
  modules: AccessModuleRow[];
  features: AccessModuleFeatureRow[];
}> {
  const empty = { modules: [] as AccessModuleRow[], features: [] as AccessModuleFeatureRow[] };
  try {
    const modsWithOrder = await supabaseServer
      .from("modules")
      .select("id, key, name, sort_order")
      .order("sort_order", { ascending: true, nullsFirst: false })
      .order("key", { ascending: true });
    const modsFallback = modsWithOrder.error
      ? await supabaseServer.from("modules").select("id, key, name").order("key", { ascending: true })
      : null;
    if (modsWithOrder.error && modsFallback?.error) return empty;
    const mods = (modsWithOrder.error ? modsFallback?.data : modsWithOrder.data) ?? [];

    const featsWithOrder = await supabaseServer
      .from("module_features")
      .select("id, module_id, key, name, sort_order")
      .order("sort_order", { ascending: true, nullsFirst: false })
      .order("key", { ascending: true });
    const featsFallback = featsWithOrder.error
      ? await supabaseServer
          .from("module_features")
          .select("id, module_id, key, name")
          .order("key", { ascending: true })
      : null;
    if (featsWithOrder.error && featsFallback?.error) {
      return {
        modules: mods.map((raw) => {
          const r = raw as Record<string, unknown>;
          return {
            id: String(r.id ?? ""),
            key: String(r.key ?? "").trim(),
            name: String(r.name ?? "").trim() || String(r.key ?? "").trim(),
            sort_order: numOrNull(r.sort_order),
          };
        }).filter((m) => m.id && m.key),
        features: [],
      };
    }
    const feats = (featsWithOrder.error ? featsFallback?.data : featsWithOrder.data) ?? [];

    const modules = (mods ?? [])
      .map((raw) => {
        const r = raw as Record<string, unknown>;
        return {
          id: String(r.id ?? ""),
          key: String(r.key ?? "").trim(),
          name: String(r.name ?? "").trim() || String(r.key ?? "").trim(),
          sort_order: numOrNull(r.sort_order),
        };
      })
      .filter((m) => m.id && m.key);
    const features = (feats ?? [])
      .map((raw) => {
        const r = raw as Record<string, unknown>;
        return {
          id: String(r.id ?? ""),
          module_id: String(r.module_id ?? "").trim(),
          key: String(r.key ?? "").trim(),
          name: String(r.name ?? "").trim() || String(r.key ?? "").trim(),
          sort_order: numOrNull(r.sort_order),
        };
      })
      .filter((f) => f.id && f.module_id && f.key);
    return { modules, features };
  } catch {
    return empty;
  }
}

async function getOrganizationType(organizationId: string): Promise<"internal" | "tenant" | null> {
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return null;
  try {
    const { data, error } = await supabaseServer.from("organizations").select("type").eq("id", oid).maybeSingle();
    if (error || !data) return null;
    const t = String((data as { type?: string }).type ?? "")
      .trim()
      .toLowerCase();
    if (t === "internal") return "internal";
    if (t === "tenant" || t === "") return "tenant";
    return "tenant";
  } catch {
    return null;
  }
}

async function loadOrgEntitlementRows(organizationId: string): Promise<{
  orgModules: Record<string, unknown>[];
  orgFeatures: Record<string, unknown>[];
} | null> {
  try {
    const { data: om, error: e1 } = await supabaseServer
      .from("organization_modules")
      .select("*")
      .eq("organization_id", organizationId);
    if (e1) return null;
    const { data: omf, error: e2 } = await supabaseServer
      .from("organization_module_features")
      .select("*")
      .eq("organization_id", organizationId);
    if (e2) return null;
    return {
      orgModules: (om ?? []) as Record<string, unknown>[],
      orgFeatures: (omf ?? []) as Record<string, unknown>[],
    };
  } catch {
    return null;
  }
}

type BuildOrgTreeOptions = {
  /**
   * When true (default), the tree is limited to what Access Management should show
   * (`filterModuleFeatureTreeToOrgLicensing`). Set false for the Platform “Modules & entitlements”
   * editor, which must list every catalog module so licensing can be configured.
   */
  applyOrgLicensingFilter?: boolean;
};

async function buildLicensedModuleFeatureTreeForOrg(
  organizationId: string,
  options: BuildOrgTreeOptions = {},
): Promise<
  | { ok: true; moduleFeatureTree: ModuleFeatureTreeNode[]; orgEntitlements: OrgEntitlementsPayload }
  | { ok: false; error: string }
> {
  const applyOrgLicensingFilter = options.applyOrgLicensingFilter !== false;
  const oid = organizationId.trim();
  if (!isUuidString(oid)) {
    return { ok: false, error: "Invalid organization." };
  }
  try {
    const { modules: dbMods, features } = await loadModulesCatalog();
    const { data, error } = await supabaseServer
      .from("permissions")
      .select("id, key, name, module, description")
      .order("module", { ascending: true })
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: PermissionCatalogRow[] = (data ?? [])
      .map((raw) => toPermissionRow(raw as Record<string, unknown>))
      .filter((x) => x.id && x.key);
    const mergedMods = mergeModulesWithPermissionKeys(dbMods, rows);
    const asAccess: AccessPermissionRow[] = rows.map((r) => ({ ...r }));
    const fullTree = buildModuleFeatureTree(mergedMods, features, asAccess);
    const moduleFeatureTree = sortAndAnnotateModuleFeatureTreeBySidebar(
      filterAccessManagementPrimaryTree(fullTree),
    );

    const orgKind = await getOrganizationType(oid);
    if (orgKind === "internal") {
      const orgEntitlements = orgEntitlementPayloadAllFeaturesUnderModules(moduleFeatureTree);
      const treeOut = applyOrgLicensingFilter
        ? filterModuleFeatureTreeToOrgLicensing(moduleFeatureTree, orgEntitlements)
        : moduleFeatureTree;
      return {
        ok: true,
        moduleFeatureTree: treeOut,
        orgEntitlements,
      };
    }

    const loaded = await loadOrgEntitlementRows(oid);
    if (!loaded) {
      const orgEntitlements: OrgEntitlementsPayload = {
        modulesExplicit: false,
        moduleEntitledById: {},
        featuresExplicitByModuleId: {},
        featureEntitledById: {},
      };
      const treeOut = applyOrgLicensingFilter
        ? filterModuleFeatureTreeToOrgLicensing(moduleFeatureTree, orgEntitlements)
        : moduleFeatureTree;
      return {
        ok: true,
        moduleFeatureTree: treeOut,
        orgEntitlements,
      };
    }
    const { data: mfLite, error: mfErr } = await supabaseServer
      .from("module_features")
      .select("id, module_id");
    if (mfErr) return { ok: false, error: mfErr.message };
    const mfRows = (mfLite ?? [])
      .map((raw) => {
        const r = raw as Record<string, unknown>;
        return { id: String(r.id ?? "").trim(), module_id: String(r.module_id ?? "").trim() };
      })
      .filter((x) => x.id && x.module_id);
    const snap = buildOrgEntitlementSnapshot(loaded.orgModules, loaded.orgFeatures, mfRows);
    const base = snapshotToPayload(snap);
    const modes = buildEntitlementModeMapsForTree(loaded.orgModules, loaded.orgFeatures, moduleFeatureTree, snap);
    const orgEntitlements: OrgEntitlementsPayload = { ...base, ...modes };
    const treeOut = applyOrgLicensingFilter
      ? filterModuleFeatureTreeToOrgLicensing(moduleFeatureTree, orgEntitlements)
      : moduleFeatureTree;
    return {
      ok: true,
      moduleFeatureTree: treeOut,
      orgEntitlements,
    };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to build licensed access tree for organization.",
    };
  }
}

export async function listPermissionsCatalogAction(): Promise<
  | { ok: true; rows: PermissionCatalogRow[]; moduleFeatureTree: ModuleFeatureTreeNode[] }
  | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  try {
    const { modules: dbMods, features } = await loadModulesCatalog();
    const { data, error } = await supabaseServer
      .from("permissions")
      .select("id, key, name, module, description")
      .order("module", { ascending: true })
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: PermissionCatalogRow[] = (data ?? [])
      .map((raw) => toPermissionRow(raw as Record<string, unknown>))
      .filter((x) => x.id && x.key);

    const mergedMods = mergeModulesWithPermissionKeys(dbMods, rows);
    const asAccess: AccessPermissionRow[] = rows.map((r) => ({ ...r }));
    const fullTree = buildModuleFeatureTree(mergedMods, features, asAccess);
    const moduleFeatureTree = sortAndAnnotateModuleFeatureTreeBySidebar(
      filterAccessManagementPrimaryTree(fullTree),
    );

    return { ok: true, rows, moduleFeatureTree };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load permissions." };
  }
}

/**
 * Module/feature tree for one org: tenant companies only see modules (and, when using per-feature
 * licensing, features) that are `enabled` in org entitlements. Internal orgs see the full catalog.
 * `orgEntitlements` drives status badges in the feature tree.
 */
export async function listAccessCatalogForOrganizationAction(
  organizationId: string,
): Promise<
  | { ok: true; moduleFeatureTree: ModuleFeatureTreeNode[]; orgEntitlements: OrgEntitlementsPayload }
  | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  return buildLicensedModuleFeatureTreeForOrg(oid, { applyOrgLicensingFilter: true });
}

/**
 * Full product module/feature tree for one org with entitlement state — for the Platform
 * “Modules & entitlements” page. Unlike {@link listAccessCatalogForOrganizationAction}, does not
 * hide unlicensed modules; admins configure licensing here.
 */
export async function getEntitlementsEditorCatalogForOrganizationAction(
  organizationId: string,
): Promise<
  | { ok: true; moduleFeatureTree: ModuleFeatureTreeNode[]; orgEntitlements: OrgEntitlementsPayload }
  | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  return buildLicensedModuleFeatureTreeForOrg(oid, { applyOrgLicensingFilter: false });
}

export async function listProfilesForAccessInspectorAction(): Promise<
  { ok: true; rows: AccessInspectorUserRow[] } | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  try {
    const { data, error } = await supabaseServer
      .from("profiles")
      .select(
        "id, full_name, organization_id, role, role_id, organizations!profiles_organization_id_fkey(name), roles!profiles_role_id_fkey(key, name)",
      )
      .order("full_name", { ascending: true })
      .limit(4000);
    if (error) return { ok: false, error: error.message };

    const rows: AccessInspectorUserRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const id = String(r.id ?? "").trim();
      const oid = String(r.organization_id ?? "").trim();
      const orgJoin = splitJoined<{ name?: string | null }>(r.organizations);
      const orgName =
        orgJoin?.name != null && String(orgJoin.name).trim()
          ? String(orgJoin.name).trim()
          : oid;
      const roleJoin = splitJoined<{ key?: string | null; name?: string | null }>(r.roles);
      const keyFromJoin = roleJoin?.key != null ? String(roleJoin.key).trim() : "";
      const legacyRole = r.role != null ? String(r.role).trim() : "";
      const role_key =
        keyFromJoin || normalizeRoleKeyForBranding(legacyRole) || "operator";
      const role_display_name =
        roleJoin?.name != null && String(roleJoin.name).trim()
          ? String(roleJoin.name).trim()
          : null;
      return {
        id,
        full_name: String(r.full_name ?? "").trim() || id,
        organization_id: oid,
        organization_name: orgName,
        role_key,
        role_display_name,
      };
    }).filter((x) => x.id && x.organization_id && isUuidString(x.organization_id));

    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load users." };
  }
}

async function resolveRoleIdForProfile(
  profileId: string,
): Promise<{ roleId: string | null; error: string | null }> {
  const { data: profile, error: pe } = await supabaseServer
    .from("profiles")
    .select("role_id, role")
    .eq("id", profileId)
    .maybeSingle();
  if (pe) return { roleId: null, error: pe.message };
  if (!profile) return { roleId: null, error: "Profile not found." };

  const p = profile as { role_id?: unknown; role?: unknown };
  const ridRaw = p.role_id;
  if (ridRaw != null && isUuidString(String(ridRaw))) {
    return { roleId: String(ridRaw).trim(), error: null };
  }
  const key = normalizeRoleKeyForBranding(p.role != null ? String(p.role) : null);
  if (!key) return { roleId: null, error: null };
  const { data: roleRow, error: re } = await supabaseServer
    .from("roles")
    .select("id")
    .eq("key", key)
    .maybeSingle();
  if (re) return { roleId: null, error: re.message };
  const id = roleRow && typeof (roleRow as { id?: unknown }).id === "string" ? String((roleRow as { id: string }).id) : "";
  return { roleId: id || null, error: null };
}

function findNodeAndBucketByModuleFeatureId(
  tree: ModuleFeatureTreeNode[],
  moduleFeatureId: string,
): { node: ModuleFeatureTreeNode; bucket: ModuleFeatureBucket } | null {
  for (const node of tree) {
    for (const bucket of node.features) {
      const feat = bucket.feature;
      if (feat && !String(feat.id).startsWith("synthetic:") && String(feat.id) === moduleFeatureId) {
        return { node, bucket };
      }
    }
  }
  return null;
}

export async function getUserEffectiveAccessAction(
  profileId: string,
): Promise<{ ok: true; data: UserEffectiveAccessResult } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const pid = profileId.trim();
  if (!isUuidString(pid)) return { ok: false, error: "Invalid user." };

  try {
    const { data: prof, error: perr } = await supabaseServer
      .from("profiles")
      .select(
        "id, full_name, organization_id, role, organizations!profiles_organization_id_fkey(name), roles!profiles_role_id_fkey(key, name)",
      )
      .eq("id", pid)
      .maybeSingle();
    if (perr) return { ok: false, error: perr.message };
    if (!prof) return { ok: false, error: "User not found." };

    const r = prof as Record<string, unknown>;
    const orgId = String(r.organization_id ?? "").trim();
    const orgJoin = splitJoined<{ name?: string | null }>(r.organizations);
    const organization_name =
      orgJoin?.name != null && String(orgJoin.name).trim()
        ? String(orgJoin.name).trim()
        : orgId;
    const full_name = String(r.full_name ?? "").trim() || pid;
    const roleJoin = splitJoined<{ key?: string | null; name?: string | null }>(r.roles);
    const keyFromJoin = roleJoin?.key != null ? String(roleJoin.key).trim() : "";
    const legacyRole = r.role != null ? String(r.role).trim() : "";
    const role_key =
      keyFromJoin || normalizeRoleKeyForBranding(legacyRole) || "operator";
    const role_display_name =
      roleJoin?.name != null && String(roleJoin.name).trim()
        ? String(roleJoin.name).trim()
        : null;

    const { roleId, error: roleErr } = await resolveRoleIdForProfile(pid);
    if (roleErr) return { ok: false, error: roleErr };

    const permIdSet = new Set<string>();
    const fromRoleIds = new Set<string>();
    const fromGroupIds = new Set<string>();

    if (roleId) {
      const { data: rp, error: rpe } = await supabaseServer
        .from("role_permissions")
        .select("permission_id")
        .eq("role_id", roleId);
      if (rpe) return { ok: false, error: rpe.message };
      for (const row of rp ?? []) {
        const id = String((row as { permission_id?: unknown }).permission_id ?? "").trim();
        if (id) {
          permIdSet.add(id);
          fromRoleIds.add(id);
        }
      }
    }

    const { data: ugRows, error: ugErr } = await supabaseServer
      .from("user_groups")
      .select("group_id")
      .eq("profile_id", pid);
    if (ugErr) return { ok: false, error: ugErr.message };

    const candidateGroupIds = [...new Set(
      (ugRows ?? [])
        .map((x) => String((x as { group_id?: unknown }).group_id ?? "").trim())
        .filter((id) => id && isUuidString(id)),
    )];

    const groupsOut: UserEffectiveGroupRow[] = [];
    if (candidateGroupIds.length > 0 && orgId && isUuidString(orgId)) {
      const { data: gRows, error: gErr } = await supabaseServer
        .from("groups")
        .select("id, organization_id, key, name")
        .in("id", candidateGroupIds)
        .eq("organization_id", orgId);
      if (gErr) return { ok: false, error: gErr.message };
      const orgGroupIds: string[] = [];
      for (const raw of gRows ?? []) {
        const gr = raw as Record<string, unknown>;
        const gid = String(gr.id ?? "").trim();
        const k = String(gr.key ?? "").trim();
        const n = String(gr.name ?? "").trim() || k;
        if (!gid) continue;
        orgGroupIds.push(gid);
        groupsOut.push({ id: gid, key: k, name: n });
      }
      if (orgGroupIds.length > 0) {
        const { data: gpRows, error: gpErr } = await supabaseServer
          .from("group_permissions")
          .select("permission_id")
          .in("group_id", orgGroupIds);
        if (gpErr) return { ok: false, error: gpErr.message };
        for (const row of gpRows ?? []) {
          const id = String((row as { permission_id?: unknown }).permission_id ?? "").trim();
          if (id) {
            permIdSet.add(id);
            fromGroupIds.add(id);
          }
        }
      }
    }

    groupsOut.sort((a, b) => a.name.localeCompare(b.name));

    const { data: ovrRows, error: ovrErr } = await supabaseServer
      .from("user_feature_access_overrides")
      .select("module_feature_id, access_level")
      .eq("profile_id", pid);
    if (ovrErr) return { ok: false, error: ovrErr.message };
    const overrideByMf = new Map<string, UserOverrideChoice>();
    for (const row of ovrRows ?? []) {
      const mf = String((row as { module_feature_id?: unknown }).module_feature_id ?? "").trim();
      const al = String((row as { access_level?: unknown }).access_level ?? "").trim();
      if (!mf || !isUserOverrideLevel(al)) continue;
      overrideByMf.set(mf, al);
    }

    let orgEntitlements: OrgEntitlementsPayload | null = null;
    let userFeatureAccessByModuleFeatureId: Record<string, UserFeatureAccessRow> = {};
    if (orgId && isUuidString(orgId)) {
      const licensed = await buildLicensedModuleFeatureTreeForOrg(orgId);
      if (!licensed.ok) return { ok: false, error: licensed.error };
      orgEntitlements = licensed.orgEntitlements;
      for (const node of licensed.moduleFeatureTree) {
        for (const bucket of node.features) {
          const feat = bucket.feature;
          if (!feat || String(feat.id).startsWith("synthetic:")) continue;
          const moduleFeatureId = String(feat.id);
          const roleLevel = effectiveLevelFromFeatureBucket(bucket, fromRoleIds);
          const groupLevel = effectiveLevelFromFeatureBucket(bucket, fromGroupIds);
          const baseline = maxAccessLevel(roleLevel, groupLevel);
          const org = orgAllowsPermission({
            snapshot: licensed.orgEntitlements,
            moduleId: node.module.id,
            moduleFeatureId,
            featureKey: bucket.featureKey,
          });
          const ovr: UserOverrideChoice = overrideByMf.get(moduleFeatureId) ?? "inherit";
          const effective = effectiveUserFeatureAfterOverride({
            orgEntitled: org.entitled,
            baseline,
            override: ovr,
          });
          userFeatureAccessByModuleFeatureId[moduleFeatureId] = {
            module_feature_id: moduleFeatureId,
            baseline,
            override: ovr,
            effective,
          };
        }
      }
      const missingMf = [...overrideByMf.keys()].filter((id) => !userFeatureAccessByModuleFeatureId[id]);
      if (missingMf.length > 0) {
        const primary = await buildPrimaryModuleFeatureTree();
        if (primary.ok) {
          for (const mfId of missingMf) {
            const hit = findNodeAndBucketByModuleFeatureId(primary.tree, mfId);
            if (!hit) continue;
            const { node, bucket } = hit;
            const feat = bucket.feature;
            if (!feat || String(feat.id).startsWith("synthetic:")) continue;
            const roleLevel = effectiveLevelFromFeatureBucket(bucket, fromRoleIds);
            const groupLevel = effectiveLevelFromFeatureBucket(bucket, fromGroupIds);
            const baseline = maxAccessLevel(roleLevel, groupLevel);
            const org = orgAllowsPermission({
              snapshot: licensed.orgEntitlements,
              moduleId: node.module.id,
              moduleFeatureId: mfId,
              featureKey: bucket.featureKey,
            });
            const ovr: UserOverrideChoice = overrideByMf.get(mfId) ?? "inherit";
            const effective = effectiveUserFeatureAfterOverride({
              orgEntitled: org.entitled,
              baseline,
              override: ovr,
            });
            userFeatureAccessByModuleFeatureId[mfId] = {
              module_feature_id: mfId,
              baseline,
              override: ovr,
              effective,
            };
          }
        }
      }
    }

    let permissions: EffectivePermissionRow[] = [];
    if (permIdSet.size > 0) {
      const { data: permRows, error: permErr } = await supabaseServer
        .from("permissions")
        .select("id, key, name, module, description")
        .in("id", [...permIdSet]);
      if (permErr) return { ok: false, error: permErr.message };
      permissions = (permRows ?? []).map((raw) => {
        const p = toPermissionRow(raw as Record<string, unknown>);
        return {
          ...p,
          fromRole: fromRoleIds.has(p.id),
          fromGroup: fromGroupIds.has(p.id),
        };
      }).filter((x) => x.id && x.key);
      permissions.sort(
        (a, b) => a.module.localeCompare(b.module) || a.name.localeCompare(b.name),
      );
    }

    return {
      ok: true,
      data: {
        profile_id: pid,
        organization_id: orgId,
        full_name,
        organization_name,
        role: { key: role_key, display_name: role_display_name },
        groups: groupsOut,
        permissions,
        userFeatureAccessByModuleFeatureId,
        orgEntitlements,
      },
    };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load access." };
  }
}

const ACCESS_REPORT_MAX_USERS = 200;
const ACCESS_REPORT_CONCURRENCY = 6;

function accessLevelRankForReport(l: UiAccessLevel): number {
  if (l === "none") return 0;
  if (l === "read") return 1;
  if (l === "write") return 2;
  if (l === "manage") return 3;
  return 0;
}

async function buildModuleFeatureLabelMapByIdFromPrimary(): Promise<
  Map<string, { moduleKey: string; moduleName: string; featureKey: string; featureName: string }>
> {
  const r = await buildPrimaryModuleFeatureTree();
  if (!r.ok) return new Map();
  const map = new Map<string, { moduleKey: string; moduleName: string; featureKey: string; featureName: string }>();
  for (const node of r.tree) {
    for (const b of node.features) {
      const feat = b.feature;
      if (!feat || String(feat.id).startsWith("synthetic:")) continue;
      const id = String(feat.id);
      const name = String((feat as { name?: unknown }).name ?? "").trim() || b.featureKey;
      map.set(id, {
        moduleKey: String(node.module.key),
        moduleName: String(node.module.name ?? node.module.key),
        featureKey: b.featureKey,
        featureName: name,
      });
    }
  }
  return map;
}

/**
 * One row per user per licensed module feature: effective = role+groups baseline + per-user feature override, within org entitlements.
 */
export type AccessReportRow = {
  organization_id: string;
  organization_name: string;
  profile_id: string;
  full_name: string;
  role_key: string;
  module_key: string;
  module_name: string;
  feature_key: string;
  feature_name: string;
  module_feature_id: string;
  baseline: UiAccessLevel;
  override: UserOverrideChoice;
  effective: UiAccessLevel;
};

export type AccessMatrixReportInput = {
  organizationId: string;
  minEffective?: "all" | "read_or_more" | "write_or_more" | "manage_only";
  userSearch?: string;
  featureSearch?: string;
  /**
   * Report-only high-level area (e.g. `operations`, `warehouse`, `returns`) — not every catalog `modules.key`.
   * Omit for all. Refine with `featureSearch` for specific features.
   */
  reportModuleGroup?: string;
};

export async function getAccessMatrixReportForOrganizationAction(
  input: AccessMatrixReportInput,
): Promise<
  | { ok: true; rows: AccessReportRow[]; userCount: number; truncated: boolean }
  | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const oid = input.organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };

  const minEff = input.minEffective ?? "all";
  const userQ = (input.userSearch ?? "").trim().toLowerCase();
  const featureQ = (input.featureSearch ?? "").trim().toLowerCase();
  const reportModuleGroup = (input.reportModuleGroup ?? "").trim() || undefined;

  try {
    const labelMap = await buildModuleFeatureLabelMapByIdFromPrimary();
    if (labelMap.size === 0) {
      return { ok: false, error: "Access catalog is empty; cannot build report." };
    }

    const { data: profs, error: pe } = await supabaseServer
      .from("profiles")
      .select("id, full_name")
      .eq("organization_id", oid)
      .order("full_name", { ascending: true })
      .limit(ACCESS_REPORT_MAX_USERS + 1);
    if (pe) return { ok: false, error: pe.message };
    const rawList = (profs ?? []) as { id?: unknown; full_name?: unknown }[];
    const truncated = rawList.length > ACCESS_REPORT_MAX_USERS;
    const profRows = rawList.slice(0, ACCESS_REPORT_MAX_USERS);

    const out: AccessReportRow[] = [];

    for (let i = 0; i < profRows.length; i += ACCESS_REPORT_CONCURRENCY) {
      const batch = profRows.slice(i, i + ACCESS_REPORT_CONCURRENCY);
      const results = await Promise.all(
        batch.map((raw) => getUserEffectiveAccessAction(String(raw.id ?? "").trim())),
      );
      for (let k = 0; k < batch.length; k++) {
        const res = results[k];
        if (!res.ok) continue;
        const data = res.data;
        if (userQ) {
          const matchUser =
            data.full_name.toLowerCase().includes(userQ)
            || data.profile_id.toLowerCase().includes(userQ)
            || data.role.key.toLowerCase().includes(userQ);
          if (!matchUser) continue;
        }
        for (const [mfId, ufa] of Object.entries(data.userFeatureAccessByModuleFeatureId)) {
          const eff = ufa.effective;
          if (minEff === "read_or_more" && accessLevelRankForReport(eff) < 1) continue;
          if (minEff === "write_or_more" && accessLevelRankForReport(eff) < 2) continue;
          if (minEff === "manage_only" && eff !== "manage") continue;
          const meta = labelMap.get(mfId);
          const moduleKey = meta?.moduleKey ?? "—";
          const moduleName = meta?.moduleName ?? "—";
          const featureKey = meta?.featureKey ?? mfId;
          const featureName = meta?.featureName ?? mfId;
          if (featureQ) {
            const bucket = `${moduleKey} ${moduleName} ${featureKey} ${featureName}`.toLowerCase();
            if (!bucket.includes(featureQ)) continue;
          }
          if (
            !rowMatchesReportModuleGroup(
              reportModuleGroup,
              moduleKey,
              featureKey,
              featureName,
            )
          ) {
            continue;
          }
          out.push({
            organization_id: data.organization_id,
            organization_name: data.organization_name,
            profile_id: data.profile_id,
            full_name: data.full_name,
            role_key: data.role.key,
            module_key: moduleKey,
            module_name: moduleName,
            feature_key: featureKey,
            feature_name: featureName,
            module_feature_id: mfId,
            baseline: ufa.baseline,
            override: ufa.override,
            effective: eff,
          });
        }
      }
    }

    out.sort(
      (a, b) =>
        a.full_name.localeCompare(b.full_name)
        || a.module_key.localeCompare(b.module_key)
        || a.feature_key.localeCompare(b.feature_key)
        || a.profile_id.localeCompare(b.profile_id),
    );

    return { ok: true, rows: out, userCount: profRows.length, truncated };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Report failed." };
  }
}

export async function getAssignedPermissionIdsForRoleAction(
  roleId: string,
): Promise<{ ok: true; permissionIds: string[] } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const rid = roleId.trim();
  if (!isUuidString(rid)) return { ok: false, error: "Invalid role." };
  try {
    const { data, error } = await supabaseServer
      .from("role_permissions")
      .select("permission_id")
      .eq("role_id", rid);
    if (error) return { ok: false, error: error.message };
    const permissionIds = [...new Set(
      (data ?? [])
        .map((x) => String((x as { permission_id?: unknown }).permission_id ?? "").trim())
        .filter((id) => id && isUuidString(id)),
    )];
    return { ok: true, permissionIds };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load role permissions." };
  }
}

export async function getAssignedPermissionIdsForGroupAction(
  groupId: string,
): Promise<{ ok: true; permissionIds: string[] } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const gid = groupId.trim();
  if (!isUuidString(gid)) return { ok: false, error: "Invalid group." };
  try {
    const { data, error } = await supabaseServer
      .from("group_permissions")
      .select("permission_id")
      .eq("group_id", gid);
    if (error) return { ok: false, error: error.message };
    const permissionIds = [...new Set(
      (data ?? [])
        .map((x) => String((x as { permission_id?: unknown }).permission_id ?? "").trim())
        .filter((id) => id && isUuidString(id)),
    )];
    return { ok: true, permissionIds };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load group permissions." };
  }
}

export async function setRolePermissionAssignedAction(input: {
  roleId: string;
  permissionId: string;
  assigned: boolean;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const roleId = input.roleId.trim();
  const permissionId = input.permissionId.trim();
  if (!isUuidString(roleId) || !isUuidString(permissionId)) {
    return { ok: false, error: "Invalid id." };
  }
  try {
    if (input.assigned) {
      const { error } = await supabaseServer.from("role_permissions").insert({
        role_id: roleId,
        permission_id: permissionId,
      });
      if (error) {
        if (error.code === "23505") return { ok: true };
        return { ok: false, error: error.message };
      }
    } else {
      const { error } = await supabaseServer
        .from("role_permissions")
        .delete()
        .eq("role_id", roleId)
        .eq("permission_id", permissionId);
      if (error) return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function setGroupPermissionAssignedAction(input: {
  groupId: string;
  permissionId: string;
  assigned: boolean;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const groupId = input.groupId.trim();
  const permissionId = input.permissionId.trim();
  if (!isUuidString(groupId) || !isUuidString(permissionId)) {
    return { ok: false, error: "Invalid id." };
  }
  try {
    if (input.assigned) {
      const { error } = await supabaseServer.from("group_permissions").insert({
        group_id: groupId,
        permission_id: permissionId,
      });
      if (error) {
        if (error.code === "23505") return { ok: true };
        return { ok: false, error: error.message };
      }
    } else {
      const { error } = await supabaseServer
        .from("group_permissions")
        .delete()
        .eq("group_id", groupId)
        .eq("permission_id", permissionId);
      if (error) return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Build primary Access Management module/feature tree (modules + `module_features` + permissions)
 * for catalog-driven updates. Used by platform access UI and `settings` page access tools.
 */
export async function getPrimaryModuleFeatureTreeForAccess(): Promise<
  { ok: true; tree: ModuleFeatureTreeNode[] } | { ok: false; error: string }
> {
  return buildPrimaryModuleFeatureTree();
}

/** Build primary catalog (modules + module_features) for a single feature update. */
async function buildPrimaryModuleFeatureTree(): Promise<
  { ok: true; tree: ModuleFeatureTreeNode[] } | { ok: false; error: string }
> {
  const { modules: dbMods, features } = await loadModulesCatalog();
  const { data, error } = await supabaseServer
    .from("permissions")
    .select("id, key, name, module, description")
    .order("module", { ascending: true })
    .order("name", { ascending: true });
  if (error) return { ok: false, error: error.message };
  const rows: PermissionCatalogRow[] = (data ?? [])
    .map((raw) => toPermissionRow(raw as Record<string, unknown>))
    .filter((x) => x.id && x.key);
  const mergedMods = mergeModulesWithPermissionKeys(dbMods, rows);
  const asAccess: AccessPermissionRow[] = rows.map((r) => ({ ...r }));
  const full = buildModuleFeatureTree(mergedMods, features, asAccess);
  return {
    ok: true,
    tree: sortAndAnnotateModuleFeatureTreeBySidebar(filterAccessManagementPrimaryTree(full)),
  };
}

async function syncTargetFeaturePermissions(
  table: "role_permissions" | "group_permissions",
  targetColumn: "role_id" | "group_id",
  targetId: string,
  allInFeature: string[],
  desired: Set<string>,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (allInFeature.length === 0) {
    return { ok: true };
  }
  const { data: current, error: ce } = await supabaseServer
    .from(table)
    .select("permission_id")
    .eq(targetColumn, targetId)
    .in("permission_id", allInFeature);
  if (ce) return { ok: false, error: ce.message };
  const have = new Set(
    (current ?? [])
      .map((r) => String((r as { permission_id?: unknown }).permission_id ?? "").trim())
      .filter((id) => id),
  );
  for (const pid of allInFeature) {
    const w = desired.has(pid);
    const h = have.has(pid);
    if (w && !h) {
      const ins: Record<string, string> = { permission_id: pid };
      ins[targetColumn] = targetId;
      const { error: ie } = await supabaseServer.from(table).insert(ins);
      if (ie && ie.code !== "23505") return { ok: false, error: ie.message };
    } else if (!w && h) {
      const { error: de } = await supabaseServer
        .from(table)
        .delete()
        .eq(targetColumn, targetId)
        .eq("permission_id", pid);
      if (de) return { ok: false, error: de.message };
    }
  }
  return { ok: true };
}

export type SetFeatureAccessLevelTarget = "role" | "group";

/**
 * Set one feature’s access level (None / Read / Write / Manage) for a role or group.
 * Per-user access uses `setUserFeatureOverrideAction` (inherit / none / read / write), not flat `user_permissions`.
 */
export async function setUserFeatureOverrideAction(input: {
  profileId: string;
  moduleFeatureId: string;
  level: UserOverrideChoice;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const profileId = input.profileId.trim();
  const moduleFeatureId = input.moduleFeatureId.trim();
  if (!isUuidString(profileId) || !isUuidString(moduleFeatureId)) {
    return { ok: false, error: "Invalid id." };
  }
  const level = input.level;
  if (level !== "inherit" && !isUserOverrideLevel(level)) {
    return { ok: false, error: "Invalid override level." };
  }
  try {
    if (level === "inherit") {
      const { error } = await supabaseServer
        .from("user_feature_access_overrides")
        .delete()
        .eq("profile_id", profileId)
        .eq("module_feature_id", moduleFeatureId);
      if (error) return { ok: false, error: error.message };
      return { ok: true };
    }
    const { error } = await supabaseServer.from("user_feature_access_overrides").upsert(
      {
        profile_id: profileId,
        module_feature_id: moduleFeatureId,
        access_level: level,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "profile_id,module_feature_id" },
    );
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Set one feature’s access level (None / Read / Write / Manage) for a role or group.
 * Backed by flat R/W/M permission rows in the catalog; updates only ids that belong to this feature bucket.
 */
export async function setFeatureAccessLevelForTargetAction(input: {
  target: SetFeatureAccessLevelTarget;
  targetId: string;
  moduleId: string;
  featureKey: string;
  level: UiAccessLevel;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const targetId = input.targetId.trim();
  const moduleId = input.moduleId.trim();
  const featureKey = input.featureKey.trim();
  if (!isUuidString(targetId)) return { ok: false, error: "Invalid target id." };
  if (!moduleId || !featureKey) return { ok: false, error: "Invalid module or feature." };

  try {
    const built = await buildPrimaryModuleFeatureTree();
    if (!built.ok) return { ok: false, error: built.error };
    const bucket = findFeatureBucket(built.tree, moduleId, featureKey);
    if (!bucket) return { ok: false, error: "Feature not found in catalog." };

    const allInFeature = allPermissionIdsInFeatureBucket(bucket);
    const desired = permissionIdsForUiLevel(bucket, input.level);

    const table: "role_permissions" | "group_permissions" =
      input.target === "role" ? "role_permissions" : "group_permissions";
    const col: "role_id" | "group_id" = input.target === "role" ? "role_id" : "group_id";

    return await syncTargetFeaturePermissions(table, col, targetId, allInFeature, desired);
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Set the same R/W/M level for every feature in a module (role or group), in one call.
 */
export async function setFeatureAccessLevelForEntireModuleForTargetAction(input: {
  target: SetFeatureAccessLevelTarget;
  targetId: string;
  moduleId: string;
  level: UiAccessLevel;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const targetId = input.targetId.trim();
  const moduleId = input.moduleId.trim();
  if (!isUuidString(targetId) || !moduleId) {
    return { ok: false, error: "Invalid target or module id." };
  }
  const table: "role_permissions" | "group_permissions" =
    input.target === "role" ? "role_permissions" : "group_permissions";
  const col: "role_id" | "group_id" = input.target === "role" ? "role_id" : "group_id";
  try {
    const built = await buildPrimaryModuleFeatureTree();
    if (!built.ok) return { ok: false, error: built.error };
    const buckets = findAllFeatureBucketsInModule(built.tree, moduleId);
    if (buckets.length === 0) {
      return { ok: false, error: "Module not found in catalog or has no features." };
    }
    for (const bucket of buckets) {
      const allInFeature = allPermissionIdsInFeatureBucket(bucket);
      const desired = permissionIdsForUiLevel(bucket, input.level);
      const res = await syncTargetFeaturePermissions(table, col, targetId, allInFeature, desired);
      if (!res.ok) return res;
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Set per-user override for all real `module_feature` rows in a module, or clear overrides (`inherit`).
 * User override DB levels are only none|read|write; `manage` is applied as `write` on the override.
 */
export async function setUserModuleBulkOverrideAction(input: {
  profileId: string;
  moduleId: string;
  level: UiAccessLevel | "inherit";
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedMessage(g.denied) };
  const profileId = input.profileId.trim();
  const moduleId = input.moduleId.trim();
  if (!isUuidString(profileId) || !moduleId) {
    return { ok: false, error: "Invalid id." };
  }
  const level = input.level;
  if (level !== "inherit" && level !== "none" && level !== "read" && level !== "write" && level !== "manage") {
    return { ok: false, error: "Invalid level." };
  }
  try {
    const built = await buildPrimaryModuleFeatureTree();
    if (!built.ok) return { ok: false, error: built.error };
    const buckets = findAllFeatureBucketsInModule(built.tree, moduleId);
    const mfIds = buckets
      .map((b) => b.feature)
      .filter(
        (f) =>
          f != null
          && String(f.id).trim() !== ""
          && !String(f.id).startsWith("synthetic:"),
      )
      .map((f) => String(f!.id));
    if (mfIds.length === 0) {
      return { ok: false, error: "No module_feature rows in this module; nothing to set." };
    }
    if (level === "inherit") {
      for (const moduleFeatureId of mfIds) {
        const { error } = await supabaseServer
          .from("user_feature_access_overrides")
          .delete()
          .eq("profile_id", profileId)
          .eq("module_feature_id", moduleFeatureId);
        if (error) return { ok: false, error: error.message };
      }
      return { ok: true };
    }
    const overrideLevel: "none" | "read" | "write" =
      level === "none" || level === "read" || level === "write" ? level : "write";
    const ts = new Date().toISOString();
    for (const moduleFeatureId of mfIds) {
      const { error } = await supabaseServer.from("user_feature_access_overrides").upsert(
        {
          profile_id: profileId,
          module_feature_id: moduleFeatureId,
          access_level: overrideLevel,
          updated_at: ts,
        },
        { onConflict: "profile_id,module_feature_id" },
      );
      if (error) return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}
