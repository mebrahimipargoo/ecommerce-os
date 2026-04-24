"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { assertManagePlatformAccess } from "../access/server-gate";
import {
  ORGANIZATION_PLAN_INVALID_MESSAGE,
  ORGANIZATION_TYPE_INVALID_MESSAGE,
  type ProvisionedOrgFieldErrors,
  isOrganizationPlanValue,
  isOrganizationTypeValue,
  validateProvisionedOrganizationFields,
} from "./provisioning-field-validation";
import {
  type CreateProvisionedOrganizationInput,
  getNewOrganizationPageAccessAction,
} from "./create-organization-actions";

export type OrganizationEditRow = {
  id: string;
  name: string;
  slug: string;
  plan: string;
  /** `organizations.type` */
  organization_type: string;
  is_active: boolean;
  registration_number: string | null;
  ceo_name: string | null;
  address: string | null;
  phone: string | null;
};

export type GetOrganizationForEditResult =
  | { ok: true; organization: OrganizationEditRow }
  | { ok: false; error: string };

function rawProvisionInputFromPayload(
  input: CreateProvisionedOrganizationInput,
): Parameters<typeof validateProvisionedOrganizationFields>[0] {
  return {
    company_name: String(input.company_name ?? ""),
    slug: String(input.slug ?? ""),
    plan: String(input.plan ?? ""),
    organization_type: input.organization_type,
    registration_number: input.registration_number,
    ceo_name: input.ceo_name,
    address: input.address,
    phone: input.phone,
  };
}

export async function getOrganizationForEditAction(
  organizationId: string,
): Promise<GetOrganizationForEditResult> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) {
    return {
      ok: false,
      error: g.denied === "not_authenticated" ? "You must be signed in." : "You do not have access to this organization.",
    };
  }

  const id = String(organizationId ?? "").trim();
  if (!isUuidString(id)) {
    return { ok: false, error: "Invalid organization id." };
  }

  const { data, error } = await supabaseServer
    .from("organizations")
    .select(
      "id, name, slug, plan, type, is_active, registration_number, ceo_name, address, phone",
    )
    .eq("id", id)
    .maybeSingle();

  if (error) {
    return { ok: false, error: error.message };
  }
  if (!data) {
    return { ok: false, error: "Organization not found." };
  }

  const row = data as Record<string, unknown>;
  const typeRaw = typeof row.type === "string" ? row.type.trim().toLowerCase() : "";
  const organization_type =
    typeRaw === "internal" ? "internal" : "tenant";
  return {
    ok: true,
    organization: {
      id: String(row.id ?? ""),
      name: typeof row.name === "string" ? row.name : "",
      slug: typeof row.slug === "string" ? row.slug : "",
      plan: typeof row.plan === "string" ? row.plan : "",
      organization_type,
      is_active: Boolean(row.is_active),
      registration_number:
        typeof row.registration_number === "string" ? row.registration_number : null,
      ceo_name: typeof row.ceo_name === "string" ? row.ceo_name : null,
      address: typeof row.address === "string" ? row.address : null,
      phone: typeof row.phone === "string" ? row.phone : null,
    },
  };
}

export type UpdateProvisionedOrganizationResult =
  | { ok: true }
  | { ok: false; fieldErrors: ProvisionedOrgFieldErrors }
  | { ok: false; error: string };

export async function updateProvisionedOrganizationAction(
  organizationId: string,
  input: CreateProvisionedOrganizationInput,
): Promise<UpdateProvisionedOrganizationResult> {
  const access = await getNewOrganizationPageAccessAction();
  if (access.accessDenied === "not_authenticated") {
    return { ok: false, error: "You must be signed in." };
  }
  if (access.accessDenied === "forbidden") {
    return {
      ok: false,
      error: "Only super_admin may edit organizations.",
    };
  }

  const id = String(organizationId ?? "").trim();
  if (!isUuidString(id)) {
    return { ok: false, error: "Invalid organization id." };
  }

  const validated = validateProvisionedOrganizationFields(rawProvisionInputFromPayload(input));
  if (!validated.ok) {
    return { ok: false, fieldErrors: validated.errors };
  }
  if (!isOrganizationPlanValue(validated.values.plan)) {
    return { ok: false, fieldErrors: { plan: ORGANIZATION_PLAN_INVALID_MESSAGE } };
  }
  if (!isOrganizationTypeValue(validated.values.organization_type)) {
    return {
      ok: false,
      fieldErrors: { organization_type: ORGANIZATION_TYPE_INVALID_MESSAGE },
    };
  }

  const {
    company_name: name,
    slug,
    plan,
    organization_type: orgType,
    registration_number,
    ceo_name,
    address,
    phone,
  } = validated.values;
  const is_active = Boolean(input.is_active);

  const { data: existing, error: existingErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", id)
    .maybeSingle();
  if (existingErr) {
    return { ok: false, error: existingErr.message };
  }
  if (!existing) {
    return { ok: false, error: "Organization not found." };
  }

  const { data: slugRow, error: slugErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("slug", slug)
    .neq("id", id)
    .maybeSingle();
  if (slugErr) {
    return { ok: false, error: slugErr.message };
  }
  if (slugRow) {
    return {
      ok: false,
      error: `The slug "${slug}" is already in use by another organization. Choose a different slug.`,
    };
  }

  const { error: updErr } = await supabaseServer
    .from("organizations")
    .update({
      name,
      slug,
      plan,
      type: orgType,
      is_active,
      registration_number,
      ceo_name,
      address,
      phone,
    })
    .eq("id", id);

  if (updErr) {
    const code = (updErr as { code?: string }).code;
    if (code === "23505") {
      return {
        ok: false,
        error: `The slug "${slug}" is already in use by another organization. Choose a different slug.`,
      };
    }
    return { ok: false, error: updErr.message ?? "Failed to update organization." };
  }

  return { ok: true };
}

export type PlatformOrganizationListRow = {
  id: string;
  name: string;
  slug: string;
  plan: string;
  is_active: boolean;
  created_at: string | null;
};

export type ListPlatformOrganizationsResult =
  | { ok: true; rows: PlatformOrganizationListRow[] }
  | { ok: false; error: string };

/**
 * Platform staff: list all rows from `public.organizations` (service role; same read gate as platform access / modules).
 */
export async function listPlatformOrganizationsAction(): Promise<ListPlatformOrganizationsResult> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) {
    return {
      ok: false,
      error:
        g.denied === "not_authenticated"
          ? "You must be signed in."
          : "You do not have access to the organization directory.",
    };
  }

  const { data, error } = await supabaseServer
    .from("organizations")
    .select("id, name, slug, plan, is_active, created_at")
    .order("created_at", { ascending: false });

  if (error) {
    return { ok: false, error: error.message };
  }

  const rows: PlatformOrganizationListRow[] = (data ?? []).map((r) => {
    const row = r as Record<string, unknown>;
    return {
      id: String(row.id ?? ""),
      name: typeof row.name === "string" ? row.name : "",
      slug: typeof row.slug === "string" ? row.slug : "",
      plan: typeof row.plan === "string" ? row.plan : "",
      is_active: Boolean(row.is_active),
      created_at:
        typeof row.created_at === "string"
          ? row.created_at
          : row.created_at != null
            ? String(row.created_at)
            : null,
    };
  });

  return { ok: true, rows };
}

/** Seeded default tenant — must not be removed via this UI. */
const PROTECTED_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

export type DeleteProvisionedOrganizationResult =
  | { ok: true }
  | { ok: false; error: string };

export async function deleteProvisionedOrganizationAction(
  organizationId: string,
): Promise<DeleteProvisionedOrganizationResult> {
  const access = await getNewOrganizationPageAccessAction();
  if (access.accessDenied === "not_authenticated") {
    return { ok: false, error: "You must be signed in." };
  }
  if (access.accessDenied === "forbidden") {
    return {
      ok: false,
      error: "Only super_admin may delete organizations.",
    };
  }

  const id = String(organizationId ?? "").trim();
  if (!isUuidString(id)) {
    return { ok: false, error: "Invalid organization id." };
  }
  if (id === PROTECTED_ORGANIZATION_ID) {
    return {
      ok: false,
      error: "The default workspace organization cannot be deleted.",
    };
  }

  const { error } = await supabaseServer.from("organizations").delete().eq("id", id);

  if (error) {
    const code = (error as { code?: string }).code;
    if (code === "23503") {
      return {
        ok: false,
        error:
          "Cannot delete this organization while other records still reference it. Remove or reassign dependent data first.",
      };
    }
    return { ok: false, error: error.message ?? "Failed to delete organization." };
  }

  return { ok: true };
}
