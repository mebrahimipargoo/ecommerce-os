"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { normalizeRoleKeyForBranding } from "../../../lib/tenant-branding-permissions";
import { getAuthenticatedPlatformRoleKey } from "../settings/platform-settings-actions";
import {
  ORGANIZATION_PLAN_INVALID_MESSAGE,
  ORGANIZATION_TYPE_INVALID_MESSAGE,
  type ProvisionedOrgFieldErrors,
  isOrganizationPlanValue,
  isOrganizationTypeValue,
  validateProvisionedOrganizationFields,
} from "./provisioning-field-validation";

const ORG_PROVISION_ROLE_KEYS = new Set(["super_admin"]);

export type NewOrganizationPageAccess = {
  accessDenied: "not_authenticated" | "forbidden" | null;
};

export async function getNewOrganizationPageAccessAction(): Promise<NewOrganizationPageAccess> {
  const raw = await getAuthenticatedPlatformRoleKey();
  if (!raw) return { accessDenied: "not_authenticated" };
  const roleKey = normalizeRoleKeyForBranding(raw);
  if (!ORG_PROVISION_ROLE_KEYS.has(roleKey)) {
    return { accessDenied: "forbidden" };
  }
  return { accessDenied: null };
}

export type CreateProvisionedOrganizationInput = {
  company_name: string;
  slug: string;
  plan: string;
  /** Maps to `public.organizations.type` (`tenant` | `internal`). */
  organization_type?: string | null;
  is_active: boolean;
  registration_number?: string | null;
  ceo_name?: string | null;
  address?: string | null;
  phone?: string | null;
};

/** Server action result: validation failures use `fieldErrors` (never trust client). */
export type CreateProvisionedOrganizationResult =
  | { ok: true; organizationId: string }
  | { ok: false; fieldErrors: ProvisionedOrgFieldErrors }
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

export async function createProvisionedOrganizationAction(
  input: CreateProvisionedOrganizationInput,
): Promise<CreateProvisionedOrganizationResult> {
  const access = await getNewOrganizationPageAccessAction();
  if (access.accessDenied === "not_authenticated") {
    return { ok: false, error: "You must be signed in." };
  }
  if (access.accessDenied === "forbidden") {
    return {
      ok: false,
      error: "Only super_admin may create organizations.",
    };
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

  const { data: slugRow, error: slugErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("slug", slug)
    .maybeSingle();
  if (slugErr) return { ok: false, error: slugErr.message };
  if (slugRow) {
    return {
      ok: false,
      error: `The slug "${slug}" is already in use. Choose a different slug.`,
    };
  }

  const { data: orgRow, error: orgErr } = await supabaseServer
    .from("organizations")
    .insert({
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
    .select("id")
    .single();

  if (orgErr || !orgRow) {
    const code = (orgErr as { code?: string } | null)?.code;
    if (code === "23505") {
      return {
        ok: false,
        error: `The slug "${slug}" is already in use. Choose a different slug.`,
      };
    }
    return {
      ok: false,
      error: orgErr?.message ?? "Failed to create organization.",
    };
  }

  const organizationId = String((orgRow as { id: unknown }).id ?? "").trim();
  if (!organizationId) {
    return { ok: false, error: "Organization was created but id is missing." };
  }

  const settingsPayload = {
    organization_id: organizationId,
    company_display_name: name,
    logo_url: null as string | null,
    is_ai_label_ocr_enabled: false,
    is_ai_packing_slip_ocr_enabled: false,
    default_claim_evidence: {} as Record<string, never>,
    credentials: {} as Record<string, never>,
    default_store_id: null as string | null,
    is_debug_mode_enabled: false,
    debug_mode: false,
  };

  const { error: settingsErr } = await supabaseServer
    .from("organization_settings")
    .insert(settingsPayload);

  if (settingsErr) {
    await supabaseServer.from("organizations").delete().eq("id", organizationId);
    return { ok: false, error: settingsErr.message };
  }

  return { ok: true, organizationId };
}
