/**
 * Shared rules for /platform/organizations/new — trim before all checks.
 */

const SLUG_RE = /^[a-z0-9-]{3,50}$/;

/**
 * UX: derive a slug from company/org name (lowercase; spaces and punctuation → hyphens).
 * Max 50 chars. May be shorter than 3 until the user types more — validation still enforces ^[a-z0-9-]{3,50}$.
 */
export function suggestSlugFromName(raw: string): string {
  return String(raw ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 50);
}
const REGISTRATION_RE = /^[A-Za-z0-9-]{3,50}$/;
const PHONE_RE = /^[0-9+\-\s()]{7,20}$/;

/** Allowed `organizations.plan` values — server and client must stay in sync. */
export const ORGANIZATION_PLAN_VALUES = ["free", "pro", "enterprise"] as const;
export type OrganizationPlan = (typeof ORGANIZATION_PLAN_VALUES)[number];

const ORGANIZATION_PLAN_SET = new Set<string>(ORGANIZATION_PLAN_VALUES);

export const ORGANIZATION_PLAN_INVALID_MESSAGE =
  "Plan must be one of: free, pro, enterprise.";

export function isOrganizationPlanValue(v: string): v is OrganizationPlan {
  return ORGANIZATION_PLAN_SET.has(v);
}

/** Allowed `organizations.type` values — server and client must stay in sync. */
export const ORGANIZATION_TYPE_VALUES = ["tenant", "internal"] as const;
export type OrganizationType = (typeof ORGANIZATION_TYPE_VALUES)[number];

const ORGANIZATION_TYPE_SET = new Set<string>(ORGANIZATION_TYPE_VALUES);

export const ORGANIZATION_TYPE_INVALID_MESSAGE =
  "Invalid organization type. Allowed values are tenant or internal (after trim and lowercase).";

export function isOrganizationTypeValue(v: string): v is OrganizationType {
  return ORGANIZATION_TYPE_SET.has(v);
}

export type ProvisionedOrgFormValues = {
  company_name: string;
  slug: string;
  plan: OrganizationPlan;
  organization_type: OrganizationType;
  registration_number: string | null;
  ceo_name: string | null;
  address: string | null;
  phone: string | null;
};

export type ProvisionedOrgFieldErrors = Partial<
  Record<
    | "company_name"
    | "slug"
    | "plan"
    | "organization_type"
    | "registration_number"
    | "ceo_name"
    | "address"
    | "phone",
    string
  >
>;

export type ProvisionedOrgValidationResult =
  | { ok: true; values: ProvisionedOrgFormValues }
  | { ok: false; errors: ProvisionedOrgFieldErrors };

const FIELD_ORDER: (keyof ProvisionedOrgFieldErrors)[] = [
  "company_name",
  "slug",
  "plan",
  "organization_type",
  "registration_number",
  "ceo_name",
  "address",
  "phone",
];

export function firstProvisionedOrgErrorMessage(errors: ProvisionedOrgFieldErrors): string {
  for (const key of FIELD_ORDER) {
    const msg = errors[key];
    if (msg) return msg;
  }
  return "Validation failed.";
}

export function validateProvisionedOrganizationFields(raw: {
  company_name: string;
  slug: string;
  plan: string;
  organization_type?: string | null;
  registration_number?: string | null;
  ceo_name?: string | null;
  address?: string | null;
  phone?: string | null;
}): ProvisionedOrgValidationResult {
  const errors: ProvisionedOrgFieldErrors = {};

  const company_name = String(raw.company_name ?? "").trim();
  if (company_name.length < 2) {
    errors.company_name = "Company name must be at least 2 characters.";
  } else if (company_name.length > 120) {
    errors.company_name = "Company name must be at most 120 characters.";
  }

  const slug = String(raw.slug ?? "").trim().toLowerCase();
  if (!SLUG_RE.test(slug)) {
    errors.slug =
      "Slug must be 3–50 characters: lowercase letters, digits, and hyphens only (no spaces).";
  }

  const planNorm = String(raw.plan ?? "").trim().toLowerCase();
  if (!isOrganizationPlanValue(planNorm)) {
    errors.plan = ORGANIZATION_PLAN_INVALID_MESSAGE;
  }
  const plan = planNorm as OrganizationPlan;

  const orgTypeNorm = String(raw.organization_type ?? "tenant").trim().toLowerCase();
  if (!isOrganizationTypeValue(orgTypeNorm)) {
    errors.organization_type = ORGANIZATION_TYPE_INVALID_MESSAGE;
  }
  const organization_type = orgTypeNorm as OrganizationType;

  const registrationTrim = String(raw.registration_number ?? "").trim();
  const registration_number = registrationTrim.length > 0 ? registrationTrim : null;
  if (registration_number !== null && !REGISTRATION_RE.test(registration_number)) {
    errors.registration_number =
      "Use 3–50 letters, digits, or hyphens only.";
  }

  const ceoTrim = String(raw.ceo_name ?? "").trim();
  const ceo_name = ceoTrim.length > 0 ? ceoTrim : null;
  if (ceo_name !== null) {
    if (ceo_name.length < 2) {
      errors.ceo_name = "CEO name must be at least 2 characters.";
    } else if (ceo_name.length > 100) {
      errors.ceo_name = "CEO name must be at most 100 characters.";
    }
  }

  const addressTrim = String(raw.address ?? "").trim();
  const address = addressTrim.length > 0 ? addressTrim : null;
  if (address !== null && address.length > 300) {
    errors.address = "Address must be at most 300 characters.";
  }

  const phoneTrim = String(raw.phone ?? "").trim();
  const phone = phoneTrim.length > 0 ? phoneTrim : null;
  if (phone !== null && !PHONE_RE.test(phone)) {
    errors.phone =
      "Enter a valid phone (7–20 characters: digits, +, spaces, hyphens, parentheses).";
  }

  if (Object.keys(errors).length > 0) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    values: {
      company_name,
      slug,
      plan,
      organization_type,
      registration_number,
      ceo_name,
      address,
      phone,
    },
  };
}
