/** Shared validation for `/platform/access` role & group keys and text fields. */

const KEY_PATTERN = /^[a-z0-9_-]{3,50}$/;

export function normalizeAccessEntityKey(raw: string): string {
  return raw
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_");
}

export type FieldErrorMap = Record<string, string>;

export function validateRoleName(nameRaw: string): string | null {
  const name = nameRaw.trim();
  if (name.length < 2) return "Name must be at least 2 characters.";
  if (name.length > 100) return "Name must be at most 100 characters.";
  return null;
}

export function validateRoleKey(keyRaw: string): string | null {
  const key = normalizeAccessEntityKey(keyRaw);
  if (!key) return "Key is required.";
  if (!KEY_PATTERN.test(key)) {
    return "Key must be 3–50 characters: lowercase letters, digits, underscores, hyphens only.";
  }
  return null;
}

export function validateDescription(descRaw: string | null | undefined): string | null {
  if (descRaw == null || String(descRaw).trim() === "") return null;
  if (String(descRaw).trim().length > 300) return "Description must be at most 300 characters.";
  return null;
}

export function validateScope(scope: string): scope is "tenant" | "system" {
  return scope === "tenant" || scope === "system";
}

export function validateGroupName(nameRaw: string): string | null {
  return validateRoleName(nameRaw);
}

export function validateGroupKey(keyRaw: string): string | null {
  return validateRoleKey(keyRaw);
}

export function collectRoleCreateErrors(input: {
  name: string;
  key: string;
  description?: string | null;
  scope: string;
}): FieldErrorMap | null {
  const errors: FieldErrorMap = {};
  const ne = validateRoleName(input.name);
  if (ne) errors.name = ne;
  const ke = validateRoleKey(input.key);
  if (ke) errors.key = ke;
  const de = validateDescription(input.description);
  if (de) errors.description = de;
  if (!validateScope(input.scope)) errors.scope = "Scope must be tenant or system.";
  return Object.keys(errors).length ? errors : null;
}

export function collectRoleUpdateErrors(input: {
  name: string;
  description?: string | null;
  scope: string;
}): FieldErrorMap | null {
  const errors: FieldErrorMap = {};
  const ne = validateRoleName(input.name);
  if (ne) errors.name = ne;
  const de = validateDescription(input.description);
  if (de) errors.description = de;
  if (!validateScope(input.scope)) errors.scope = "Scope must be tenant or system.";
  return Object.keys(errors).length ? errors : null;
}

export function collectGroupCreateErrors(input: {
  organization_id: string;
  name: string;
  key: string;
  description?: string | null;
}): FieldErrorMap | null {
  const errors: FieldErrorMap = {};
  const oid = input.organization_id.trim();
  if (!oid) errors.organization_id = "Organization is required.";
  const ne = validateGroupName(input.name);
  if (ne) errors.name = ne;
  const ke = validateGroupKey(input.key);
  if (ke) errors.key = ke;
  const de = validateDescription(input.description);
  if (de) errors.description = de;
  return Object.keys(errors).length ? errors : null;
}

export function collectGroupUpdateErrors(input: {
  name: string;
  key: string;
  description?: string | null;
}): FieldErrorMap | null {
  const errors: FieldErrorMap = {};
  const ne = validateGroupName(input.name);
  if (ne) errors.name = ne;
  const ke = validateGroupKey(input.key);
  if (ke) errors.key = ke;
  const de = validateDescription(input.description);
  if (de) errors.description = de;
  return Object.keys(errors).length ? errors : null;
}
