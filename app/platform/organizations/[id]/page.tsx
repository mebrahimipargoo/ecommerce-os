"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { CheckCircle2, Loader2 } from "lucide-react";
import { useParams, useRouter } from "next/navigation";
import { getPlatformAccessPageAccessAction } from "../../access/access-actions";
import { getNewOrganizationPageAccessAction } from "../create-organization-actions";
import {
  deleteProvisionedOrganizationAction,
  getOrganizationForEditAction,
  updateProvisionedOrganizationAction,
  type OrganizationEditRow,
} from "../edit-organization-actions";
import {
  type ProvisionedOrgFieldErrors,
  isOrganizationPlanValue,
  isOrganizationTypeValue,
  suggestSlugFromName,
  validateProvisionedOrganizationFields,
} from "../provisioning-field-validation";
import {
  responsiveFormInput,
  responsiveFormSelect,
  responsivePageInner,
  responsivePageNarrow,
  responsivePageOuter,
} from "../../../../lib/responsive-page-shell";
import { useUserRole } from "../../../../components/UserRoleContext";
import { PageHeaderWithInfo } from "../../components/page-header-with-info";

function inputClass(invalid: boolean): string {
  return invalid
    ? `${responsiveFormInput} border-destructive focus-visible:ring-destructive`
    : responsiveFormInput;
}

function selectClass(invalid: boolean): string {
  return invalid
    ? `${responsiveFormSelect} border-destructive focus-visible:ring-destructive`
    : responsiveFormSelect;
}

export default function EditOrganizationPage() {
  const router = useRouter();
  const params = useParams();
  const rawId = params?.id;
  const organizationId = typeof rawId === "string" ? rawId : Array.isArray(rawId) ? rawId[0] : "";

  const { refreshProfile } = useUserRole();
  const [loading, setLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [plan, setPlan] = useState("free");
  const [organizationType, setOrganizationType] = useState("tenant");
  const [isActive, setIsActive] = useState(true);
  const [registrationNumber, setRegistrationNumber] = useState("");
  const [ceoName, setCeoName] = useState("");
  const [address, setAddress] = useState("");
  const [phone, setPhone] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<ProvisionedOrgFieldErrors>({});
  /** If false, name changes update slug via {@link suggestSlugFromName}. Set true after load if slug ≠ auto(name), or when user edits slug. */
  const [slugManuallyEdited, setSlugManuallyEdited] = useState(false);
  /** Provisioning: create / update / delete org rows — super_admin only. */
  const [canProvisionOrgs, setCanProvisionOrgs] = useState(true);

  function clearFieldError(key: keyof ProvisionedOrgFieldErrors) {
    setFieldErrors((prev) => {
      if (!prev[key]) return prev;
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const platformRes = await getPlatformAccessPageAccessAction();
      if (cancelled) return;
      setAccessDenied(platformRes.accessDenied);
      if (platformRes.accessDenied) {
        setLoading(false);
        return;
      }

      const [provRes, orgRes] = await Promise.all([
        getNewOrganizationPageAccessAction(),
        getOrganizationForEditAction(organizationId),
      ]);
      if (cancelled) return;
      setCanProvisionOrgs(!provRes.accessDenied);
      if (!orgRes.ok) {
        setLoadError(orgRes.error);
        setLoading(false);
        return;
      }

      const o: OrganizationEditRow = orgRes.organization;
      const planNorm = String(o.plan ?? "").trim().toLowerCase();
      setName(o.name);
      setSlug(o.slug);
      const loadedSlug = String(o.slug ?? "").trim().toLowerCase();
      const autoSlug = suggestSlugFromName(o.name);
      setSlugManuallyEdited(loadedSlug !== autoSlug);
      setPlan(isOrganizationPlanValue(planNorm) ? planNorm : "free");
      const typeNorm = String(o.organization_type ?? "tenant").trim().toLowerCase();
      setOrganizationType(isOrganizationTypeValue(typeNorm) ? typeNorm : "tenant");
      setIsActive(o.is_active);
      setRegistrationNumber(o.registration_number ?? "");
      setCeoName(o.ceo_name ?? "");
      setAddress(o.address ?? "");
      setPhone(o.phone ?? "");
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [organizationId]);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canProvisionOrgs) {
      return;
    }
    setSubmitting(true);
    setError(null);
    setSaved(false);
    setFieldErrors({});

    const validated = validateProvisionedOrganizationFields({
      company_name: name,
      slug,
      plan,
      organization_type: organizationType,
      registration_number: registrationNumber,
      ceo_name: ceoName,
      address,
      phone,
    });

    if (!validated.ok) {
      setFieldErrors(validated.errors);
      setSubmitting(false);
      return;
    }

    const res = await updateProvisionedOrganizationAction(organizationId, {
      company_name: validated.values.company_name,
      slug: validated.values.slug,
      plan: validated.values.plan,
      organization_type: validated.values.organization_type,
      is_active: isActive,
      registration_number: validated.values.registration_number,
      ceo_name: validated.values.ceo_name,
      address: validated.values.address,
      phone: validated.values.phone,
    });
    setSubmitting(false);
    if (!res.ok) {
      if ("fieldErrors" in res) {
        setFieldErrors(res.fieldErrors);
        return;
      }
      setError(res.error);
      return;
    }
    setSaved(true);
    await refreshProfile();
    router.refresh();
  }

  async function onDelete() {
    if (!canProvisionOrgs) {
      return;
    }
    setDeleteError(null);
    const ok = window.confirm(
      `Delete organization "${name}" (${slug})? This cannot be undone.`,
    );
    if (!ok) return;
    setDeleting(true);
    const res = await deleteProvisionedOrganizationAction(organizationId);
    setDeleting(false);
    if (!res.ok) {
      setDeleteError(res.error);
      return;
    }
    await refreshProfile();
    router.push("/platform/organizations");
    router.refresh();
  }

  if (loading) {
    return (
      <div className={responsivePageOuter}>
        <div className={`${responsivePageInner} flex min-h-[40vh] items-center justify-center`}>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
            Loading…
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Edit organization</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in to view this page."
              : "You do not have access to this page. It requires a platform access role (for example system_admin, programmer, or super_admin)."}
          </p>
        </div>
      </div>
    );
  }

  if (loadError) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Edit organization</h1>
          <p className="mt-2 text-sm text-destructive">{loadError}</p>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      <div className={`${responsivePageInner} min-w-0 space-y-6`}>
        <header className="min-w-0">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
            <PageHeaderWithInfo
              className="min-w-0 flex-1 mb-0"
              title="Edit organization"
              titleClassName="text-2xl font-bold tracking-tight text-foreground sm:text-3xl"
              helpPanelClassName="mt-3 max-w-3xl space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm text-muted-foreground"
              infoAriaLabel="About edit organization"
            >
              <p>
                Set company <strong className="font-medium text-foreground">modules and feature entitlements</strong> (licensing) on
                the dedicated page — same data shown as status in{" "}
                <Link className="font-medium text-primary underline" href="/platform/access">
                  Access Management
                </Link>
                .
              </p>
              <p className="max-w-3xl break-words text-sm leading-relaxed sm:max-w-4xl sm:text-base lg:max-w-5xl">
                Internal tool — updates{" "}
                <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
                  public.organizations
                </code>{" "}
                only. Id:{" "}
                <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px] break-all">
                  {organizationId}
                </code>
              </p>
            </PageHeaderWithInfo>
            {organizationId ? (
              <Link
                href={`/platform/organizations/${encodeURIComponent(organizationId)}/modules`}
                className="inline-flex w-full shrink-0 items-center justify-center rounded-lg border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground shadow-sm transition hover:bg-muted/60 sm:w-auto"
              >
                Manage modules
              </Link>
            ) : null}
          </div>
        </header>

        {!canProvisionOrgs ? (
          <div
            className="rounded-xl border border-sky-500/40 bg-sky-500/10 px-4 py-3 text-sm leading-relaxed text-foreground"
            role="status"
          >
            <p className="font-medium">Organization profile is read-only</p>
            <p className="mt-1 text-muted-foreground">
              Only <span className="font-medium text-foreground">super_admin</span> can change
              name, slug, and plan. You can still use <strong>Manage modules</strong> to set
              entitlements.
            </p>
          </div>
        ) : null}

        {error ? (
          <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm leading-relaxed text-destructive">
            {error}
          </div>
        ) : null}

        {deleteError ? (
          <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm leading-relaxed text-destructive">
            {deleteError}
          </div>
        ) : null}

        {saved ? (
          <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-4 text-sm leading-relaxed text-emerald-950 dark:text-emerald-100">
            <div className="flex min-w-0 gap-2">
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
              <div className="min-w-0 space-y-1">
                <p className="font-medium">Organization saved</p>
                <p className="break-words text-muted-foreground dark:text-emerald-200/90">
                  Changes were written to{" "}
                  <code className="inline-block max-w-full break-all rounded bg-emerald-500/20 px-1.5 py-0.5 font-mono text-[11px] text-foreground sm:text-[12px]">
                    organizations
                  </code>
                  .
                </p>
              </div>
            </div>
          </div>
        ) : null}

        <form onSubmit={onSubmit} className="w-full min-w-0">
          <fieldset
            disabled={!canProvisionOrgs}
            className="grid w-full min-w-0 grid-cols-1 gap-5 rounded-2xl border border-border bg-card p-4 shadow-sm sm:p-6 md:gap-6 md:p-8 lg:grid-cols-2 lg:gap-x-8 lg:gap-y-5"
          >
          <div className="min-w-0 lg:col-span-1">
            <label htmlFor="edit-org-name" className="mb-1.5 block text-sm font-medium text-foreground">
              Name{" "}
              <span className="text-destructive" aria-hidden="true">
                *
              </span>
            </label>
            <input
              id="edit-org-name"
              type="text"
              value={name}
              onChange={(ev) => {
                const v = ev.target.value;
                setName(v);
                clearFieldError("company_name");
                if (!slugManuallyEdited) {
                  setSlug(suggestSlugFromName(v));
                }
              }}
              className={inputClass(Boolean(fieldErrors.company_name))}
              autoComplete="organization"
              aria-invalid={fieldErrors.company_name ? true : undefined}
              aria-describedby={fieldErrors.company_name ? "edit-org-name-error" : undefined}
            />
            {fieldErrors.company_name ? (
              <p id="edit-org-name-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.company_name}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-1">
            <label htmlFor="edit-org-slug" className="mb-1.5 block text-sm font-medium text-foreground">
              Slug{" "}
              <span className="text-destructive" aria-hidden="true">
                *
              </span>
            </label>
            <input
              id="edit-org-slug"
              type="text"
              value={slug}
              onChange={(ev) => {
                setSlug(ev.target.value);
                setSlugManuallyEdited(true);
                clearFieldError("slug");
              }}
              className={inputClass(Boolean(fieldErrors.slug))}
              autoComplete="off"
              spellCheck={false}
              aria-invalid={fieldErrors.slug ? true : undefined}
              aria-describedby={fieldErrors.slug ? "edit-org-slug-error" : "edit-org-slug-hint"}
            />
            {fieldErrors.slug ? (
              <p id="edit-org-slug-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.slug}
              </p>
            ) : (
              <p id="edit-org-slug-hint" className="mt-1 text-xs text-muted-foreground">
                When it matches the auto value from name, it updates as you type the name. Edit this
                field to set a custom slug. 3–50 characters: lowercase letters, digits, and hyphens
                only. Must be unique.
              </p>
            )}
          </div>

          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground lg:col-span-2">
            Optional details
          </p>
          <div className="min-w-0 lg:col-span-1">
            <label
              htmlFor="edit-org-registration-number"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Registration Number
            </label>
            <input
              id="edit-org-registration-number"
              type="text"
              value={registrationNumber}
              onChange={(ev) => {
                setRegistrationNumber(ev.target.value);
                clearFieldError("registration_number");
              }}
              className={inputClass(Boolean(fieldErrors.registration_number))}
              autoComplete="off"
              aria-invalid={fieldErrors.registration_number ? true : undefined}
              aria-describedby={
                fieldErrors.registration_number ? "edit-org-registration-number-error" : undefined
              }
            />
            {fieldErrors.registration_number ? (
              <p id="edit-org-registration-number-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.registration_number}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-1">
            <label htmlFor="edit-org-ceo-name" className="mb-1.5 block text-sm font-medium text-foreground">
              CEO Name
            </label>
            <input
              id="edit-org-ceo-name"
              type="text"
              value={ceoName}
              onChange={(ev) => {
                setCeoName(ev.target.value);
                clearFieldError("ceo_name");
              }}
              className={inputClass(Boolean(fieldErrors.ceo_name))}
              autoComplete="name"
              aria-invalid={fieldErrors.ceo_name ? true : undefined}
              aria-describedby={fieldErrors.ceo_name ? "edit-org-ceo-name-error" : undefined}
            />
            {fieldErrors.ceo_name ? (
              <p id="edit-org-ceo-name-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.ceo_name}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-2">
            <label htmlFor="edit-org-address" className="mb-1.5 block text-sm font-medium text-foreground">
              Address
            </label>
            <textarea
              id="edit-org-address"
              value={address}
              onChange={(ev) => {
                setAddress(ev.target.value);
                clearFieldError("address");
              }}
              rows={3}
              className={`${inputClass(Boolean(fieldErrors.address))} min-h-[88px] resize-y py-2`}
              autoComplete="street-address"
              aria-invalid={fieldErrors.address ? true : undefined}
              aria-describedby={fieldErrors.address ? "edit-org-address-error" : undefined}
            />
            {fieldErrors.address ? (
              <p id="edit-org-address-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.address}
              </p>
            ) : null}
          </div>

          <div className="grid grid-cols-1 gap-5 lg:col-span-2 lg:grid-cols-2 lg:gap-x-8 lg:gap-y-0">
            <div className="min-w-0">
              <label htmlFor="edit-org-phone" className="mb-1.5 block text-sm font-medium text-foreground">
                Phone
              </label>
              <input
                id="edit-org-phone"
                type="tel"
                value={phone}
                onChange={(ev) => {
                  setPhone(ev.target.value);
                  clearFieldError("phone");
                }}
                className={inputClass(Boolean(fieldErrors.phone))}
                autoComplete="tel"
                aria-invalid={fieldErrors.phone ? true : undefined}
                aria-describedby={fieldErrors.phone ? "edit-org-phone-error" : undefined}
              />
              {fieldErrors.phone ? (
                <p id="edit-org-phone-error" className="mt-1 text-sm text-destructive">
                  {fieldErrors.phone}
                </p>
              ) : null}
            </div>
            <div className="min-w-0">
              <label htmlFor="edit-org-plan" className="mb-1.5 block text-sm font-medium text-foreground">
                Plan
              </label>
              <select
                id="edit-org-plan"
                value={plan}
                onChange={(ev) => {
                  setPlan(ev.target.value);
                  clearFieldError("plan");
                }}
                className={selectClass(Boolean(fieldErrors.plan))}
                aria-invalid={fieldErrors.plan ? true : undefined}
                aria-describedby={fieldErrors.plan ? "edit-org-plan-error" : "edit-org-plan-hint"}
              >
                <option value="free">Free — Basic usage</option>
                <option value="pro">Pro — Advanced features</option>
                <option value="enterprise">Enterprise — Full access</option>
              </select>
              {fieldErrors.plan ? (
                <p id="edit-org-plan-error" className="mt-1 text-sm text-destructive">
                  {fieldErrors.plan}
                </p>
              ) : (
                <p id="edit-org-plan-hint" className="mt-1 text-xs text-muted-foreground">
                  Select the subscription level for this company.
                </p>
              )}
            </div>
          </div>

          <div className="min-w-0 lg:col-span-2 lg:max-w-md">
            <label
              htmlFor="edit-org-organization-type"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Organization Type
            </label>
            <select
              id="edit-org-organization-type"
              value={organizationType}
              onChange={(ev) => {
                setOrganizationType(ev.target.value);
                clearFieldError("organization_type");
              }}
              className={selectClass(Boolean(fieldErrors.organization_type))}
              aria-invalid={fieldErrors.organization_type ? true : undefined}
              aria-describedby={
                fieldErrors.organization_type
                  ? "edit-org-organization-type-error"
                  : "edit-org-organization-type-hint"
              }
            >
              <option value="tenant">Tenant (Customer company)</option>
              <option value="internal">Internal (Platform company)</option>
            </select>
            {fieldErrors.organization_type ? (
              <p id="edit-org-organization-type-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.organization_type}
              </p>
            ) : (
              <p id="edit-org-organization-type-hint" className="mt-1 text-xs text-muted-foreground">
                Stored as <code className="rounded bg-muted px-1 font-mono text-[11px]">organizations.type</code>.
              </p>
            )}
          </div>

          <div className="flex min-h-11 items-start gap-3 sm:min-h-0 sm:items-center lg:col-span-2">
            <input
              id="edit-org-active"
              type="checkbox"
              checked={isActive}
              onChange={(ev) => setIsActive(ev.target.checked)}
              className="mt-0.5 h-5 w-5 shrink-0 rounded border-input sm:mt-0 sm:h-4 sm:w-4"
            />
            <label
              htmlFor="edit-org-active"
              className="cursor-pointer text-sm font-medium leading-snug text-foreground"
            >
              Is active
            </label>
          </div>
          <div className="pt-2 lg:col-span-2">
            <button
              type="submit"
              disabled={submitting}
              className="inline-flex min-h-11 w-full touch-manipulation items-center justify-center gap-2 rounded-lg bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-violet-700 disabled:opacity-50 sm:min-h-10 sm:w-auto"
            >
              {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              Save changes
            </button>
          </div>
          </fieldset>
        </form>

        {canProvisionOrgs ? (
        <section
          className="rounded-2xl border border-destructive/30 bg-destructive/5 p-4 shadow-sm sm:p-6"
          aria-labelledby="edit-org-delete-heading"
        >
          <h2
            id="edit-org-delete-heading"
            className="text-sm font-semibold text-destructive"
          >
            Delete organization
          </h2>
          <p className="mt-2 text-sm text-muted-foreground">
            Permanently removes this row from{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-[11px]">organizations</code>.
            Fails if other data still references this tenant.
          </p>
          <button
            type="button"
            onClick={() => void onDelete()}
            disabled={deleting || submitting}
            className="mt-4 inline-flex min-h-10 items-center justify-center gap-2 rounded-lg border border-destructive/60 bg-background px-4 py-2 text-sm font-semibold text-destructive shadow-sm hover:bg-destructive/10 disabled:opacity-50"
          >
            {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
            Delete organization
          </button>
        </section>
        ) : null}
      </div>
    </div>
  );
}
