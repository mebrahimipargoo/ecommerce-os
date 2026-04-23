"use client";

import React, { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { useRouter } from "next/navigation";
import {
  createProvisionedOrganizationAction,
  getNewOrganizationPageAccessAction,
} from "../create-organization-actions";
import {
  type ProvisionedOrgFieldErrors,
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

export default function NewOrganizationPage() {
  const router = useRouter();
  const { refreshProfile } = useUserRole();
  const [loading, setLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [companyName, setCompanyName] = useState("");
  const [slug, setSlug] = useState("");
  /** Once the user edits the slug field, stop auto-syncing from company name. */
  const [slugManuallyEdited, setSlugManuallyEdited] = useState(false);
  const [plan, setPlan] = useState("free");
  const [organizationType, setOrganizationType] = useState("tenant");
  const [isActive, setIsActive] = useState(true);
  const [registrationNumber, setRegistrationNumber] = useState("");
  const [ceoName, setCeoName] = useState("");
  const [address, setAddress] = useState("");
  const [phone, setPhone] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState<ProvisionedOrgFieldErrors>({});

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
      const res = await getNewOrganizationPageAccessAction();
      if (cancelled) return;
      setAccessDenied(res.accessDenied);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    setFieldErrors({});

    const validated = validateProvisionedOrganizationFields({
      company_name: companyName,
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

    const res = await createProvisionedOrganizationAction({
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
    await refreshProfile();
    router.push(`/platform/organizations/${res.organizationId}`);
  }

  if (loading) {
    return (
      <div className={responsivePageOuter}>
        <div className={`${responsivePageInner} flex min-h-[40vh] items-center justify-center`}>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
            Checking access…
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">New organization</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in to view this page."
              : "This page is restricted to super_admin only."}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      <div className={`${responsivePageInner} min-w-0 space-y-6`}>
        <PageHeaderWithInfo
          className="min-w-0"
          title="Provision organization"
          titleClassName="text-2xl font-bold tracking-tight text-foreground sm:text-3xl"
          helpPanelClassName="mt-3 max-w-3xl space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm leading-relaxed text-muted-foreground break-words sm:max-w-4xl sm:text-base lg:max-w-5xl"
          infoAriaLabel="About provision organization"
        >
          <p>
            Internal tool — creates a row in{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
              organizations
            </code>{" "}
            and default{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
              organization_settings
            </code>
            . Does not create users or invitations.
          </p>
        </PageHeaderWithInfo>

        {error ? (
          <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm leading-relaxed text-destructive">
            {error}
          </div>
        ) : null}

        <form
          onSubmit={onSubmit}
          className="grid w-full min-w-0 grid-cols-1 gap-5 rounded-2xl border border-border bg-card p-4 shadow-sm sm:p-6 md:gap-6 md:p-8 lg:grid-cols-2 lg:gap-x-8 lg:gap-y-5"
        >
          <div className="min-w-0 lg:col-span-1">
            <label
              htmlFor="org-company-name"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Company name{" "}
              <span className="text-destructive" aria-hidden="true">
                *
              </span>
            </label>
            <input
              id="org-company-name"
              type="text"
              value={companyName}
              onChange={(ev) => {
                const v = ev.target.value;
                setCompanyName(v);
                clearFieldError("company_name");
                if (!slugManuallyEdited) {
                  setSlug(suggestSlugFromName(v));
                }
              }}
              className={inputClass(Boolean(fieldErrors.company_name))}
              autoComplete="organization"
              aria-invalid={fieldErrors.company_name ? true : undefined}
              aria-describedby={fieldErrors.company_name ? "org-company-name-error" : undefined}
            />
            {fieldErrors.company_name ? (
              <p id="org-company-name-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.company_name}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-1">
            <label
              htmlFor="org-slug"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Slug{" "}
              <span className="text-destructive" aria-hidden="true">
                *
              </span>
            </label>
            <input
              id="org-slug"
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
              aria-describedby={
                fieldErrors.slug ? "org-slug-error" : "org-slug-hint"
              }
            />
            {fieldErrors.slug ? (
              <p id="org-slug-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.slug}
              </p>
            ) : (
              <p id="org-slug-hint" className="mt-1 text-xs text-muted-foreground">
                Filled from company name until you edit this field. 3–50 characters: lowercase letters,
                digits, and hyphens only. Must be unique.
              </p>
            )}
          </div>

          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground lg:col-span-2">
            Optional details
          </p>
          <div className="min-w-0 lg:col-span-1">
            <label
              htmlFor="org-registration-number"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Registration Number
            </label>
            <input
              id="org-registration-number"
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
                fieldErrors.registration_number ? "org-registration-number-error" : undefined
              }
            />
            {fieldErrors.registration_number ? (
              <p id="org-registration-number-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.registration_number}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-1">
            <label htmlFor="org-ceo-name" className="mb-1.5 block text-sm font-medium text-foreground">
              CEO Name
            </label>
            <input
              id="org-ceo-name"
              type="text"
              value={ceoName}
              onChange={(ev) => {
                setCeoName(ev.target.value);
                clearFieldError("ceo_name");
              }}
              className={inputClass(Boolean(fieldErrors.ceo_name))}
              autoComplete="name"
              aria-invalid={fieldErrors.ceo_name ? true : undefined}
              aria-describedby={fieldErrors.ceo_name ? "org-ceo-name-error" : undefined}
            />
            {fieldErrors.ceo_name ? (
              <p id="org-ceo-name-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.ceo_name}
              </p>
            ) : null}
          </div>
          <div className="min-w-0 lg:col-span-2">
            <label htmlFor="org-address" className="mb-1.5 block text-sm font-medium text-foreground">
              Address
            </label>
            <textarea
              id="org-address"
              value={address}
              onChange={(ev) => {
                setAddress(ev.target.value);
                clearFieldError("address");
              }}
              rows={3}
              className={`${inputClass(Boolean(fieldErrors.address))} min-h-[88px] resize-y py-2`}
              autoComplete="street-address"
              aria-invalid={fieldErrors.address ? true : undefined}
              aria-describedby={fieldErrors.address ? "org-address-error" : undefined}
            />
            {fieldErrors.address ? (
              <p id="org-address-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.address}
              </p>
            ) : null}
          </div>

          <div className="grid grid-cols-1 gap-5 lg:col-span-2 lg:grid-cols-2 lg:gap-x-8 lg:gap-y-0">
            <div className="min-w-0">
              <label htmlFor="org-phone" className="mb-1.5 block text-sm font-medium text-foreground">
                Phone
              </label>
              <input
                id="org-phone"
                type="tel"
                value={phone}
                onChange={(ev) => {
                  setPhone(ev.target.value);
                  clearFieldError("phone");
                }}
                className={inputClass(Boolean(fieldErrors.phone))}
                autoComplete="tel"
                aria-invalid={fieldErrors.phone ? true : undefined}
                aria-describedby={fieldErrors.phone ? "org-phone-error" : undefined}
              />
              {fieldErrors.phone ? (
                <p id="org-phone-error" className="mt-1 text-sm text-destructive">
                  {fieldErrors.phone}
                </p>
              ) : null}
            </div>
            <div className="min-w-0">
              <label htmlFor="org-plan" className="mb-1.5 block text-sm font-medium text-foreground">
                Plan
              </label>
              <select
                id="org-plan"
                value={plan}
                onChange={(ev) => {
                  setPlan(ev.target.value);
                  clearFieldError("plan");
                }}
                className={selectClass(Boolean(fieldErrors.plan))}
                aria-invalid={fieldErrors.plan ? true : undefined}
                aria-describedby={fieldErrors.plan ? "org-plan-error" : "org-plan-hint"}
              >
                <option value="free">Free — Basic usage</option>
                <option value="pro">Pro — Advanced features</option>
                <option value="enterprise">Enterprise — Full access</option>
              </select>
              {fieldErrors.plan ? (
                <p id="org-plan-error" className="mt-1 text-sm text-destructive">
                  {fieldErrors.plan}
                </p>
              ) : (
                <p id="org-plan-hint" className="mt-1 text-xs text-muted-foreground">
                  Select the subscription level for this company. This can control feature access later.
                </p>
              )}
            </div>
          </div>

          <div className="min-w-0 lg:col-span-2 lg:max-w-md">
            <label
              htmlFor="org-organization-type"
              className="mb-1.5 block text-sm font-medium text-foreground"
            >
              Organization Type
            </label>
            <select
              id="org-organization-type"
              value={organizationType}
              onChange={(ev) => {
                setOrganizationType(ev.target.value);
                clearFieldError("organization_type");
              }}
              className={selectClass(Boolean(fieldErrors.organization_type))}
              aria-invalid={fieldErrors.organization_type ? true : undefined}
              aria-describedby={
                fieldErrors.organization_type
                  ? "org-organization-type-error"
                  : "org-organization-type-hint"
              }
            >
              <option value="tenant">Tenant (Customer company)</option>
              <option value="internal">Internal (Platform company)</option>
            </select>
            {fieldErrors.organization_type ? (
              <p id="org-organization-type-error" className="mt-1 text-sm text-destructive">
                {fieldErrors.organization_type}
              </p>
            ) : (
              <p id="org-organization-type-hint" className="mt-1 text-xs text-muted-foreground">
                Stored as <code className="rounded bg-muted px-1 font-mono text-[11px]">organizations.type</code>.
              </p>
            )}
          </div>

          <div className="flex min-h-11 items-start gap-3 sm:min-h-0 sm:items-center lg:col-span-2">
            <input
              id="org-active"
              type="checkbox"
              checked={isActive}
              onChange={(ev) => setIsActive(ev.target.checked)}
              className="mt-0.5 h-5 w-5 shrink-0 rounded border-input sm:mt-0 sm:h-4 sm:w-4"
            />
            <label
              htmlFor="org-active"
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
              Create organization
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
