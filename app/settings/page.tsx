"use client";

import React, { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, BadgeCheck, BarChart3, Building2, CheckCircle2, CreditCard, Cpu, Crown,
  Globe, HardDrive, ImageIcon, KeyRound, Loader2, Package, PackageX, Pencil, Plus, Printer,
  RefreshCw, RotateCcw, Save, ScanLine, Settings, ShieldAlert, ShieldCheck, Store, Tag, Trash2,
  Truck, TriangleAlert, UserCog, Users, Wifi, X, Zap,
} from "lucide-react";
import {
  getClaimAgentConfig,
  getCoreSettings,
  getFefoSettings,
  saveClaimAgentConfig,
  saveInventoryFefoSettings,
  saveCoreSettings,
} from "./workspace-settings-actions";
import { BRAND_LOGO_IMG_CLASSNAME } from "../../lib/brand-logo-classes";
import { uploadOrganizationLogoAction } from "./upload-organization-logo-action";
import { AgentApiKeysSection } from "./AgentApiKeysSection";
import { RoleTagCombobox } from "./RoleTagCombobox";
import { upsertProviderApiKey, getProviderApiKey } from "./organization-api-keys-actions";
import {
  DEFAULT_CLAIM_AGENT_CONFIG,
  DEFAULT_FEFO,
  type ClaimAgentConfig,
  type CoreSettings,
  type InventoryModuleConfig,
} from "./workspace-settings-types";
import {
  clearAIUnifiedKeyFromStorage,
  clearGeminiApiKeyFromStorage,
  clearOpenAIApiKeyFromStorage,
  DEFAULT_BASE_URLS,
  getAIConfigsFromStorage,
  getAIRoleAssignmentsFromStorage,
  getBarcodeModeFromStorage,
  getLabelPrinterFromStorage,
  setAIConfigsInStorage,
  setAIRoleAssignmentsInStorage,
  setBarcodeModeInStorage,
  setLabelPrinterInStorage,
  getDefaultStoreIdFromStorage,
  setDefaultStoreIdInStorage,
  normalizeAIRoleTag,
  type AIConfig,
  type AIConfigStatus,
  type AIProvider,
  type AIRoleAssignments,
  type BarcodeMode,
  type LabelPrinter,
} from "../../lib/openai-settings";
import { useBranding } from "../../components/BrandingContext";
import { isAdminRole, useUserRole } from "../../components/UserRoleContext";
import { FALLBACK_ORGANIZATION_ID } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";
import { DatabaseTag } from "../../components/DatabaseTag";
import type { AdapterProviderKey } from "../../lib/adapters";
import {
  listMarketplaces, listStores, insertStore, insertMarketplace,
  updateMarketplace, testConnection, deleteStore, updateStore,
  getMarketplaceCredentialsForEdit,
  testMarketplaceCredentials,
  type RbacContext,
  type StorePublicRow,
} from "./adapters/actions";
import { getClaimQueueSyncStatus, syncClaimQueueNow } from "../claim-engine/logistics-sync-actions";
import {
  CLAIM_EVIDENCE_KEY_LABELS,
  type ClaimEvidenceKey,
  mergeDefaultClaimEvidence,
  type DefaultClaimEvidence,
} from "../claim-engine/claim-evidence-settings";
import {
  getOrganizationClaimEvidenceDefaults,
  saveOrganizationClaimEvidenceDefaults,
} from "./organization-claim-evidence-actions";
import {
  getOrganizationDefaultStoreId,
  saveOrganizationDefaultStoreId,
} from "./organization-default-store-actions";

// ─── Types & Constants ────────────────────────────────────────────────────────

type ToastState = { msg: string; ok: boolean } | null;
type TabId =
  // ── System & Workspace ──────────────────────────────────────────────────────
  | "general"
  | "team"
  | "billing"
  // ── Infrastructure ──────────────────────────────────────────────────────────
  | "marketplaces"
  | "ai_quotas"
  | "hardware"
  // ── Business Modules ────────────────────────────────────────────────────────
  | "returns_processing"
  | "inventory_fefo"
  | "claim_engine"
  | "reports_analytics";

type StoreRecord = {
  id: string;
  provider: string;
  nickname: string;
  display_id?: string;
  organization_id: string;
  role_required: string;
  created_at: string;
};

const PLATFORM_LABELS: Record<string, string> = {
  amazon:  "Amazon",
  walmart: "Walmart",
  ebay:    "eBay",
  target:  "Target",
  shopify: "Shopify",
  custom:  "Custom",
};

// ─── Marketplace credential field definitions ─────────────────────────────────

type CredField = {
  key: string; label: string; type: "text" | "password";
  placeholder: string; credKey: string;
};

const PLATFORM_CRED_FIELDS: Record<string, CredField[]> = {
  /** Amazon SP-API uses the dedicated form block below (all keys live in `marketplaces.credentials`). */
  amazon: [],
  walmart: [
    { key: "clientId",     label: "Client ID",     type: "text",     placeholder: "Walmart API Client ID", credKey: "client_id"    },
    { key: "clientSecret", label: "Client Secret", type: "password", placeholder: "Paste client secret",   credKey: "client_secret" },
  ],
  ebay: [
    { key: "appId",  label: "App ID",  type: "text", placeholder: "eBay App ID",  credKey: "app_id"  },
    { key: "certId", label: "Cert ID", type: "text", placeholder: "eBay Cert ID", credKey: "cert_id" },
    { key: "devId",  label: "Dev ID",  type: "text", placeholder: "eBay Dev ID",  credKey: "dev_id"  },
  ],
  shopify: [
    { key: "shopDomain",  label: "Shop Domain",   type: "text",     placeholder: "your-store.myshopify.com", credKey: "shop_domain"  },
    { key: "accessToken", label: "Access Token",  type: "password", placeholder: "shpat_…",                  credKey: "access_token" },
  ],
  custom: [
    { key: "apiUrl", label: "API URL", type: "text",     placeholder: "https://api.example.com", credKey: "api_url" },
    { key: "apiKey", label: "API Key", type: "password", placeholder: "Your API key",            credKey: "api_key" },
  ],
};

const AMAZON_SP_API_DEFAULTS = {
  region: "us-east-1",
  endpoint: "https://sellingpartnerapi-na.amazon.com",
} as const;

const AMAZON_CREDENTIAL_ROWS: {
  key: string;
  label: string;
  type: "text" | "password";
  placeholder: string;
}[] = [
  { key: "seller_id", label: "Seller ID (Merchant Token)", type: "text", placeholder: "e.g. A1BCDEFGHIJKL" },
  { key: "marketplace_id", label: "Marketplace ID", type: "text", placeholder: "e.g. ATVPDKIKX0DER" },
  { key: "lwa_client_id", label: "LWA Client ID", type: "text", placeholder: "amzn1.application-oa2-client…" },
  { key: "lwa_client_secret", label: "LWA Client Secret", type: "password", placeholder: "Paste client secret" },
  { key: "aws_access_key", label: "AWS Access Key", type: "text", placeholder: "AKIA…" },
  { key: "aws_secret_key", label: "AWS Secret Key", type: "password", placeholder: "••••••••" },
  { key: "refresh_token", label: "Refresh Token", type: "password", placeholder: "Atzr|…" },
  { key: "region", label: "Region", type: "text", placeholder: "us-east-1" },
  { key: "endpoint", label: "SP-API Endpoint", type: "text", placeholder: "https://sellingpartnerapi-na.amazon.com" },
];

const PLATFORM_TO_PROVIDER: Record<string, string | null> = {
  amazon:  "amazon_sp_api",
  walmart: "walmart_api",
  ebay:    "ebay_api",
  target:  null,
  shopify: null,
  custom:  null,
};

const PLANS = ["Free Tier", "Pro Tier", "Enterprise"] as const;
type SaasPlan = typeof PLANS[number];

const PLAN_LIMITS: Record<SaasPlan, { ai_calls: number; stores: number; items: number }> = {
  "Free Tier":  { ai_calls: 500,      stores: 1,        items: 2_000   },
  "Pro Tier":   { ai_calls: 5_000,    stores: 5,        items: 20_000  },
  "Enterprise": { ai_calls: Infinity, stores: Infinity, items: Infinity },
};

const MOCK_AI_CALLS      = 450;
const MOCK_SCANNED_ITEMS = 1_247;

const SELECT_CLS =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const INPUT_CLS =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL_CLS = "mb-2 block text-sm font-medium leading-none";
const HINT_CLS  = "mb-2 text-xs text-muted-foreground";

/** Reserves hint height so inputs do not shift when helper copy or warnings appear. */
function StableField({
  label,
  hint,
  children,
}: {
  label: React.ReactNode;
  hint?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-0">
      <div className="mb-2 block text-sm font-medium leading-none">{label}</div>
      <div className="min-h-[2.5rem] text-xs text-muted-foreground">
        {hint ?? <span className="invisible select-none">.</span>}
      </div>
      {children}
    </div>
  );
}

const PROVIDER_LABELS: Record<AIProvider, string> = {
  openai: "OpenAI",
  gemini: "Google Gemini",
  custom: "Custom / Other",
};

const STATUS_META: Record<AIConfigStatus, { label: string; cls: string }> = {
  untested: { label: "Untested",  cls: "border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400" },
  active:   { label: "Active ✓", cls: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300" },
  testing:  { label: "Testing…",  cls: "border-amber-200 bg-amber-50 text-amber-600 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  error:    { label: "Error",     cls: "border-rose-200 bg-rose-50 text-rose-600 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400" },
};

// ─── Grouped sidebar navigation ───────────────────────────────────────────────

type NavGroup = {
  label: string;
  items: { id: TabId; label: string; icon: React.ReactNode; proOnly?: boolean }[];
};

const NAV_GROUPS: NavGroup[] = [
  {
    label: "SYSTEM & WORKSPACE",
    items: [
      { id: "general",  label: "General & White-label", icon: <Building2  className="h-4 w-4" /> },
      { id: "team",     label: "Team & Roles",          icon: <Users      className="h-4 w-4" /> },
      { id: "billing",  label: "Billing & Subscription", icon: <CreditCard className="h-4 w-4" /> },
    ],
  },
  {
    label: "INFRASTRUCTURE",
    items: [
      { id: "marketplaces", label: "Marketplaces & Stores", icon: <Store     className="h-4 w-4" /> },
      { id: "ai_quotas",    label: "AI & OCR Engine",       icon: <Cpu       className="h-4 w-4" /> },
      { id: "hardware",     label: "Hardware Scanners",     icon: <HardDrive className="h-4 w-4" /> },
    ],
  },
  {
    label: "BUSINESS MODULES",
    items: [
      { id: "returns_processing", label: "Returns Processing",  icon: <RotateCcw   className="h-4 w-4" />, proOnly: true },
      { id: "inventory_fefo",     label: "Inventory & FEFO",    icon: <Package     className="h-4 w-4" />, proOnly: true },
      { id: "claim_engine",       label: "Claim Engine",        icon: <ShieldCheck className="h-4 w-4" />, proOnly: true },
      { id: "reports_analytics",  label: "Reports & Analytics", icon: <BarChart3   className="h-4 w-4" />, proOnly: true },
    ],
  },
];

function newBlankConfig(): Omit<AIConfig, "id"> {
  return {
    providerName:     "",
    provider:         "openai",
    baseURL:          DEFAULT_BASE_URLS.openai,
    apiKey:           "",
    role:             "default",
    status:           "untested",
    isGlobalOverride: false,
  };
}

// ─── UsageMeter sub-component ─────────────────────────────────────────────────

function UsageMeter({
  label, used, limit, color = "sky", hint,
}: {
  label: string;
  used: number;
  limit: number;
  color?: "violet" | "sky" | "emerald";
  hint?: string;
}) {
  const isUnlimited = limit === Infinity;
  const pct = isUnlimited ? 100 : Math.min((used / limit) * 100, 100);
  const isNearLimit = !isUnlimited && pct >= 80;
  const isAtLimit   = !isUnlimited && pct >= 100;

  const barCls = isAtLimit
    ? "bg-rose-500"
    : isNearLimit
    ? "bg-amber-500"
    : color === "violet" ? "bg-violet-500"
    : color === "emerald" ? "bg-emerald-500"
    : "bg-sky-500";

  const trackCls =
    color === "violet"  ? "bg-violet-100 dark:bg-violet-900/30"
    : color === "emerald" ? "bg-emerald-100 dark:bg-emerald-900/30"
    : "bg-sky-100 dark:bg-sky-900/30";

  const countCls = isAtLimit
    ? "text-rose-600 dark:text-rose-400"
    : isNearLimit
    ? "text-amber-600 dark:text-amber-400"
    : color === "violet"  ? "text-violet-700 dark:text-violet-300"
    : color === "emerald" ? "text-emerald-700 dark:text-emerald-300"
    : "text-sky-700 dark:text-sky-300";

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-4">
        <div className="min-w-0">
          <p className="text-sm font-semibold">{label}</p>
          {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
        </div>
        <span className={`shrink-0 text-sm font-bold tabular-nums ${countCls}`}>
          {used.toLocaleString()}{isUnlimited ? " / ∞" : ` / ${limit.toLocaleString()}`}
        </span>
      </div>
      <div className={`h-3 w-full overflow-hidden rounded-full ${trackCls}`}>
        <div
          className={`h-3 rounded-full transition-all duration-700 ${barCls} ${isUnlimited ? "opacity-30" : ""}`}
          style={{ width: isUnlimited ? "100%" : `${pct}%` }}
        />
      </div>
      {isAtLimit && !isUnlimited && (
        <p className="text-xs font-medium text-rose-600 dark:text-rose-400">
          Limit reached — upgrade your plan to continue.
        </p>
      )}
      {isNearLimit && !isAtLimit && (
        <p className="text-xs font-medium text-amber-600 dark:text-amber-400">
          {Math.round(pct)}% of quota used — consider upgrading soon.
        </p>
      )}
    </div>
  );
}

// ─── Main Page ────────────────────────────────────────────────────────────────

export default function SettingsPage() {
  const { role, organizationId, actorUserId } = useUserRole();
  const { refresh: refreshBranding } = useBranding();

  const tenantCtx = useMemo(
    () => ({ actorProfileId: actorUserId, organizationId }),
    [actorUserId, organizationId],
  );

  const settingsRbac = useMemo((): RbacContext => {
    const cid =
      organizationId?.trim() && isUuidString(organizationId.trim())
        ? organizationId.trim()
        : FALLBACK_ORGANIZATION_ID;
    return {
      organization_id: cid,
      /** Settings route is admin-only; adapter RBAC hierarchy uses admin/editor/viewer. */
      user_role: "admin",
    };
  }, [organizationId]);

  const [activeTab, setActiveTab] = useState<TabId>("general");
  const [mounted,   setMounted]   = useState(false);

  // ── General Preferences ────────────────────────────────────────────────────
  const [defaultStoreId,  setDefaultStoreId]  = useState<string>("");
  const [storesList,      setStoresList]      = useState<StorePublicRow[]>([]);
  const [storesListLoading, setStoresListLoading] = useState(false);

  // ── White-label / Tenant Customization (core_settings JSONB) ───────────────
  const [companyName,         setCompanyName]         = useState<string>("");
  const [companyLogoUrl,      setCompanyLogoUrl]      = useState<string>("");
  const [logoUploading,       setLogoUploading]       = useState(false);
  const [coreSettingsLoading, setCoreSettingsLoading] = useState(false);
  const [coreSettingsSaving,  setCoreSettingsSaving]  = useState(false);

  // ── AI configs ─────────────────────────────────────────────────────────────
  const [configs,    setConfigs]    = useState<AIConfig[]>([]);
  const [showForm,   setShowForm]   = useState(false);
  const [formData,   setFormData]   = useState<Omit<AIConfig, "id">>(newBlankConfig());
  const [testingId,  setTestingId]  = useState<string | null>(null);
  const [savingForm, setSavingForm] = useState(false);

  // ── Role assignments ───────────────────────────────────────────────────────
  const [assignments, setAssignments] = useState<AIRoleAssignments>({
    defaultGeneral: null,
    defaultVision:  null,
  });

  // ── Hardware ───────────────────────────────────────────────────────────────
  const [barcodeMode,  setBarcodeMode]  = useState<BarcodeMode>("physical");
  const [labelPrinter, setLabelPrinter] = useState<LabelPrinter>("system");
  const [hwSaved,      setHwSaved]      = useState(false);

  // ── Stores ─────────────────────────────────────────────────────────────────
  const [connections,   setConnections]   = useState<StoreRecord[]>([]);
  const [storesLoading, setStoresLoading] = useState(false);

  // ── Add Store Modal ────────────────────────────────────────────────────────
  const [showAddStoreModal,   setShowAddStoreModal]   = useState(false);
  const [editingStoreId,      setEditingStoreId]      = useState<string | null>(null);
  const [newStoreName,        setNewStoreName]        = useState("");
  const [newStorePlatform,    setNewStorePlatform]    = useState("amazon");
  const [newStoreRegion,      setNewStoreRegion]      = useState("US");
  const [newStoreCredentials, setNewStoreCredentials] = useState<Record<string, string>>({});
  const [addStoreSaving,      setAddStoreSaving]      = useState(false);
  const [storeModalCredLoading, setStoreModalCredLoading] = useState(false);
  const [storeModalTestLoading, setStoreModalTestLoading] = useState(false);
  const [storeTestStatus, setStoreTestStatus] = useState<Record<string, "idle" | "testing" | "ok" | "error">>({});
  const [deletingStoreId, setDeletingStoreId] = useState<string | null>(null);

  // ── Billing / SaaS plan ────────────────────────────────────────────────────
  const [mockPlan, setMockPlan] = useState<SaasPlan>("Free Tier");

  // ── Inventory / FEFO module settings ──────────────────────────────────────
  const [fefoSettings,   setFefoSettings]   = useState<InventoryModuleConfig>(DEFAULT_FEFO);
  const [fefoLoading,    setFefoLoading]    = useState(false);
  const [fefoSaving,     setFefoSaving]     = useState(false);
  const [fefoLocalEdit,  setFefoLocalEdit]  = useState<InventoryModuleConfig>(DEFAULT_FEFO);

  const [claimAgentLocal, setClaimAgentLocal] = useState<ClaimAgentConfig>(DEFAULT_CLAIM_AGENT_CONFIG);
  const [claimAgentSaved, setClaimAgentSaved] = useState<ClaimAgentConfig>(DEFAULT_CLAIM_AGENT_CONFIG);
  const [claimAgentLoading, setClaimAgentLoading] = useState(false);
  const [claimAgentSaving, setClaimAgentSaving] = useState(false);

  const [claimEvidenceLocal, setClaimEvidenceLocal] = useState<Record<ClaimEvidenceKey, boolean>>(() =>
    mergeDefaultClaimEvidence(null),
  );
  const [claimEvidenceLoading, setClaimEvidenceLoading] = useState(false);
  const [claimEvidenceSaving, setClaimEvidenceSaving] = useState(false);

  const [logisticsSyncStatus, setLogisticsSyncStatus] = useState<{
    pendingSyncCount: number;
    systemUpToDate: boolean;
    readyForClaimCount?: number;
  } | null>(null);
  const [logisticsSyncLoading, setLogisticsSyncLoading] = useState(false);
  const [logisticsSyncBusy, setLogisticsSyncBusy] = useState(false);

  const [toast, setToast] = useState<ToastState>(null);

  function closeAddConnectionModal() {
    setShowForm(false);
    setFormData(newBlankConfig());
  }

  useEffect(() => {
    if (!showForm) return;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setShowForm(false);
        setFormData(newBlankConfig());
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      document.body.style.overflow = prevOverflow;
      window.removeEventListener("keydown", onKey);
    };
  }, [showForm]);

  // ── Hydrate from localStorage + DB ────────────────────────────────────────
  useEffect(() => {
    setMounted(true);
    const localConfigs = getAIConfigsFromStorage();
    setConfigs(localConfigs);
    setAssignments(getAIRoleAssignmentsFromStorage());
    setBarcodeMode(getBarcodeModeFromStorage());
    setLabelPrinter(getLabelPrinterFromStorage());
    const saved = localStorage.getItem("mock_saas_plan") as SaasPlan | null;
    if (saved && (PLANS as readonly string[]).includes(saved)) setMockPlan(saved as SaasPlan);

    // Merge OpenAI key from DB — enables cross-device access
    void getProviderApiKey("OpenAI").then((dbKey) => {
      if (!dbKey) return;
      setConfigs((prev) => {
        const hasOpenAI = prev.some((c) => c.provider === "openai" && c.apiKey.trim());
        if (hasOpenAI) return prev; // Local key already present — don't overwrite
        const merged: AIConfig[] = [
          ...prev,
          {
            id: `cfg_db_openai_${Date.now()}`,
            providerName: "OpenAI",
            provider: "openai" as AIProvider,
            baseURL: DEFAULT_BASE_URLS.openai,
            apiKey: dbKey,
            role: "default",
            status: "untested" as AIConfigStatus,
          },
        ];
        setAIConfigsInStorage(merged);
        return merged;
      });
    });
  }, []);

  // ── Default store: organization_settings.default_store_id is canonical; localStorage is fallback ─
  useEffect(() => {
    if (!mounted) return;
    let cancelled = false;
    void getOrganizationDefaultStoreId(tenantCtx).then((serverId) => {
      if (cancelled) return;
      if (serverId) {
        setDefaultStoreId(serverId);
        setDefaultStoreIdInStorage(serverId);
      } else {
        setDefaultStoreId(getDefaultStoreIdFromStorage());
      }
    });
    return () => {
      cancelled = true;
    };
  }, [mounted, tenantCtx]);

  // ── Load core_settings (white-label) from DB ──────────────────────────────
  useEffect(() => {
    if (!mounted) return;
    async function loadCoreSettings() {
      setCoreSettingsLoading(true);
      try {
        const cfg = await getCoreSettings(organizationId ?? undefined);
        setCompanyName(cfg.company_name ?? "");
        setCompanyLogoUrl(cfg.company_logo_url ?? "");
      } finally {
        setCoreSettingsLoading(false);
      }
    }
    loadCoreSettings();
  }, [mounted, organizationId]);

  // ── Load FEFO settings from DB ─────────────────────────────────────────────
  useEffect(() => {
    if (!mounted) return;
    async function loadFefoSettings() {
      setFefoLoading(true);
      try {
        const cfg = await getFefoSettings();
        setFefoSettings(cfg);
        setFefoLocalEdit(cfg);
      } finally {
        setFefoLoading(false);
      }
    }
    loadFefoSettings();
  }, [mounted]);

  // ── Claim Engine / Agent Control (module_configs.claim_agent_config) ───────
  useEffect(() => {
    if (!mounted) return;
    async function loadClaimAgent() {
      setClaimAgentLoading(true);
      try {
        const cfg = await getClaimAgentConfig();
        setClaimAgentSaved(cfg);
        setClaimAgentLocal(cfg);
      } finally {
        setClaimAgentLoading(false);
      }
    }
    loadClaimAgent();
  }, [mounted]);

  // ── Claim queue sync status (Logistics AI Agent) ──────────────────────────
  useEffect(() => {
    if (!mounted || activeTab !== "claim_engine") return;
    let cancelled = false;
    setLogisticsSyncLoading(true);
    getClaimQueueSyncStatus()
      .then((r) => {
        if (cancelled || !r.ok) return;
        setLogisticsSyncStatus({
          pendingSyncCount: r.pendingSyncCount ?? 0,
          systemUpToDate: r.systemUpToDate ?? true,
          readyForClaimCount: r.readyForClaimCount,
        });
      })
      .finally(() => {
        if (!cancelled) setLogisticsSyncLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [mounted, activeTab]);

  // ── Default claim evidence (organization_settings.default_claim_evidence) ─
  useEffect(() => {
    if (!mounted || activeTab !== "claim_engine") return;
    let cancelled = false;
    setClaimEvidenceLoading(true);
    getOrganizationClaimEvidenceDefaults(tenantCtx)
      .then((r) => {
        if (cancelled) return;
        setClaimEvidenceLocal(r);
      })
      .finally(() => {
        if (!cancelled) setClaimEvidenceLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [mounted, activeTab, tenantCtx]);

  // ── Load connected marketplace credentials ─────────────────────────────────
  useEffect(() => {
    if (!mounted) return;
    let cancelled = false;
    setStoresLoading(true);
    listMarketplaces(settingsRbac)
      .then((res) => {
        if (cancelled) return;
        if (res.ok && res.data) setConnections(res.data as StoreRecord[]);
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setStoresLoading(false); });
    return () => { cancelled = true; };
  }, [mounted, settingsRbac]);

  // ── Load stores (stores table) for Default Store selector ─────────────────
  useEffect(() => {
    if (!mounted) return;
    let cancelled = false;
    setStoresListLoading(true);
    listStores(settingsRbac)
      .then((res) => {
        if (cancelled) return;
        if (!res.ok || !res.data) {
          console.error("Store error:", res.ok === false ? res.error : "No data");
          return;
        }
        console.log("Fetched stores:", res.data);
        setStoresList(res.data);
      })
      .catch((e) => {
        console.error("Store error:", e);
      })
      .finally(() => { if (!cancelled) setStoresListLoading(false); });
    return () => { cancelled = true; };
  }, [mounted, settingsRbac]);

  function showToast(msg: string, ok: boolean) {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4500);
  }

  // ── Paywall helpers ────────────────────────────────────────────────────────
  const planLimits  = PLAN_LIMITS[mockPlan];
  const canAddStore = planLimits.stores === Infinity || storesList.length < planLimits.stores;

  // ── General save ───────────────────────────────────────────────────────────
  async function handleSaveGeneral(e: React.FormEvent) {
    e.preventDefault();
    const res = await saveOrganizationDefaultStoreId(defaultStoreId.trim() || null, tenantCtx);
    if (!res.ok) {
      showToast(res.error ?? "Failed to save default store.", false);
      return;
    }
    setDefaultStoreIdInStorage(defaultStoreId);
    void refreshBranding();
    showToast("General preferences saved.", true);
  }

  // ── Logo file upload → server action (logos bucket + organization_settings) ─
  async function handleLogoFileUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setLogoUploading(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      if (actorUserId) fd.append("actor_profile_id", actorUserId);
      if (organizationId?.trim()) fd.append("organization_id", organizationId.trim());
      const res = await uploadOrganizationLogoAction(fd);
      if (!res.ok) throw new Error(res.error ?? "Logo upload failed.");
      setCompanyLogoUrl(res.publicUrl);
      const nameRes = await saveCoreSettings({ company_name: companyName.trim() }, tenantCtx);
      if (!nameRes.ok) throw new Error(nameRes.error ?? "Failed to save workspace name.");
      void refreshBranding();
      showToast("Logo uploaded and saved.", true);
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Logo upload failed.", false);
    } finally {
      setLogoUploading(false);
    }
  }

  // ── White-label / Tenant save ──────────────────────────────────────────────
  async function handleSaveWhitelabel(e: React.FormEvent) {
    e.preventDefault();
    setCoreSettingsSaving(true);
    const url = companyLogoUrl.trim();
    const res = await saveCoreSettings({
      company_name:     companyName.trim(),
      company_logo_url: url,
      logo_url:         url,
    }, tenantCtx);
    setCoreSettingsSaving(false);
    if (!res.ok) {
      showToast(res.error ?? "Failed to save white-label settings.", false);
      return;
    }
    void refreshBranding();
    showToast("White-label settings saved.", true);
  }

  // ── Provider change ────────────────────────────────────────────────────────
  function handleFormProviderChange(p: AIProvider) {
    setFormData((prev) => ({
      ...prev,
      provider: p,
      baseURL: p !== "custom" ? DEFAULT_BASE_URLS[p] : prev.baseURL,
    }));
  }

  // ── Test connection ────────────────────────────────────────────────────────
  async function runTest(cfg: Pick<AIConfig, "provider" | "baseURL" | "apiKey">, id: string | null) {
    if (!cfg.apiKey.trim())  { showToast("Enter an API key first.", false); return; }
    if (!cfg.baseURL.trim()) { showToast("Enter a Base URL first.", false); return; }
    const resolvedId = id ?? "__form__";
    setTestingId(resolvedId);

    if (id) {
      const updated = configs.map((c) =>
        c.id === id ? { ...c, status: "testing" as AIConfigStatus } : c,
      );
      setConfigs(updated);
      setAIConfigsInStorage(updated);
    }

    let ok = false;
    try {
      if (cfg.provider === "gemini") {
        const res = await fetch(
          `${cfg.baseURL.replace(/\/$/, "")}/models?key=${encodeURIComponent(cfg.apiKey.trim())}`,
        );
        ok = res.ok;
        if (res.ok) {
          showToast("Google Gemini connection successful ✓", true);
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(`Gemini error ${res.status}: ${body?.error?.message ?? "Invalid key"}`, false);
        }
      } else {
        const res = await fetch(`${cfg.baseURL.replace(/\/$/, "")}/models`, {
          headers: { Authorization: `Bearer ${cfg.apiKey.trim()}` },
        });
        ok = res.ok;
        if (res.ok) {
          showToast(`${cfg.provider === "openai" ? "OpenAI" : "Custom API"} connection successful ✓`, true);
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(`Error ${res.status}: ${body?.error?.message ?? "Connection failed"}`, false);
        }
      }
    } catch {
      showToast("Network error — check your connection or CORS policy.", false);
    } finally {
      setTestingId(null);
      if (id) {
        const newStatus: AIConfigStatus = ok ? "active" : "error";
        const updated = configs.map((c) =>
          c.id === id ? { ...c, status: newStatus } : c,
        );
        setConfigs(updated);
        setAIConfigsInStorage(updated);
      }
    }
  }

  // ── Save new AI config ─────────────────────────────────────────────────────
  async function handleAddConfig(e: React.FormEvent) {
    e.preventDefault();
    if (!formData.apiKey.trim())  { showToast("API key is required.", false); return; }
    if (!formData.baseURL.trim()) { showToast("Base URL is required.", false); return; }
    setSavingForm(true);

    const base = configs.map((c) =>
      formData.isGlobalOverride ? { ...c, isGlobalOverride: false } : c,
    );
    const newCfg: AIConfig = {
      ...formData,
      id:           `cfg_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      providerName: formData.providerName.trim() || PROVIDER_LABELS[formData.provider],
      apiKey:       formData.apiKey.trim(),
      baseURL:      formData.baseURL.trim(),
      role:         normalizeAIRoleTag(formData.role),
      status:       "untested",
    };
    const updated = [...base, newCfg];
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    setFormData(newBlankConfig());
    setShowForm(false);
    setSavingForm(false);

    // For OpenAI configs: persist key to DB and show explicit DB save result
    if (newCfg.provider === "openai" && newCfg.apiKey) {
      try {
        const res = await upsertProviderApiKey("OpenAI", newCfg.apiKey);
        if (!res.ok) {
          console.error("[Settings] DB save failed:", res.error);
          showToast(
            `Key saved locally but DB sync failed: ${res.error ?? "unknown error"}. Key won't work cross-device.`,
            false,
          );
        } else {
          showToast("OpenAI key saved & synced to database ✓ — works on all devices.", true);
        }
      } catch (err) {
        console.error("[Settings] upsertProviderApiKey threw:", err);
        showToast(
          `Key saved locally but DB sync threw an error: ${err instanceof Error ? err.message : String(err)}`,
          false,
        );
      }
    } else {
      showToast("API connection saved.", true);
    }
  }

  function handleToggleGlobalOverride(id: string) {
    const target = configs.find((c) => c.id === id);
    if (!target) return;
    const willBeGlobal = !target.isGlobalOverride;
    const updated = configs.map((c) => ({
      ...c,
      isGlobalOverride: c.id === id ? willBeGlobal : false,
    }));
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    showToast(
      willBeGlobal
        ? `"${target.providerName}" is now the global provider for all tasks.`
        : "Global override removed.",
      true,
    );
  }

  function handleRoleChange(id: string, newRole: string) {
    const tag = normalizeAIRoleTag(newRole);
    const updated = configs.map((c) => (c.id === id ? { ...c, role: tag } : c));
    setConfigs(updated);
    setAIConfigsInStorage(updated);
  }

  function handleSaveAssignments() {
    setAIRoleAssignmentsInStorage(assignments);
    showToast("Role assignments saved.", true);
  }

  function handleDeleteConfig(id: string) {
    if (!window.confirm("Remove this API configuration? This cannot be undone.")) return;
    const updated = configs.filter((c) => c.id !== id);
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    const newAssign: AIRoleAssignments = {
      defaultGeneral: assignments.defaultGeneral === id ? null : assignments.defaultGeneral,
      defaultVision:  assignments.defaultVision  === id ? null : assignments.defaultVision,
    };
    setAssignments(newAssign);
    setAIRoleAssignmentsInStorage(newAssign);
    showToast("Configuration removed.", true);
  }

  function handleClearAll() {
    if (!window.confirm("Delete ALL saved API configurations and keys? This cannot be undone.")) return;
    clearAIUnifiedKeyFromStorage();
    clearOpenAIApiKeyFromStorage();
    clearGeminiApiKeyFromStorage();
    setAIConfigsInStorage([]);
    setAIRoleAssignmentsInStorage({ defaultGeneral: null, defaultVision: null });
    setConfigs([]);
    setAssignments({ defaultGeneral: null, defaultVision: null });
    showToast("All API configurations cleared.", true);
  }

  async function handleSaveFefo(e: React.FormEvent) {
    e.preventDefault();
    if (mockPlan === "Free Tier") { showToast("Upgrade to Pro to customise FEFO rules.", false); return; }
    const critical = Number(fefoLocalEdit.fefo_critical_days);
    const warning  = Number(fefoLocalEdit.fefo_warning_days);
    if (isNaN(critical) || critical < 1)  { showToast("Critical days must be ≥ 1.", false); return; }
    if (isNaN(warning)  || warning  < 1)  { showToast("Warning days must be ≥ 1.", false); return; }
    if (critical >= warning) { showToast("Critical days must be less than Warning days.", false); return; }
    setFefoSaving(true);
    const res = await saveInventoryFefoSettings({ fefo_critical_days: critical, fefo_warning_days: warning });
    setFefoSaving(false);
    if (!res.ok) { showToast(res.error ?? "Failed to save FEFO settings.", false); return; }
    setFefoSettings({ fefo_critical_days: critical, fefo_warning_days: warning });
    showToast("Inventory / FEFO settings saved.", true);
  }

  async function handleLogisticsSyncNow() {
    if (mockPlan === "Free Tier") return;
    setLogisticsSyncBusy(true);
    try {
      const res = await syncClaimQueueNow();
      if (!res.ok) {
        showToast(res.error ?? "Sync failed.", false);
        return;
      }
      showToast(`Synced ${res.generated ?? 0} submission(s).`, true);
      const st = await getClaimQueueSyncStatus();
      if (st.ok) {
        setLogisticsSyncStatus({
          pendingSyncCount: st.pendingSyncCount ?? 0,
          systemUpToDate: st.systemUpToDate ?? true,
          readyForClaimCount: st.readyForClaimCount,
        });
      }
    } finally {
      setLogisticsSyncBusy(false);
    }
  }

  async function handleSaveClaimAgent(e: React.FormEvent) {
    e.preventDefault();
    if (mockPlan === "Free Tier") {
      showToast("Upgrade to Pro to configure Claim Engine agent controls.", false);
      return;
    }
    const maxUsd = Number(claimAgentLocal.max_auto_submit_amount_usd);
    if (isNaN(maxUsd) || maxUsd < 0) {
      showToast("Maximum auto-submit amount must be zero or positive.", false);
      return;
    }
    const intervalH = Math.max(1, Math.min(168, Number(claimAgentLocal.logistics_sync_interval_hours ?? 2)));
    if (Number.isNaN(intervalH)) {
      showToast("Sync frequency must be between 1 and 168 hours.", false);
      return;
    }
    setClaimAgentSaving(true);
    const res = await saveClaimAgentConfig({
      auto_generate_pdf_reports: claimAgentLocal.auto_generate_pdf_reports ?? true,
      allow_agent_direct_submit: claimAgentLocal.allow_agent_direct_submit ?? false,
      max_auto_submit_amount_usd: maxUsd,
      autonomous_claim_submission_0_50_usd: claimAgentLocal.autonomous_claim_submission_0_50_usd ?? false,
      require_manual_approval_bulk_submission: claimAgentLocal.require_manual_approval_bulk_submission ?? true,
      logistics_background_sync_enabled: claimAgentLocal.logistics_background_sync_enabled ?? false,
      logistics_sync_interval_hours: intervalH,
    });
    setClaimAgentSaving(false);
    if (!res.ok) {
      showToast(res.error ?? "Failed to save agent settings.", false);
      return;
    }
    const merged: ClaimAgentConfig = {
      ...DEFAULT_CLAIM_AGENT_CONFIG,
      ...claimAgentLocal,
      max_auto_submit_amount_usd: maxUsd,
      logistics_sync_interval_hours: intervalH,
    };
    setClaimAgentSaved(merged);
    setClaimAgentLocal(merged);
    showToast("Claim agent settings saved.", true);
  }

  async function handleSaveClaimEvidence(e: React.FormEvent) {
    e.preventDefault();
    if (mockPlan === "Free Tier") {
      showToast("Upgrade to Pro to configure default claim evidence.", false);
      return;
    }
    setClaimEvidenceSaving(true);
    const patch: DefaultClaimEvidence = {};
    (Object.keys(CLAIM_EVIDENCE_KEY_LABELS) as ClaimEvidenceKey[]).forEach((k) => {
      patch[k] = claimEvidenceLocal[k];
    });
    const res = await saveOrganizationClaimEvidenceDefaults(patch, tenantCtx);
    setClaimEvidenceSaving(false);
    if (!res.ok) {
      showToast(res.error ?? "Failed to save evidence defaults.", false);
      return;
    }
    setClaimEvidenceLocal(mergeDefaultClaimEvidence(patch));
    showToast("Default claim evidence saved.", true);
  }

  function handleSaveHardware(e: React.FormEvent) {
    e.preventDefault();
    setBarcodeModeInStorage(barcodeMode);
    setLabelPrinterInStorage(labelPrinter);
    setHwSaved(true);
    setTimeout(() => setHwSaved(false), 2500);
    showToast("Hardware settings saved.", true);
  }

  function closeAddStoreModal() {
    setShowAddStoreModal(false);
    setEditingStoreId(null);
    setStoreModalCredLoading(false);
    setStoreModalTestLoading(false);
  }

  function buildStoreCredentialsForSave(): Record<string, string> {
    if (newStorePlatform === "amazon") {
      const keys = [
        "seller_id",
        "marketplace_id",
        "lwa_client_id",
        "lwa_client_secret",
        "aws_access_key",
        "aws_secret_key",
        "refresh_token",
        "region",
        "endpoint",
      ];
      const out: Record<string, string> = {};
      for (const k of keys) {
        const v = newStoreCredentials[k]?.trim();
        if (v) out[k] = v;
      }
      return out;
    }
    const credFields = PLATFORM_CRED_FIELDS[newStorePlatform] ?? [];
    const out: Record<string, string> = {};
    for (const f of credFields) {
      const v = newStoreCredentials[f.key]?.trim();
      if (v) out[f.credKey] = v;
    }
    return out;
  }

  async function handleTestModalConnection() {
    const provider = PLATFORM_TO_PROVIDER[newStorePlatform];
    if (!provider) {
      showToast("Connection test is not available for this platform.", false);
      return;
    }
    const creds = buildStoreCredentialsForSave();
    if (newStorePlatform === "amazon") {
      if (!creds.lwa_client_id?.trim() || !creds.lwa_client_secret?.trim()) {
        showToast("Enter LWA Client ID and LWA Client Secret to test.", false);
        return;
      }
    } else if (newStorePlatform === "walmart") {
      if (!creds.client_id?.trim() || !creds.client_secret?.trim()) {
        showToast("Enter Client ID and Client Secret to test.", false);
        return;
      }
    } else if (Object.keys(creds).length === 0) {
      showToast("Enter API credentials in the form to test.", false);
      return;
    }
    setStoreModalTestLoading(true);
    const res = await testMarketplaceCredentials(provider as AdapterProviderKey, creds, settingsRbac);
    setStoreModalTestLoading(false);
    showToast(
      res.ok ? "Connection verified successfully ✓" : (res.error ?? "Connection test failed."),
      res.ok,
    );
  }

  function openAddStoreModal() {
    setEditingStoreId(null);
    setNewStoreName("");
    setNewStorePlatform("amazon");
    setNewStoreRegion("US");
    setNewStoreCredentials({
      region: AMAZON_SP_API_DEFAULTS.region,
      endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
    });
    setShowAddStoreModal(true);
  }

  async function openEditStoreModal(store: StorePublicRow) {
    setEditingStoreId(store.id);
    setNewStoreName(store.name);
    setNewStorePlatform(store.platform);
    setNewStoreRegion("US");
    setNewStoreCredentials({});
    setShowAddStoreModal(true);
    if (store.marketplace_id) {
      setStoreModalCredLoading(true);
      try {
        const res = await getMarketplaceCredentialsForEdit(store.marketplace_id, settingsRbac);
        if (res.ok && res.data) {
          if (store.platform === "amazon") {
            setNewStoreCredentials({
              region: AMAZON_SP_API_DEFAULTS.region,
              endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
              ...res.data,
            });
          } else {
            const mapped: Record<string, string> = {};
            const fields = PLATFORM_CRED_FIELDS[store.platform] ?? [];
            for (const f of fields) {
              const v = res.data[f.credKey];
              if (v) mapped[f.key] = v;
            }
            setNewStoreCredentials(mapped);
          }
        } else if (store.platform === "amazon") {
          setNewStoreCredentials({
            region: AMAZON_SP_API_DEFAULTS.region,
            endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
          });
        }
      } finally {
        setStoreModalCredLoading(false);
      }
    } else if (store.platform === "amazon") {
      setNewStoreCredentials({
        region: AMAZON_SP_API_DEFAULTS.region,
        endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
      });
    }
  }

  async function handleAddStore(e: React.FormEvent) {
    e.preventDefault();
    if (!newStoreName.trim()) { showToast("Store name is required.", false); return; }
    setAddStoreSaving(true);

    const creds = buildStoreCredentialsForSave();
    const provider = PLATFORM_TO_PROVIDER[newStorePlatform];

    if (editingStoreId) {
      const res = await updateStore(editingStoreId, { name: newStoreName }, settingsRbac);
      if (!res.ok) {
        setAddStoreSaving(false);
        showToast(res.error ?? "Failed to update store.", false);
        return;
      }

      if (Object.keys(creds).length > 0 && provider) {
        const existingStore = storesList.find((s) => s.id === editingStoreId);
        if (existingStore?.marketplace_id) {
          const mpRes = await updateMarketplace(existingStore.marketplace_id, { credentials: creds }, settingsRbac);
          if (!mpRes.ok) {
            setAddStoreSaving(false);
            showToast(mpRes.error ?? "Failed to update API credentials.", false);
            return;
          }
        } else {
          const mpRes = await insertMarketplace({
            provider: provider as Parameters<typeof insertMarketplace>[0]["provider"],
            nickname: newStoreName.trim(),
            credentials: creds,
          }, settingsRbac);
          if (!mpRes.ok || !mpRes.data?.id) {
            setAddStoreSaving(false);
            showToast(mpRes.error ?? "Failed to save API credentials.", false);
            return;
          }
          const linkRes = await updateStore(editingStoreId, { marketplace_id: mpRes.data.id }, settingsRbac);
          if (!linkRes.ok) {
            setAddStoreSaving(false);
            showToast(linkRes.error ?? "Failed to link credentials to store.", false);
            return;
          }
        }
      }

      const refreshed = await listStores(settingsRbac);
      if (refreshed.ok && refreshed.data) setStoresList(refreshed.data);
      setAddStoreSaving(false);
      closeAddStoreModal();
      showToast(`Store "${newStoreName.trim()}" updated.`, true);
    } else {
      let marketplace_id: string | undefined;
      if (provider && Object.keys(creds).length > 0) {
        const mpRes = await insertMarketplace({
          provider: provider as Parameters<typeof insertMarketplace>[0]["provider"],
          nickname: newStoreName.trim(),
          credentials: creds,
        }, settingsRbac);
        if (!mpRes.ok) {
          setAddStoreSaving(false);
          showToast(mpRes.error ?? "Failed to save API credentials.", false);
          return;
        }
        marketplace_id = mpRes.data?.id;
      }

      const res = await insertStore({
        name: newStoreName,
        platform: newStorePlatform,
        region: newStoreRegion,
        marketplace_id,
      }, settingsRbac);
      setAddStoreSaving(false);
      if (!res.ok) { showToast(res.error ?? "Failed to create store.", false); return; }

      const refreshed = await listStores(settingsRbac);
      if (refreshed.ok && refreshed.data) setStoresList(refreshed.data);
      setNewStoreName("");
      setNewStorePlatform("amazon");
      setNewStoreRegion("US");
      setNewStoreCredentials({
        region: AMAZON_SP_API_DEFAULTS.region,
        endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
      });
      closeAddStoreModal();
      showToast(`Store "${res.data?.name}" created successfully.`, true);
    }
  }

  async function handleDeleteStore(store: StorePublicRow) {
    if (!window.confirm(`Delete "${store.name}"? This cannot be undone.`)) return;
    setDeletingStoreId(store.id);
    const res = await deleteStore(store.id, settingsRbac);
    setDeletingStoreId(null);
    if (!res.ok) { showToast(res.error ?? "Failed to delete store.", false); return; }
    setStoresList((prev) => prev.filter((s) => s.id !== store.id));
    if (defaultStoreId === store.id) {
      setDefaultStoreId("");
      setDefaultStoreIdInStorage("");
      void saveOrganizationDefaultStoreId(null, tenantCtx);
    }
    showToast(`Store "${store.name}" deleted.`, true);
  }

  async function handleTestStoreConnection(store: StorePublicRow) {
    if (!store.marketplace_id) {
      showToast("No API credentials linked. Edit the store to add credentials first.", false);
      return;
    }
    setStoreTestStatus((prev) => ({ ...prev, [store.id]: "testing" }));
    const res = await testConnection(store.marketplace_id, settingsRbac);
    setStoreTestStatus((prev) => ({ ...prev, [store.id]: res.ok ? "ok" : "error" }));
    showToast(
      res.ok ? "Connection verified successfully ✓" : (res.error ?? "Connection test failed."),
      res.ok,
    );
  }

  if (!mounted) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!isAdminRole(role)) return null;

  const globalProvider = configs.find((c) => c.isGlobalOverride) ?? null;
  const isOverridden   = globalProvider !== null;

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="w-full max-w-7xl mx-auto px-2 sm:px-4 py-8">
      <Link
        href="/"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Back
      </Link>

      {/* Page header */}
      <div className="mb-8 flex items-center gap-3">
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-100 dark:bg-violet-950/50">
          <KeyRound className="h-6 w-6 text-violet-600 dark:text-violet-400" />
        </div>
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Settings</h1>
          <p className="text-sm text-muted-foreground">
            Workspace configuration · Admin-only
          </p>
        </div>
      </div>

      {/* Success / error toast — fixed so it stays visible while scrolling long settings */}
      {toast && (
        <div
          role="status"
          aria-live="polite"
          className={[
            "fixed bottom-6 right-6 z-[80] flex max-w-md items-center gap-3 rounded-lg border px-4 py-3 text-sm font-medium shadow-lg",
            toast.ok
              ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700/50 dark:bg-emerald-950/90 dark:text-emerald-200"
              : "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-700/50 dark:bg-rose-950/90 dark:text-rose-200",
          ].join(" ")}
        >
          {toast.ok
            ? <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" />
            : <ShieldAlert   className="h-4 w-4 shrink-0" />}
          {toast.msg}
        </div>
      )}

      {/* ── Enterprise layout: fixed sidebar + flex content ─────────────────── */}
      <div className="flex flex-col md:flex-row w-full gap-6">

        {/* LEFT SIDEBAR — 3-tier: System & Workspace / Infrastructure / Business Modules */}
        <nav className="w-full md:w-64 md:flex-shrink-0 md:border-r md:border-border md:pr-6 md:sticky md:top-6 md:self-start">
          <div className="flex flex-col w-full gap-1">

            {NAV_GROUPS.map((group) => (
              <div key={group.label} className="mb-2">
                {/* Section header */}
                <p className="mb-1.5 px-2 text-[10px] font-bold uppercase tracking-widest text-muted-foreground/60">
                  {group.label}
                </p>

                {group.items.map(({ id, label, icon, proOnly }) => {
                  const isLocked = proOnly && mockPlan === "Free Tier";
                  return (
                    <button
                      key={id}
                      type="button"
                      onClick={() => setActiveTab(id)}
                      className={[
                        "flex w-full items-center gap-3 rounded-xl px-4 py-2.5 text-sm font-semibold transition-all text-left",
                        activeTab === id
                          ? "bg-violet-100 text-violet-700 dark:bg-violet-950/50 dark:text-violet-300"
                          : "text-muted-foreground hover:bg-accent hover:text-foreground",
                        isLocked ? "opacity-70" : "",
                      ].join(" ")}
                    >
                      {icon}
                      <span className="flex-1">{label}</span>
                      {isLocked && (
                        <Crown className="h-3 w-3 shrink-0 text-amber-500" />
                      )}
                    </button>
                  );
                })}
              </div>
            ))}

            {/* Plan badge in sidebar */}
            <div className="hidden md:block mt-4 rounded-xl border border-border bg-muted/40 px-4 py-3">
              <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Current Plan</p>
              <p className={`mt-1 text-sm font-bold ${
                mockPlan === "Free Tier"  ? "text-slate-600 dark:text-slate-300"
                : mockPlan === "Pro Tier" ? "text-sky-600 dark:text-sky-400"
                : "text-violet-600 dark:text-violet-400"
              }`}>
                {mockPlan === "Enterprise" && <Zap className="mr-1 inline h-3.5 w-3.5" />}
                {mockPlan}
              </p>
            </div>
          </div>
        </nav>

        {/* RIGHT CONTENT AREA */}
        <div className="flex-1 min-w-0 w-full overflow-hidden">

          {/* ══════════════ GENERAL & WHITE-LABEL ══════════════ */}
          {activeTab === "general" && (
            <div className="space-y-6">

              {/* ── Tenant Customization / White-label ───────────────────── */}
              <form onSubmit={handleSaveWhitelabel} className="space-y-5">
                <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-6">
                  <div className="flex items-start gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-violet-100 dark:bg-violet-950/50">
                      <Building2 className="h-5 w-5 text-violet-600 dark:text-violet-400" />
                    </div>
                    <div>
                      <h2 className="text-base font-bold">Tenant Customization</h2>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        White-label branding fields. Used for{" "}
                        <span className="font-semibold">report generation</span> and{" "}
                        <span className="font-semibold">custom dashboard branding</span>.
                        Stored in <code className="rounded bg-muted px-1 font-mono text-[11px]">core_settings</code>.
                      </p>
                    </div>
                  </div>

                  {coreSettingsLoading ? (
                    <div className="flex items-center gap-2 py-2">
                      <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      <span className="text-xs text-muted-foreground">Loading branding settings…</span>
                    </div>
                  ) : (
                    <div className="grid gap-5 sm:grid-cols-2">
                      <div>
                        <div className="mb-1.5 flex items-center gap-2">
                          <Building2 className="h-3.5 w-3.5 text-muted-foreground" />
                          <label className={LABEL_CLS}>Company Name</label>
                        </div>
                        <p className={HINT_CLS}>
                          Displayed on generated reports, packing slips, and email footers.
                        </p>
                        <input
                          type="text"
                          value={companyName}
                          onChange={(e) => setCompanyName(e.target.value)}
                          placeholder="e.g. Acme Fulfillment Co."
                          className={INPUT_CLS}
                        />
                      </div>

                      <div>
                        <div className="mb-1.5 flex items-center gap-2">
                          <ImageIcon className="h-3.5 w-3.5 text-muted-foreground" />
                          <label className={LABEL_CLS}>Company Logo</label>
                        </div>
                        <p className={HINT_CLS}>
                          PNG, SVG, or WebP. Stored in the public <code className="rounded bg-muted px-1 font-mono text-[10px]">logos</code> bucket
                          and <code className="rounded bg-muted px-1 font-mono text-[10px]">organization_settings.logo_url</code>. Shown in the sidebar,
                          settings preview, and claim PDF header (max 180×50 display).
                        </p>
                        <label className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-md border-2 border-dashed border-input bg-muted/30 py-3 text-sm font-medium transition hover:bg-muted/50 ${logoUploading ? "text-muted-foreground" : companyLogoUrl ? "border-emerald-500/50 text-emerald-800 dark:text-emerald-200" : "text-foreground"}`}>
                          {logoUploading
                            ? <><Loader2 className="h-4 w-4 animate-spin" />Uploading…</>
                            : companyLogoUrl
                              ? <><ImageIcon className="h-4 w-4" />Logo saved — click to replace</>
                              : <><ImageIcon className="h-4 w-4" />Upload Logo File</>}
                          <input type="file" accept="image/*" className="hidden" disabled={logoUploading} onChange={handleLogoFileUpload} />
                        </label>
                      </div>
                    </div>
                  )}

                  {companyLogoUrl && (
                    <div className="flex items-center gap-3 rounded-lg border border-border bg-muted/30 px-4 py-3">
                      <div className="flex h-[45px] w-full max-w-[160px] shrink-0 items-center justify-start overflow-hidden">
                        {/* eslint-disable-next-line @next/next/no-img-element */}
                        <img
                          src={companyLogoUrl}
                          alt="Company logo preview"
                          className={BRAND_LOGO_IMG_CLASSNAME}
                          onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = "none"; }}
                        />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-xs font-semibold">Logo Preview</p>
                        <p className="text-[11px] text-muted-foreground truncate">Displayed on sidebar &amp; reports.</p>
                      </div>
                      <button type="button" onClick={() => setCompanyLogoUrl("")} className="shrink-0 text-xs text-rose-500 underline hover:text-rose-700">Remove</button>
                    </div>
                  )}
                </div>

                <button
                  type="submit"
                  disabled={coreSettingsSaving || coreSettingsLoading}
                  className="inline-flex min-w-[220px] items-center justify-center gap-2 rounded-md bg-violet-600 px-4 py-2 text-sm font-semibold text-white shadow transition hover:bg-violet-700 disabled:opacity-50"
                >
                  {coreSettingsSaving
                    ? <><Loader2 className="h-4 w-4 animate-spin" />Saving…</>
                    : <><Save    className="h-4 w-4" />Save White-label Settings</>}
                </button>
              </form>

              {/* ── General / Operational Preferences ───────────────────── */}
              <form onSubmit={handleSaveGeneral} className="space-y-5">
                <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-6">
                  <div>
                    <h2 className="text-base font-bold">Operational Preferences</h2>
                    <p className="text-xs text-muted-foreground mt-0.5">
                      Operational defaults applied across the warehouse workflow.
                    </p>
                  </div>

                  <div>
                    <div className="mb-1.5 flex items-center gap-2">
                      <Tag className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                      <label className={LABEL_CLS}>Default Store</label>
                    </div>
                    <p className={HINT_CLS}>
                      Saved to <code className="rounded bg-muted px-1 font-mono text-[11px]">organization_settings.default_store_id</code>.
                      Fallback when a scanned barcode can&apos;t be matched to a parent package.
                      Amazon FNSKUs (starting with{" "}
                      <code className="rounded bg-muted px-1 font-mono text-[11px]">X00</code> or{" "}
                      <code className="rounded bg-muted px-1 font-mono text-[11px]">B00</code>) are
                      auto-matched to your first active Amazon store.
                    </p>
                    {storesListLoading ? (
                      <div className="flex items-center gap-2 py-3">
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                        <span className="text-xs text-muted-foreground">Loading stores…</span>
                      </div>
                    ) : storesList.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-border bg-muted/20 px-4 py-3 text-xs text-muted-foreground">
                        No stores found. Add a store in the{" "}
                        <strong>Marketplaces &amp; Stores</strong> tab first.
                      </div>
                    ) : (
                      <select
                        value={defaultStoreId}
                        onChange={(e) => setDefaultStoreId(e.target.value)}
                        className={SELECT_CLS}
                      >
                        <option value="">— No default (unknown) —</option>
                        {storesList.map((s) => (
                          <option key={s.id} value={s.id}>
                            {s.name}{" "}
                            ({PLATFORM_LABELS[s.platform] ?? s.platform}
                            {!s.is_active ? " · inactive" : ""})
                          </option>
                        ))}
                      </select>
                    )}
                    {defaultStoreId && (
                      <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                        <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                        Standalone scans will fall back to{" "}
                        <strong>
                          {storesList.find((s) => s.id === defaultStoreId)?.name ?? defaultStoreId}
                        </strong>{" "}
                        when no package or prefix is detected.
                      </p>
                    )}
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  <button
                    type="submit"
                    className="inline-flex min-w-[180px] flex-1 items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90"
                  >
                    <Save className="h-4 w-4" />
                    Save General Preferences
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* ══════════════ MARKETPLACES & STORES ══════════════ */}
          {activeTab === "marketplaces" && (
            <div className="space-y-6">

              {/* ── Connected Stores card ─────────────────────────────────── */}
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-5">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-sky-100 dark:bg-sky-950/50">
                      <Store className="h-5 w-5 text-sky-600 dark:text-sky-400" />
                    </div>
                    <div>
                      <h2 className="text-base font-bold">Connected Stores</h2>
                      <p className="text-xs text-muted-foreground">
                        {storesList.length}
                        {planLimits.stores === Infinity ? "" : ` / ${planLimits.stores}`} store
                        {storesList.length !== 1 ? "s" : ""} on{" "}
                        <span className="font-semibold">{mockPlan}</span>
                      </p>
                    </div>
                  </div>

                  {/* Add New Store — paywall gated */}
                  {canAddStore ? (
                    <button
                      type="button"
                      onClick={openAddStoreModal}
                      className="inline-flex shrink-0 items-center gap-2 rounded-xl bg-sky-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-sky-700 active:scale-[0.98]"
                    >
                      <Plus className="h-4 w-4" />
                      Add New Store
                    </button>
                  ) : (
                    <button
                      type="button"
                      disabled
                      title="Upgrade your plan to connect more stores"
                      className="inline-flex shrink-0 cursor-not-allowed items-center gap-2 rounded-xl bg-amber-400 px-4 py-2.5 text-sm font-bold text-amber-950 opacity-95"
                    >
                      👑 Upgrade to Add Stores
                    </button>
                  )}
                </div>

                {/* Paywall banner */}
                {!canAddStore && (
                  <div className="flex items-start gap-3 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 dark:border-amber-700/40 dark:bg-amber-950/20">
                    <Zap className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                    <p className="text-xs font-medium text-amber-800 dark:text-amber-300">
                      You&apos;ve reached the <strong>{mockPlan}</strong> limit of{" "}
                      {planLimits.stores} store{planLimits.stores !== 1 ? "s" : ""}. Upgrade to Pro
                      or Enterprise to connect additional stores.
                    </p>
                  </div>
                )}

                {/* Stores list — card grid */}
                {storesListLoading ? (
                  <div className="flex items-center gap-2 py-4">
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                    <p className="text-sm text-muted-foreground">Loading stores…</p>
                  </div>
                ) : storesList.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-border bg-muted/20 py-10 text-center">
                    <Store className="mx-auto mb-3 h-9 w-9 text-muted-foreground/30" />
                    <p className="text-sm font-semibold text-muted-foreground">No stores yet.</p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      Click <strong>Add New Store</strong> above to connect your first marketplace store.
                    </p>
                  </div>
                ) : (
                  <div className="grid gap-3 sm:grid-cols-2">
                    {storesList.map((s) => {
                      const testSt     = storeTestStatus[s.id] ?? "idle";
                      const isDeleting = deletingStoreId === s.id;
                      const isTesting  = testSt === "testing";
                      const isDefault  = s.id === defaultStoreId;

                      // Derive display status: prefer test result over DB active flag
                      const statusInvalid = testSt === "error";
                      const statusActive  = testSt === "ok" || (s.is_active && testSt === "idle");

                      return (
                        <div
                          key={s.id}
                          className={[
                            "relative flex flex-col gap-3 rounded-xl border p-4 transition",
                            isDefault
                              ? "border-violet-300 ring-1 ring-violet-200 bg-violet-50/30 dark:border-violet-600/60 dark:ring-violet-800/30 dark:bg-violet-950/10"
                              : "border-border bg-background hover:border-sky-200 hover:shadow-sm",
                          ].join(" ")}
                        >
                          <div className="flex items-start justify-between gap-3 min-w-0">
                            <div className="min-w-0 flex-1 space-y-0.5">
                              <p className="text-sm font-bold leading-tight truncate">{s.name}</p>
                              <p className="text-xs font-medium text-muted-foreground">
                                Marketplace · {PLATFORM_LABELS[s.platform] ?? s.platform}
                              </p>
                            </div>
                            <div className="flex shrink-0 flex-col items-end gap-1.5">
                              {isDefault && (
                                <span className="inline-flex items-center gap-1 rounded-full border border-violet-300 bg-violet-100 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                                  ★ Default
                                </span>
                              )}
                              <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-bold ${
                                statusInvalid
                                  ? "border-rose-200 bg-rose-50 text-rose-600 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                                  : statusActive
                                  ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300"
                                  : "border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400"
                              }`}>
                                {statusInvalid ? (
                                  <><TriangleAlert className="h-2.5 w-2.5" />Invalid</>
                                ) : statusActive ? (
                                  <><BadgeCheck className="h-2.5 w-2.5" />Active</>
                                ) : (
                                  "Inactive"
                                )}
                              </span>
                              {s.marketplace_id && (
                                <span className="inline-flex items-center rounded-full border border-violet-200 bg-violet-50 px-1.5 py-0.5 text-[9px] font-bold text-violet-600 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                                  API
                                </span>
                              )}
                            </div>
                          </div>

                          {/* Store ID */}
                          <p className="font-mono text-[10px] text-muted-foreground">
                            ID: {s.id.slice(0, 8)}…
                          </p>

                          {/* Action buttons */}
                          <div className="flex flex-wrap items-center gap-1.5 pt-1 border-t border-border">
                            {/* Test Connection */}
                            <button
                              type="button"
                              onClick={() => void handleTestStoreConnection(s)}
                              disabled={isTesting || !s.marketplace_id}
                              title={!s.marketplace_id ? "No API credentials linked — edit to add credentials" : undefined}
                              className={[
                                "inline-flex items-center gap-1.5 rounded-lg border px-2.5 py-1.5 text-[11px] font-semibold transition disabled:opacity-40 disabled:cursor-not-allowed",
                                testSt === "ok"
                                  ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300"
                                  : testSt === "error"
                                  ? "border-rose-200 bg-rose-50 text-rose-600 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                                  : "border-border bg-background text-muted-foreground hover:border-sky-300 hover:text-sky-600",
                              ].join(" ")}
                            >
                              {isTesting ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : testSt === "ok" ? (
                                <BadgeCheck className="h-3 w-3" />
                              ) : testSt === "error" ? (
                                <TriangleAlert className="h-3 w-3" />
                              ) : (
                                <RefreshCw className="h-3 w-3" />
                              )}
                              {isTesting ? "Testing…" : testSt === "ok" ? "Verified" : testSt === "error" ? "Failed" : "Test Connection"}
                            </button>

                            {/* Edit */}
                            <button
                              type="button"
                              onClick={() => void openEditStoreModal(s)}
                              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-background px-2.5 py-1.5 text-[11px] font-semibold text-muted-foreground transition hover:border-sky-300 hover:text-sky-600"
                            >
                              <Pencil className="h-3 w-3" />
                              Edit
                            </button>

                            {/* Delete */}
                            <button
                              type="button"
                              onClick={() => void handleDeleteStore(s)}
                              disabled={isDeleting}
                              className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-[11px] font-semibold text-rose-600 transition hover:bg-rose-100 disabled:opacity-50 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                            >
                              {isDeleting
                                ? <Loader2 className="h-3 w-3 animate-spin" />
                                : <Trash2  className="h-3 w-3" />}
                              {isDeleting ? "Deleting…" : "Delete"}
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* ── Hierarchy info banner ────────────────────────────────── */}
              <div className="rounded-2xl border border-violet-200 bg-violet-50/60 px-5 py-4 dark:border-violet-700/40 dark:bg-violet-950/20 space-y-1">
                <p className="flex items-center gap-2 text-sm font-bold text-violet-800 dark:text-violet-200">
                  <Zap className="h-4 w-4" />
                  Store Inheritance — Pallet → Package → Item
                </p>
                <p className="text-xs text-violet-700 dark:text-violet-400">
                  When you assign a store to a <strong>Pallet</strong>, all Packages created inside it
                  automatically inherit that store. Items then inherit from their parent Package.
                  Standalone items fall back to your <strong>Default Store</strong> set in General settings.
                </p>
              </div>

              {/* ── Supported marketplace integrations overview ─────────── */}
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-4">
                <h3 className="text-sm font-bold">Supported Marketplaces</h3>
                <div className="grid gap-3 sm:grid-cols-2">
                  {[
                    { name: "Amazon SP-API",  desc: "FBA/FBM returns, A-to-Z claims automation",    active: true  },
                    { name: "Walmart DSV",     desc: "Supplier returns portal, RMA workflows",        active: true  },
                    { name: "eBay",            desc: "Money Back Guarantee automation",               active: false, soon: true },
                    { name: "Target / Circle", desc: "Freight claim &amp; supplier integration",      active: false, soon: true },
                  ].map((m) => (
                    <div
                      key={m.name}
                      className="flex items-start gap-3 rounded-xl border border-border bg-background p-4"
                    >
                      <div className={`mt-0.5 h-2.5 w-2.5 shrink-0 rounded-full ${m.soon ? "bg-slate-300 dark:bg-slate-600" : "bg-emerald-400"}`} />
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <p className="text-sm font-semibold">{m.name}</p>
                          {m.soon && (
                            <span className="rounded-full bg-muted px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-muted-foreground">
                              Soon
                            </span>
                          )}
                        </div>
                        <p className="mt-0.5 text-xs text-muted-foreground" dangerouslySetInnerHTML={{ __html: m.desc }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* ══════════════ AI & OCR QUOTAS ══════════════ */}
          {activeTab === "ai_quotas" && (
            <div className="relative space-y-4">
              <DatabaseTag table="organization_settings" />

              {isOverridden && (
                <div className="flex items-center gap-3 rounded-2xl border-2 border-violet-300 bg-violet-50 px-4 py-3 dark:border-violet-600/50 dark:bg-violet-950/30">
                  <Zap className="h-5 w-5 shrink-0 text-violet-600 dark:text-violet-400" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-bold text-violet-700 dark:text-violet-300">
                      Global Override Active
                    </p>
                    <p className="truncate text-xs text-violet-600 dark:text-violet-400">
                      All tasks routed through <strong>{globalProvider!.providerName}</strong>.
                      Role assignments disabled.
                    </p>
                  </div>
                </div>
              )}

              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
                <div className="mb-4 flex items-center justify-between">
                  <div>
                    <h2 className="text-base font-bold">API Connections</h2>
                    <p className="text-xs text-muted-foreground">
                      Internal LLM routing (browser-stored keys). Add providers here, then assign roles — separate from{" "}
                      <span className="font-medium text-foreground/80">Workspace API keys</span> at the bottom.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => setShowForm(true)}
                    className="inline-flex shrink-0 items-center gap-1.5 rounded-xl border border-sky-200 bg-sky-50 px-3 py-2 text-xs font-semibold text-sky-700 transition hover:bg-sky-100 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
                  >
                    <Plus className="h-3.5 w-3.5" />
                    Add Connection
                  </button>
                </div>

                {configs.length === 0 && (
                  <div className="flex flex-col items-center gap-2 rounded-2xl border border-dashed border-border bg-muted/30 py-10 text-center">
                    <KeyRound className="h-8 w-8 text-muted-foreground/40" />
                    <p className="text-sm font-medium text-muted-foreground">No API connections saved yet.</p>
                    <p className="text-xs text-muted-foreground">
                      Click <strong>Add Connection</strong> to get started.
                    </p>
                  </div>
                )}

                {configs.length > 0 && (
                  <div className="space-y-3">
                    {configs.map((cfg) => {
                      const isTesting  = testingId === cfg.id;
                      const statusMeta = STATUS_META[cfg.status ?? "untested"];
                      const isGlobal   = !!cfg.isGlobalOverride;
                      return (
                        <div
                          key={cfg.id}
                          className={[
                            "group rounded-xl border bg-background p-4 transition",
                            isGlobal
                              ? "border-violet-300 ring-1 ring-violet-200 dark:border-violet-600/60 dark:ring-violet-800/40"
                              : "border-border hover:border-sky-300 dark:hover:border-sky-700",
                          ].join(" ")}
                        >
                          <div className="flex flex-wrap items-start gap-2">
                            <p className="font-semibold text-sm text-foreground leading-tight min-w-0 flex-1">
                              {cfg.providerName || PROVIDER_LABELS[cfg.provider]}
                            </p>
                            {isGlobal && (
                              <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                                <Zap className="h-3 w-3" />Global
                              </span>
                            )}
                            <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold ${statusMeta.cls}`}>
                              {isTesting ? "Testing…" : statusMeta.label}
                            </span>
                            <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                              {PROVIDER_LABELS[cfg.provider]}
                            </span>
                          </div>
                          <p className="mt-1.5 font-mono text-[11px] text-muted-foreground truncate">
                            {cfg.baseURL}
                          </p>
                          <p className="mt-0.5 font-mono text-[11px] text-muted-foreground">
                            {cfg.apiKey.length > 8
                              ? `${cfg.apiKey.slice(0, 4)}${"•".repeat(Math.min(cfg.apiKey.length - 6, 20))}${cfg.apiKey.slice(-2)}`
                              : "••••••••"}
                          </p>
                          <div className="mt-3 flex flex-wrap items-center gap-2">
                            <RoleTagCombobox
                              id={`role-tag-${cfg.id}`}
                              value={cfg.role}
                              onChange={(v) => handleRoleChange(cfg.id, v)}
                              className="min-w-[120px] max-w-[220px] rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] font-semibold text-slate-600 focus:outline-none focus:ring-2 focus:ring-sky-500/30 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
                              aria-label="Role tag"
                              placeholder="Tag…"
                            />
                            <label className="flex cursor-pointer items-center gap-1.5">
                              <input
                                type="checkbox"
                                checked={isGlobal}
                                onChange={() => handleToggleGlobalOverride(cfg.id)}
                                className="h-3.5 w-3.5 rounded accent-violet-600"
                              />
                              <span className="text-[11px] font-medium text-muted-foreground">
                                Use for all tasks
                              </span>
                            </label>
                            <div className="flex-1" />
                            <button
                              type="button"
                              onClick={() => runTest(cfg, cfg.id)}
                              disabled={isTesting}
                              className="inline-flex items-center gap-1.5 rounded-lg border border-border px-2.5 py-1 text-xs font-medium text-muted-foreground transition hover:border-sky-300 hover:text-sky-600 disabled:opacity-50"
                            >
                              {isTesting
                                ? <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                : <Wifi    className="h-3.5 w-3.5" />}
                              {isTesting ? "Testing…" : "Test"}
                            </button>
                            <button
                              type="button"
                              onClick={() => handleDeleteConfig(cfg.id)}
                              className="inline-flex items-center gap-1.5 rounded-lg border border-rose-200 bg-rose-50 px-2.5 py-1 text-xs font-medium text-rose-600 transition hover:bg-rose-100 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                              Remove
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* New connection — modal (keeps LLM setup separate from Workspace keys below) */}
              {showForm && (
                <div
                  className="fixed inset-0 z-[100] flex items-center justify-center p-4"
                  role="dialog"
                  aria-modal="true"
                  aria-labelledby="new-ai-connection-title"
                >
                  <button
                    type="button"
                    className="absolute inset-0 bg-black/50 backdrop-blur-[1px]"
                    aria-label="Close dialog"
                    onClick={closeAddConnectionModal}
                  />
                  <div className="relative z-10 max-h-[min(90vh,720px)] w-full max-w-lg overflow-y-auto rounded-2xl border border-border bg-card p-6 shadow-xl">
                    <form onSubmit={handleAddConfig} className="space-y-5">
                      <div className="flex items-start justify-between gap-3 border-b border-border pb-4">
                        <div>
                          <h3 id="new-ai-connection-title" className="text-base font-bold text-foreground">
                            New API Connection
                          </h3>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Browser-stored LLM credentials for routing and OCR. Separate from workspace API keys for external bots.
                          </p>
                        </div>
                        <button
                          type="button"
                          onClick={closeAddConnectionModal}
                          className="rounded-full p-1.5 text-muted-foreground transition hover:bg-muted"
                        >
                          <X className="h-4 w-4" />
                        </button>
                      </div>

                      <StableField
                        label="Connection name"
                        hint={
                          <>
                            A label to identify this connection (e.g. &quot;Gemini Flash — Chat&quot;).
                          </>
                        }
                      >
                        <input
                          type="text"
                          value={formData.providerName}
                          onChange={(e) => setFormData((p) => ({ ...p, providerName: e.target.value }))}
                          placeholder="e.g. GPT-4o Vision"
                          className={INPUT_CLS}
                        />
                      </StableField>

                      <StableField
                        label="Provider type"
                        hint="Grouped by vendor. Custom covers self-hosted and compatible proxies."
                      >
                        <select
                          value={formData.provider}
                          onChange={(e) => handleFormProviderChange(e.target.value as AIProvider)}
                          className={SELECT_CLS}
                        >
                          <optgroup label="OpenAI">
                            <option value="openai">OpenAI  (GPT-4o, o3, o4-mini…)</option>
                          </optgroup>
                          <optgroup label="Google">
                            <option value="gemini">Google Gemini</option>
                          </optgroup>
                          <optgroup label="Custom & self-hosted">
                            <option value="custom">Custom / local  (Ollama, Groq, Azure OpenAI…)</option>
                          </optgroup>
                        </select>
                      </StableField>

                      <StableField
                        label={
                          <span className="inline-flex items-center gap-1.5">
                            <Tag className="h-3.5 w-3.5" />
                            Role tag
                          </span>
                        }
                        hint="Used for routing presets below — choose a category or enter a custom tag."
                      >
                        <RoleTagCombobox
                          id="new-connection-role-tag"
                          value={formData.role}
                          onChange={(v) => setFormData((p) => ({ ...p, role: v }))}
                          className={INPUT_CLS}
                          placeholder="default"
                          aria-label="Role tag for new connection"
                        />
                      </StableField>

                      <StableField
                        label="Base URL"
                        hint="Auto-filled for known providers; edit freely for custom deployments."
                      >
                        <input
                          type="text"
                          autoComplete="off"
                          spellCheck={false}
                          value={formData.baseURL}
                          onChange={(e) => setFormData((p) => ({ ...p, baseURL: e.target.value }))}
                          placeholder="https://api.openai.com/v1"
                          className={`${INPUT_CLS} font-mono`}
                        />
                        <div className="mt-1.5 min-h-[2.75rem]">
                          {formData.provider === "custom" ? (
                            <p className="text-xs text-amber-600 dark:text-amber-400">
                              Custom providers: ensure CORS allows requests from this origin, or proxy via your backend.
                            </p>
                          ) : (
                            <span className="invisible select-none">.</span>
                          )}
                        </div>
                      </StableField>

                      <StableField
                        label="API key"
                        hint="Stored only in this browser&apos;s localStorage — never sent to our servers."
                      >
                        <div className="flex items-stretch gap-2">
                          <input
                            type="password"
                            autoComplete="off"
                            value={formData.apiKey}
                            onChange={(e) => setFormData((p) => ({ ...p, apiKey: e.target.value }))}
                            placeholder={
                              formData.provider === "openai" ? "sk-…"
                              : formData.provider === "gemini" ? "AIza…"
                              : "Your API key…"
                            }
                            className={`${INPUT_CLS} min-w-0 flex-1 font-mono`}
                          />
                          <button
                            type="button"
                            onClick={() => runTest(formData, null)}
                            disabled={testingId === "__form__"}
                            className="inline-flex h-10 shrink-0 items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-4 text-sm font-semibold text-sky-700 transition hover:bg-sky-100 disabled:opacity-50 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
                          >
                            {testingId === "__form__"
                              ? <Loader2 className="h-4 w-4 animate-spin" />
                              : <Wifi    className="h-4 w-4" />}
                            {testingId === "__form__" ? "Testing…" : "Test"}
                          </button>
                        </div>
                      </StableField>

                      <div className="rounded-xl border border-violet-200 bg-violet-50/60 p-4 dark:border-violet-700/40 dark:bg-violet-950/20">
                        <label className="flex cursor-pointer items-start gap-3">
                          <input
                            type="checkbox"
                            checked={!!formData.isGlobalOverride}
                            onChange={(e) => setFormData((p) => ({ ...p, isGlobalOverride: e.target.checked }))}
                            className="mt-0.5 h-4 w-4 rounded accent-violet-600"
                          />
                          <div>
                            <p className="text-sm font-bold text-violet-800 dark:text-violet-200">
                              Use this configuration for all roles
                            </p>
                            <p className="mt-0.5 text-xs text-violet-700 dark:text-violet-400">
                              When checked, this API becomes the sole provider for every task.
                            </p>
                          </div>
                        </label>
                      </div>

                      <div className="flex gap-2 pt-1">
                        <button
                          type="button"
                          onClick={closeAddConnectionModal}
                          className="inline-flex flex-1 items-center justify-center gap-2 rounded-xl border border-border py-3 text-sm font-semibold transition hover:bg-muted"
                        >
                          Cancel
                        </button>
                        <button
                          type="submit"
                          disabled={savingForm}
                          className="inline-flex flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 py-3 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:opacity-50"
                        >
                          <Save className="h-4 w-4" />
                          Save connection
                        </button>
                      </div>
                    </form>
                  </div>
                </div>
              )}

              {/* Role Assignment */}
              {configs.length > 0 && (
                <div
                  className={[
                    "rounded-2xl border bg-card p-6 shadow-sm transition",
                    isOverridden ? "border-border opacity-60 pointer-events-none select-none" : "border-border",
                  ].join(" ")}
                >
                  <div className="mb-4">
                    <div className="flex items-center gap-2">
                      <Globe className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                      <h2 className="text-base font-bold">Role Assignment</h2>
                      {isOverridden && (
                        <span className="ml-1 inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-[10px] font-bold text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                          <Zap className="h-3 w-3" />Overridden by Global Provider
                        </span>
                      )}
                    </div>
                    <p className="mt-1 text-xs text-muted-foreground">
                      Choose which saved API handles each task type.
                    </p>
                  </div>

                  <div className="grid gap-5 sm:grid-cols-2">
                    <div>
                      <label className={LABEL_CLS}>Default General API</label>
                      <p className={HINT_CLS}>Used for general chat, decision-making, and summaries.</p>
                      <select
                        value={assignments.defaultGeneral ?? ""}
                        onChange={(e) =>
                          setAssignments((p) => ({ ...p, defaultGeneral: e.target.value || null }))
                        }
                        disabled={isOverridden}
                        className={SELECT_CLS}
                      >
                        <option value="">— Not assigned —</option>
                        {configs.map((c) => (
                          <option key={c.id} value={c.id}>
                            {c.providerName || PROVIDER_LABELS[c.provider]}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className={LABEL_CLS}>Default OCR / Vision API</label>
                      <p className={HINT_CLS}>Used for packing-slip photos and image analysis.</p>
                      <select
                        value={assignments.defaultVision ?? ""}
                        onChange={(e) =>
                          setAssignments((p) => ({ ...p, defaultVision: e.target.value || null }))
                        }
                        disabled={isOverridden}
                        className={SELECT_CLS}
                      >
                        <option value="">— Not assigned —</option>
                        {configs.map((c) => (
                          <option key={c.id} value={c.id}>
                            {c.providerName || PROVIDER_LABELS[c.provider]}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {!isOverridden && (
                    <div className="mt-5">
                      <button
                        type="button"
                        onClick={handleSaveAssignments}
                        className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-sky-700"
                      >
                        <Save className="h-4 w-4" />
                        Save Role Assignments
                      </button>
                    </div>
                  )}
                </div>
              )}

              {configs.length > 0 && (
                <div className="flex justify-end">
                  <button
                    type="button"
                    onClick={handleClearAll}
                    className="inline-flex items-center gap-2 rounded-xl border border-rose-200 bg-rose-50 px-4 py-2.5 text-sm font-semibold text-rose-600 transition hover:bg-rose-100 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                  >
                    <Trash2 className="h-4 w-4" />
                    Clear All Configurations
                  </button>
                </div>
              )}

              <div className="border-t border-border pt-8">
                <p className="mb-4 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Workspace &amp; external integrations
                </p>
                <AgentApiKeysSection showToast={showToast} />
              </div>
            </div>
          )}

          {/* ══════════════ HARDWARE SCANNERS ══════════════ */}
          {activeTab === "hardware" && (
            <form onSubmit={handleSaveHardware} className="space-y-6">
              <div className="space-y-6 rounded-2xl border border-border bg-card p-6 shadow-sm">

                <div>
                  <div className="mb-2 flex items-center gap-2">
                    <ScanLine className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                    <label className="text-sm font-semibold">Barcode Input Mode</label>
                  </div>
                  <p className={HINT_CLS}>
                    Physical / keyboard-wedge scanners (e.g. Netum C750) emulate a keyboard. Select
                    &quot;Web / Camera&quot; only if you use a browser-based QR camera scanner.
                  </p>
                  <select
                    value={barcodeMode}
                    onChange={(e) => setBarcodeMode(e.target.value as BarcodeMode)}
                    className={SELECT_CLS}
                  >
                    <option value="physical">Physical Scanner  (Keyboard Emulation — Netum, Honeywell, Zebra…)</option>
                    <option value="camera">Web / Camera Scanner</option>
                  </select>
                  {barcodeMode === "physical" && (
                    <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                      <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                      Keyboard-wedge mode — &quot;Scan&quot; buttons show a ready indicator; no camera window.
                    </p>
                  )}
                  {barcodeMode === "camera" && (
                    <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-sky-600 dark:text-sky-400">
                      <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                      Camera mode — &quot;Scan&quot; buttons open a live QR/barcode camera modal.
                    </p>
                  )}
                </div>

                <div>
                  <div className="mb-2 flex items-center gap-2">
                    <Printer className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                    <label className="text-sm font-semibold">Default Label Printer</label>
                  </div>
                  <p className={HINT_CLS}>Select the default printer for shipping and return labels.</p>
                  <select
                    value={labelPrinter}
                    onChange={(e) => setLabelPrinter(e.target.value as LabelPrinter)}
                    className={SELECT_CLS}
                  >
                    <option value="system">System Default</option>
                    <option value="zebra_zd410">Zebra ZD410</option>
                    <option value="zebra_zd620">Zebra ZD620</option>
                    <option value="brother_ql">Brother QL Series</option>
                  </select>
                </div>

                <div className="rounded-xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-700/50 dark:bg-sky-950/40">
                  <p className="text-xs font-semibold text-sky-700 dark:text-sky-300">
                    Physical Scanner Hook  (usePhysicalScanner)
                  </p>
                  <p className="mt-1 text-xs text-sky-600 dark:text-sky-400">
                    Per-input{" "}
                    <code className="rounded bg-sky-100 px-1 dark:bg-sky-900">keydown</code>{" "}
                    listeners measure inter-keystroke timing. Wired into the{" "}
                    <strong>Item</strong>, <strong>Package</strong>, and <strong>RMA</strong> inputs.
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                <button
                  type="submit"
                  className="inline-flex min-w-[180px] flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 py-3 text-sm font-semibold text-white transition hover:bg-sky-700"
                >
                  <Save className="h-4 w-4" />
                  Save Hardware Settings
                </button>
              </div>
              {hwSaved && (
                <p className="text-sm font-medium text-emerald-600 dark:text-emerald-400">Saved.</p>
              )}
            </form>
          )}

          {/* ══════════════ INVENTORY & FEFO ══════════════ */}
          {activeTab === "inventory_fefo" && (
            <div className="space-y-6">

              {/* Module header */}
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-orange-100 dark:bg-orange-950/50">
                  <Package className="h-5 w-5 text-orange-600 dark:text-orange-400" />
                </div>
                <div>
                  <h2 className="text-base font-bold">Inventory &amp; FEFO Settings</h2>
                  <p className="text-xs text-muted-foreground">
                    Configure First-Expired, First-Out expiry threshold rules for your warehouse.
                  </p>
                </div>
              </div>

              {/* Paywall banner for Free Tier */}
              {mockPlan === "Free Tier" && (
                <div className="relative overflow-hidden rounded-2xl border-2 border-amber-300 bg-amber-50 px-6 py-5 dark:border-amber-600/50 dark:bg-amber-950/20">
                  <div className="flex items-start gap-4">
                    <Crown className="mt-0.5 h-6 w-6 shrink-0 text-amber-500" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-bold text-amber-900 dark:text-amber-200">
                        Pro Feature — Upgrade to Customise FEFO Rules
                      </p>
                      <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">
                        Free Tier uses standard 30-day critical / 90-day warning thresholds.
                        Upgrade to <strong>Pro</strong> or <strong>Enterprise</strong> to set custom expiry thresholds
                        per your product categories.
                      </p>
                      <button
                        type="button"
                        onClick={() => setActiveTab("billing")}
                        className="mt-3 inline-flex items-center gap-2 rounded-xl bg-amber-500 px-4 py-2 text-sm font-bold text-white transition hover:bg-amber-600"
                      >
                        <Zap className="h-3.5 w-3.5" />
                        Upgrade Plan
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* FEFO Config card */}
              <form onSubmit={handleSaveFefo}>
                <div className={[
                  "relative rounded-2xl border border-border bg-card p-6 shadow-sm space-y-6 transition-all",
                  mockPlan === "Free Tier" ? "opacity-50 pointer-events-none select-none" : "",
                ].join(" ")}>

                  {fefoLoading ? (
                    <div className="flex items-center gap-2 py-4">
                      <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      <span className="text-sm text-muted-foreground">Loading FEFO settings…</span>
                    </div>
                  ) : (
                    <>
                      <div>
                        <h3 className="text-sm font-bold">Expiry Threshold Rules</h3>
                        <p className="text-xs text-muted-foreground mt-0.5">
                          These values control the colour-coding on the Expiry Date column in the Items table.
                          Settings are stored in the <code className="rounded bg-muted px-1 font-mono text-[11px]">workspace_settings</code> database table.
                        </p>
                      </div>

                      {/* Current thresholds visualization */}
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="inline-flex items-center gap-1.5 rounded-full border border-red-200 bg-red-50 px-3 py-1 text-xs font-bold text-red-700 dark:border-red-700/60 dark:bg-red-950/40 dark:text-red-300">
                          <span className="h-2 w-2 rounded-full bg-red-500" />
                          🔴 Critical ≤ {fefoSettings.fefo_critical_days}d
                        </span>
                        <span className="text-xs text-muted-foreground">→</span>
                        <span className="inline-flex items-center gap-1.5 rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-bold text-amber-700 dark:border-amber-700/60 dark:bg-amber-950/40 dark:text-amber-300">
                          <span className="h-2 w-2 rounded-full bg-amber-400" />
                          🟡 Warning ≤ {fefoSettings.fefo_warning_days}d
                        </span>
                        <span className="text-xs text-muted-foreground">→</span>
                        <span className="inline-flex items-center gap-1.5 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-bold text-emerald-700 dark:border-emerald-700/60 dark:bg-emerald-950/40 dark:text-emerald-300">
                          <span className="h-2 w-2 rounded-full bg-emerald-500" />
                          🟢 OK &gt; {fefoSettings.fefo_warning_days}d
                        </span>
                      </div>

                      <div className="grid gap-5 sm:grid-cols-2">

                        {/* Critical days */}
                        <div className="flex flex-col">
                          <div className="flex-1">
                            <label className={LABEL_CLS}>
                              🔴 Critical Expiry (Days)
                            </label>
                            <p className={`${HINT_CLS} min-h-[2.5rem]`}>
                              Items expiring within this many days are flagged <strong>Critical</strong> (red).
                            </p>
                          </div>
                          <input
                            type="number"
                            min={1}
                            max={fefoLocalEdit.fefo_warning_days - 1}
                            value={fefoLocalEdit.fefo_critical_days}
                            onChange={(e) =>
                              setFefoLocalEdit((p) => ({ ...p, fefo_critical_days: Number(e.target.value) }))
                            }
                            className={INPUT_CLS}
                          />
                        </div>

                        {/* Warning days */}
                        <div className="flex flex-col">
                          <div className="flex-1">
                            <label className={LABEL_CLS}>
                              🟡 Warning Expiry (Days)
                            </label>
                            <p className={`${HINT_CLS} min-h-[2.5rem]`}>
                              Items expiring within this many days (but above Critical) are flagged <strong>Warning</strong> (amber).
                            </p>
                          </div>
                          <input
                            type="number"
                            min={fefoLocalEdit.fefo_critical_days + 1}
                            value={fefoLocalEdit.fefo_warning_days}
                            onChange={(e) =>
                              setFefoLocalEdit((p) => ({ ...p, fefo_warning_days: Number(e.target.value) }))
                            }
                            className={INPUT_CLS}
                          />
                        </div>
                      </div>

                      <div className="rounded-xl border border-orange-200 bg-orange-50/60 px-4 py-3 dark:border-orange-700/40 dark:bg-orange-950/20">
                        <p className="text-xs font-semibold text-orange-800 dark:text-orange-200">FEFO Compliance</p>
                        <p className="mt-1 text-xs text-orange-700 dark:text-orange-400">
                          First-Expired, First-Out rules ensure perishable inventory is dispatched before
                          expiry. These thresholds are applied dynamically to the Items table in real time.
                          Changes take effect immediately on all users.
                        </p>
                      </div>

                      <div className="flex gap-3">
                        <button
                          type="submit"
                          disabled={fefoSaving}
                          className="inline-flex items-center gap-2 rounded-xl bg-orange-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-orange-700 disabled:opacity-50"
                        >
                          {fefoSaving ? (
                            <><Loader2 className="h-4 w-4 animate-spin" />Saving…</>
                          ) : (
                            <><Save className="h-4 w-4" />Save FEFO Rules</>
                          )}
                        </button>
                        <button
                          type="button"
                          onClick={() => setFefoLocalEdit(fefoSettings)}
                          className="inline-flex items-center gap-2 rounded-xl border border-border bg-muted/50 px-4 py-2.5 text-sm font-semibold text-muted-foreground transition hover:bg-accent"
                        >
                          Reset
                        </button>
                      </div>
                    </>
                  )}
                </div>
              </form>

              {/* Info card — JSONB architecture note */}
              <div className="rounded-2xl border border-violet-200 bg-violet-50/60 px-5 py-4 dark:border-violet-700/40 dark:bg-violet-950/20 space-y-1">
                <p className="flex items-center gap-2 text-sm font-bold text-violet-800 dark:text-violet-200">
                  <Zap className="h-4 w-4" />
                  Scalable JSONB Architecture
                </p>
                <p className="text-xs text-violet-700 dark:text-violet-400">
                  All module settings are stored in a single <code className="rounded bg-violet-100 px-1 font-mono text-[11px] dark:bg-violet-900/30">module_configs JSONB</code> column.
                  Adding new module settings (Claims, Shipping, etc.) never requires a schema migration — just
                  add new keys to the JSON object.
                </p>
              </div>

            </div>
          )}

          {/* ══════════════ CLAIM ENGINE (Placeholder) ══════════════ */}
          {activeTab === "claim_engine" && (
            <div className="space-y-6">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-rose-100 dark:bg-rose-950/50">
                  <ShieldCheck className="h-5 w-5 text-rose-600 dark:text-rose-400" />
                </div>
                <div>
                  <h2 className="text-base font-bold">Claim Engine Settings</h2>
                  <p className="text-xs text-muted-foreground">Configure automated claim rules and escalation logic.</p>
                </div>
              </div>

              {mockPlan === "Free Tier" && (
                <div className="rounded-2xl border-2 border-amber-300 bg-amber-50 px-6 py-5 dark:border-amber-600/50 dark:bg-amber-950/20">
                  <div className="flex items-start gap-4">
                    <Crown className="mt-0.5 h-6 w-6 shrink-0 text-amber-500" />
                    <div>
                      <p className="text-sm font-bold text-amber-900 dark:text-amber-200">
                        Pro Feature — Upgrade to Unlock Claim Automation
                      </p>
                      <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">
                        The Claim Engine module is available on Pro and Enterprise plans.
                      </p>
                      <button
                        type="button"
                        onClick={() => setActiveTab("billing")}
                        className="mt-3 inline-flex items-center gap-2 rounded-xl bg-amber-500 px-4 py-2 text-sm font-bold text-white transition hover:bg-amber-600"
                      >
                        <Zap className="h-3.5 w-3.5" />
                        Upgrade Plan
                      </button>
                    </div>
                  </div>
                </div>
              )}

              <form
                onSubmit={handleSaveClaimEvidence}
                className={[
                  "space-y-6 transition-all",
                  mockPlan === "Free Tier" ? "opacity-50 pointer-events-none select-none" : "",
                ].join(" ")}
              >
                <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-4">
                  <div className="flex items-start gap-3">
                    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-emerald-100 dark:bg-emerald-950/50">
                      <ImageIcon className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <h3 className="text-sm font-bold text-foreground">Default Claim Evidence</h3>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        Choose which inherited photos are pre-selected when generating a claim PDF. Stored in{" "}
                        <code className="rounded bg-muted px-1 font-mono text-[10px]">organization_settings.default_claim_evidence</code> (JSONB).
                      </p>
                    </div>
                  </div>
                  {claimEvidenceLoading ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Loading…
                    </div>
                  ) : (
                    <div className="grid gap-3 sm:grid-cols-2">
                      {(Object.keys(CLAIM_EVIDENCE_KEY_LABELS) as ClaimEvidenceKey[]).map((key) => (
                        <label key={key} className="flex cursor-pointer items-start gap-3 rounded-xl border border-border bg-muted/10 p-3">
                          <input
                            type="checkbox"
                            className="mt-0.5 h-4 w-4 rounded accent-emerald-600"
                            checked={claimEvidenceLocal[key] ?? false}
                            onChange={(e) =>
                              setClaimEvidenceLocal((p) => ({ ...p, [key]: e.target.checked }))
                            }
                          />
                          <span className="text-sm font-medium text-foreground">{CLAIM_EVIDENCE_KEY_LABELS[key]}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  <div className="flex flex-wrap gap-2 border-t border-border pt-4">
                    <button
                      type="submit"
                      disabled={claimEvidenceSaving || claimEvidenceLoading || mockPlan === "Free Tier"}
                      className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-emerald-500 disabled:opacity-50"
                    >
                      {claimEvidenceSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                      Save evidence defaults
                    </button>
                  </div>
                </div>
              </form>

              <form
                onSubmit={handleSaveClaimAgent}
                className={[
                  "space-y-6 transition-all",
                  mockPlan === "Free Tier" ? "opacity-50 pointer-events-none select-none" : "",
                ].join(" ")}
              >
                <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-5">
                  <div className="flex items-start gap-3">
                    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-sky-100 dark:bg-sky-950/50">
                      <Truck className="h-5 w-5 text-sky-600 dark:text-sky-400" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <h3 className="text-sm font-bold text-foreground">Logistics AI Agent</h3>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        Align the claim queue with returns marked ready for claim. Manual sync enqueues missing submissions; background
                        processing uses the interval below (for cron or worker hints).
                      </p>
                    </div>
                  </div>

                  {logisticsSyncLoading ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Checking queue…
                    </div>
                  ) : (
                    <div className="flex flex-col gap-3 rounded-xl border border-border bg-muted/10 p-4 sm:flex-row sm:items-center sm:justify-between">
                      <div className="text-xs text-muted-foreground">
                        {logisticsSyncStatus ? (
                          logisticsSyncStatus.systemUpToDate ? (
                            <span className="font-semibold text-emerald-600 dark:text-emerald-400">System up to date</span>
                          ) : (
                            <span>
                              <span className="font-semibold tabular-nums text-foreground">
                                {logisticsSyncStatus.pendingSyncCount}
                              </span>{" "}
                              return(s) need a queue row
                              {typeof logisticsSyncStatus.readyForClaimCount === "number" ? (
                                <span className="text-muted-foreground">
                                  {" "}
                                  ({logisticsSyncStatus.readyForClaimCount} ready for claim total)
                                </span>
                              ) : null}
                            </span>
                          )
                        ) : (
                          <span>Queue status unavailable.</span>
                        )}
                      </div>
                      <button
                        type="button"
                        disabled={
                          mockPlan === "Free Tier" ||
                          logisticsSyncBusy ||
                          logisticsSyncLoading ||
                          (logisticsSyncStatus?.systemUpToDate ?? false)
                        }
                        onClick={() => void handleLogisticsSyncNow()}
                        className="inline-flex shrink-0 items-center justify-center gap-2 rounded-xl bg-sky-600 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-sky-500 disabled:opacity-50"
                      >
                        {logisticsSyncBusy ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <RefreshCw className="h-4 w-4" />
                        )}
                        {logisticsSyncStatus?.systemUpToDate ? "System up to date" : "Sync now"}
                      </button>
                    </div>
                  )}

                  <div className="space-y-4 rounded-xl border border-border bg-muted/10 p-4">
                    <label className="flex cursor-pointer items-start gap-3">
                      <input
                        type="checkbox"
                        className="mt-1 h-4 w-4 rounded accent-sky-600"
                        checked={claimAgentLocal.logistics_background_sync_enabled ?? false}
                        onChange={(e) =>
                          setClaimAgentLocal((p) => ({
                            ...p,
                            logistics_background_sync_enabled: e.target.checked,
                          }))
                        }
                      />
                      <span>
                        <span className="block text-sm font-semibold">Background processing</span>
                        <span className="text-xs text-muted-foreground">
                          When enabled, periodic sync runs can enqueue missing claim submissions (configure your scheduler using the
                          interval below).
                        </span>
                      </span>
                    </label>
                    <div>
                      <label className={LABEL_CLS}>Sync frequency (hours)</label>
                      <p className={HINT_CLS}>How often background sync should run (e.g. 2 = every 2 hours). Range 1–168.</p>
                      <input
                        type="number"
                        min={1}
                        max={168}
                        step={1}
                        value={claimAgentLocal.logistics_sync_interval_hours ?? 2}
                        onChange={(e) =>
                          setClaimAgentLocal((p) => ({
                            ...p,
                            logistics_sync_interval_hours: Number(e.target.value),
                          }))
                        }
                        className={`${INPUT_CLS} mt-2 max-w-[140px] font-mono tabular-nums`}
                      />
                    </div>
                  </div>
                </div>

                <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-6">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                  <div className="flex items-start gap-3">
                    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-violet-100 dark:bg-violet-950/50">
                      <Cpu className="h-5 w-5 text-violet-600 dark:text-violet-400" />
                    </div>
                    <div>
                      <h3 className="text-sm font-bold text-foreground">Agent Control</h3>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        White-label automation: PDF generation and optional direct marketplace submission limits.
                        Stored in <code className="rounded bg-muted px-1 font-mono text-[10px]">module_configs.claim_agent_config</code>.
                      </p>
                    </div>
                  </div>
                </div>

                {claimAgentLoading ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading agent settings…
                  </div>
                ) : (
                  <>
                    <div className="space-y-4 rounded-xl border border-border bg-muted/10 p-4">
                      <label className="flex cursor-pointer items-start gap-3">
                        <input
                          type="checkbox"
                          className="mt-1 h-4 w-4 rounded accent-rose-600"
                          checked={claimAgentLocal.auto_generate_pdf_reports ?? true}
                          onChange={(e) =>
                            setClaimAgentLocal((p) => ({ ...p, auto_generate_pdf_reports: e.target.checked }))
                          }
                        />
                        <span>
                          <span className="block text-sm font-semibold">Auto-generate PDF reports</span>
                          <span className="text-xs text-muted-foreground">
                            When enabled, daily claim PDFs are generated automatically for eligible submissions (default: on).
                          </span>
                        </span>
                      </label>

                      <label className="flex cursor-pointer items-start gap-3 border-t border-border pt-4">
                        <input
                          type="checkbox"
                          className="mt-1 h-4 w-4 rounded accent-rose-600"
                          checked={claimAgentLocal.allow_agent_direct_submit ?? false}
                          onChange={(e) =>
                            setClaimAgentLocal((p) => ({ ...p, allow_agent_direct_submit: e.target.checked }))
                          }
                        />
                        <span>
                          <span className="block text-sm font-semibold">Allow agent to submit directly to marketplace</span>
                          <span className="text-xs text-muted-foreground">
                            Permits automated filing where supported; keep off for manual review (default: off).
                          </span>
                        </span>
                      </label>

                      <label className="flex cursor-pointer items-start gap-3 border-t border-border pt-4">
                        <input
                          type="checkbox"
                          className="mt-1 h-4 w-4 rounded accent-rose-600"
                          checked={claimAgentLocal.autonomous_claim_submission_0_50_usd ?? false}
                          onChange={(e) =>
                            setClaimAgentLocal((p) => ({
                              ...p,
                              autonomous_claim_submission_0_50_usd: e.target.checked,
                            }))
                          }
                        />
                        <span>
                          <span className="block text-sm font-semibold">Autonomous Claim Submission ($0–$50)</span>
                          <span className="text-xs text-muted-foreground">
                            When enabled with direct submit, the agent may file small claims automatically (default: off).
                          </span>
                        </span>
                      </label>

                      <label className="flex cursor-pointer items-start gap-3 border-t border-border pt-4">
                        <input
                          type="checkbox"
                          className="mt-1 h-4 w-4 rounded accent-rose-600"
                          checked={claimAgentLocal.require_manual_approval_bulk_submission ?? true}
                          onChange={(e) =>
                            setClaimAgentLocal((p) => ({
                              ...p,
                              require_manual_approval_bulk_submission: e.target.checked,
                            }))
                          }
                        />
                        <span>
                          <span className="block text-sm font-semibold">Require Manual Approval for Bulk Submission</span>
                          <span className="text-xs text-muted-foreground">
                            Extra guardrail for white-label tenants before bulk marketplace actions (default: on).
                          </span>
                        </span>
                      </label>
                    </div>

                    <div>
                      <label className={LABEL_CLS}>
                        Maximum claim amount for auto-submit (USD)
                      </label>
                      <p className={HINT_CLS}>
                        Claims at or below this amount may be auto-submitted when direct submit is allowed. Range $0–$50,000.
                      </p>
                      <div className="mt-2 flex flex-col gap-3 sm:flex-row sm:items-center">
                        <input
                          type="range"
                          min={0}
                          max={50000}
                          step={50}
                          value={Math.min(
                            50000,
                            Math.max(0, Number(claimAgentLocal.max_auto_submit_amount_usd ?? 500)),
                          )}
                          onChange={(e) =>
                            setClaimAgentLocal((p) => ({
                              ...p,
                              max_auto_submit_amount_usd: Number(e.target.value),
                            }))
                          }
                          className="h-2 w-full max-w-md cursor-pointer accent-rose-600"
                        />
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-mono font-semibold tabular-nums">
                            ${Number(claimAgentLocal.max_auto_submit_amount_usd ?? 500).toLocaleString("en-US")}
                          </span>
                          <input
                            type="number"
                            min={0}
                            max={50000}
                            step={1}
                            value={claimAgentLocal.max_auto_submit_amount_usd ?? 500}
                            onChange={(e) =>
                              setClaimAgentLocal((p) => ({
                                ...p,
                                max_auto_submit_amount_usd: Number(e.target.value),
                              }))
                            }
                            className={`${INPUT_CLS} w-28 font-mono`}
                          />
                        </div>
                      </div>
                    </div>

                    <div className="flex flex-wrap gap-2">
                      <button
                        type="submit"
                        disabled={claimAgentSaving}
                        className="inline-flex items-center gap-2 rounded-xl bg-rose-600 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-rose-500 disabled:opacity-50"
                      >
                        {claimAgentSaving ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Save className="h-4 w-4" />
                        )}
                        Save agent settings
                      </button>
                      <button
                        type="button"
                        disabled={claimAgentSaving}
                        onClick={() => setClaimAgentLocal(claimAgentSaved)}
                        className="inline-flex items-center gap-2 rounded-xl border border-border px-4 py-2.5 text-sm font-semibold text-muted-foreground transition hover:bg-accent disabled:opacity-50"
                      >
                        <RotateCcw className="h-4 w-4" />
                        Reset
                      </button>
                    </div>
                  </>
                )}
                </div>
              </form>
            </div>
          )}

          {/* ══════════════ BILLING & SUBSCRIPTION ══════════════ */}
          {activeTab === "billing" && (
            <div className="space-y-6">

              {/* Plan card + switcher */}
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-5">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h2 className="text-base font-bold">Billing &amp; Subscription</h2>
                    <p className="text-xs text-muted-foreground mt-0.5">
                      Monitor usage quotas and manage your workspace plan.
                    </p>
                  </div>
                  <span
                    className={[
                      "shrink-0 inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-bold",
                      mockPlan === "Free Tier"
                        ? "border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
                        : mockPlan === "Pro Tier"
                        ? "border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
                        : "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300",
                    ].join(" ")}
                  >
                    {mockPlan === "Enterprise" && <Zap className="h-3.5 w-3.5" />}
                    {mockPlan}
                  </span>
                </div>

                <div>
                  <label className={LABEL_CLS}>Test Plan (Local Mock)</label>
                  <p className={HINT_CLS}>
                    Switch plans to preview paywalls and feature gating. Saved in this browser only.
                  </p>
                  <select
                    value={mockPlan}
                    onChange={(e) => {
                      const plan = e.target.value as SaasPlan;
                      setMockPlan(plan);
                      localStorage.setItem("mock_saas_plan", plan);
                      showToast(`Plan switched to ${plan}. Paywalls updated.`, true);
                    }}
                    className={SELECT_CLS}
                  >
                    {PLANS.map((p) => (
                      <option key={p} value={p}>{p}</option>
                    ))}
                  </select>
                </div>

                {/* Plan comparison quick-view */}
                <div className="grid gap-3 sm:grid-cols-3">
                  {(["Free Tier", "Pro Tier", "Enterprise"] as SaasPlan[]).map((p) => (
                    <div
                      key={p}
                      className={[
                        "rounded-xl border p-3 transition",
                        mockPlan === p
                          ? "border-violet-300 bg-violet-50 dark:border-violet-600/50 dark:bg-violet-950/30"
                          : "border-border bg-background",
                      ].join(" ")}
                    >
                      <p className={`text-xs font-bold ${mockPlan === p ? "text-violet-700 dark:text-violet-300" : "text-foreground"}`}>
                        {p}
                      </p>
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        {PLAN_LIMITS[p].ai_calls === Infinity ? "∞" : PLAN_LIMITS[p].ai_calls.toLocaleString()} AI calls ·{" "}
                        {PLAN_LIMITS[p].stores === Infinity ? "∞" : PLAN_LIMITS[p].stores} store{PLAN_LIMITS[p].stores !== 1 ? "s" : ""}
                      </p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Usage meters */}
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-6">
                <div className="flex items-center gap-2">
                  <BarChart3 className="h-5 w-5 text-violet-600 dark:text-violet-400" />
                  <h3 className="text-base font-bold">Usage This Month</h3>
                </div>

                <UsageMeter
                  label="AI Invoices Scanned"
                  used={MOCK_AI_CALLS}
                  limit={planLimits.ai_calls}
                  color="violet"
                  hint="Packing slip OCR + product label scans via AI vision"
                />

                <UsageMeter
                  label="Connected Stores"
                  used={storesList.length}
                  limit={planLimits.stores}
                  color="sky"
                  hint="Active marketplace integrations"
                />

                <UsageMeter
                  label="Items Processed"
                  used={MOCK_SCANNED_ITEMS}
                  limit={planLimits.items}
                  color="emerald"
                  hint="Total return items scanned and logged this billing period"
                />
              </div>

              {/* Upgrade CTA */}
              {mockPlan !== "Enterprise" && (
                <div className="rounded-2xl border-2 border-violet-200 bg-violet-50/60 p-6 dark:border-violet-700/40 dark:bg-violet-950/20 space-y-3">
                  <div className="flex items-center gap-2">
                    <Zap className="h-5 w-5 text-violet-600 dark:text-violet-400" />
                    <h3 className="text-base font-bold text-violet-800 dark:text-violet-200">
                      {mockPlan === "Free Tier" ? "Upgrade to Pro" : "Upgrade to Enterprise"}
                    </h3>
                  </div>
                  <p className="text-sm text-violet-700 dark:text-violet-400">
                    {mockPlan === "Free Tier"
                      ? "Get 5 stores, 5,000 AI calls/month, priority support, and advanced analytics."
                      : "Unlimited stores, unlimited AI calls, dedicated account manager, custom SLAs, and SSO."}
                  </p>
                  <div className="flex flex-wrap gap-3">
                    <button
                      type="button"
                      onClick={() => showToast("Contact sales@example.com to upgrade your plan.", true)}
                      className="inline-flex items-center gap-2 rounded-xl bg-violet-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-700"
                    >
                      <Zap className="h-4 w-4" />
                      Upgrade Plan
                    </button>
                    <button
                      type="button"
                      onClick={() => showToast("Sales team will reach out within 24 hours.", true)}
                      className="inline-flex items-center gap-2 rounded-xl border border-violet-300 bg-white px-5 py-2.5 text-sm font-semibold text-violet-700 transition hover:bg-violet-50 dark:border-violet-600/50 dark:bg-transparent dark:text-violet-300"
                    >
                      Talk to Sales
                    </button>
                  </div>
                </div>
              )}

              {mockPlan === "Enterprise" && (
                <div className="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-emerald-50 px-5 py-4 dark:border-emerald-700/50 dark:bg-emerald-950/30">
                  <CheckCircle2 className="h-5 w-5 shrink-0 text-emerald-600 dark:text-emerald-400" />
                  <div>
                    <p className="text-sm font-bold text-emerald-800 dark:text-emerald-200">
                      You&apos;re on Enterprise — unlimited everything.
                    </p>
                    <p className="text-xs text-emerald-700 dark:text-emerald-400">
                      No quotas, dedicated infrastructure, and a named account manager.
                    </p>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* ══════════════ RETURNS PROCESSING (placeholder) ══════════════ */}
          {activeTab === "returns_processing" && (
            <div className="space-y-6">
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-4">
                <div className="flex items-start gap-3">
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-rose-100 dark:bg-rose-950/50">
                    <RotateCcw className="h-5 w-5 text-rose-600 dark:text-rose-400" />
                  </div>
                  <div>
                    <h2 className="text-base font-bold">Returns Processing</h2>
                    <p className="text-xs text-muted-foreground mt-0.5">
                      Granular configuration for the Returns Intelligence module — SLA rules,
                      disposition workflows, fraud detection thresholds, and auto-routing logic.
                    </p>
                  </div>
                </div>

                <div className="flex items-start gap-3 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 dark:border-amber-700/40 dark:bg-amber-950/20">
                  <Zap className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                  <div>
                    <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">
                      Module Settings — Coming in Phase 1
                    </p>
                    <p className="mt-0.5 text-xs text-amber-700 dark:text-amber-400">
                      Settings for SLA day limits per carrier, return reason code mappings,
                      condition grading rules, and HITL approval thresholds will be
                      configured here and persisted to{" "}
                      <code className="rounded bg-amber-100 px-1 font-mono text-[11px] dark:bg-amber-900/40">
                        module_configs-&gt;returns
                      </code>.
                    </p>
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  {[
                    { label: "SLA Day Limits",          desc: "Per-carrier return SLA thresholds (Amazon, Walmart, eBay).",  icon: <RotateCcw className="h-4 w-4 text-rose-500" /> },
                    { label: "Disposition Rules",        desc: "Auto-route by condition: Restock, Liquidate, Destroy.",       icon: <PackageX  className="h-4 w-4 text-orange-500" /> },
                    { label: "Fraud Detection",          desc: "Configurable anomaly score thresholds for claim reviews.",    icon: <ShieldAlert className="h-4 w-4 text-amber-500" /> },
                    { label: "HITL Approval Workflow",   desc: "Define which actions require human-in-the-loop sign-off.",   icon: <UserCog   className="h-4 w-4 text-sky-500" /> },
                  ].map((item) => (
                    <div
                      key={item.label}
                      className="flex items-start gap-3 rounded-xl border border-dashed border-border bg-muted/20 p-4 opacity-60"
                    >
                      {item.icon}
                      <div>
                        <p className="text-sm font-semibold">{item.label}</p>
                        <p className="mt-0.5 text-xs text-muted-foreground">{item.desc}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* ══════════════ REPORTS & ANALYTICS (placeholder) ══════════════ */}
          {activeTab === "reports_analytics" && (
            <div className="space-y-6">
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-4">
                <div className="flex items-start gap-3">
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-sky-100 dark:bg-sky-950/50">
                    <BarChart3 className="h-5 w-5 text-sky-600 dark:text-sky-400" />
                  </div>
                  <div>
                    <h2 className="text-base font-bold">Reports &amp; Analytics</h2>
                    <p className="text-xs text-muted-foreground mt-0.5">
                      Configure custom table column preferences, default date ranges,
                      export formats, and scheduled report delivery. All preferences
                      are stored in{" "}
                      <code className="rounded bg-muted px-1 font-mono text-[11px]">module_configs-&gt;reports</code>.
                    </p>
                  </div>
                </div>

                <div className="flex items-start gap-3 rounded-xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-700/40 dark:bg-sky-950/20">
                  <BarChart3 className="mt-0.5 h-4 w-4 shrink-0 text-sky-600 dark:text-sky-400" />
                  <div>
                    <p className="text-sm font-semibold text-sky-800 dark:text-sky-200">
                      Column Config Storage — Ready
                    </p>
                    <p className="mt-0.5 text-xs text-sky-700 dark:text-sky-400">
                      The{" "}
                      <code className="rounded bg-sky-100 px-1 font-mono text-[11px] dark:bg-sky-900/40">
                        module_configs-&gt;reports
                      </code>{" "}
                      JSONB key has been seeded and is ready to store your custom table
                      column layouts once the Reports module is built.
                    </p>
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  {[
                    { label: "Table Column Preferences", desc: "Show/hide and reorder columns per report view.",             icon: <BarChart3  className="h-4 w-4 text-sky-500" /> },
                    { label: "Default Date Range",       desc: "Pre-select rolling windows (7d, 30d, 90d, custom).",        icon: <RefreshCw  className="h-4 w-4 text-slate-500" /> },
                    { label: "Export Formats",           desc: "Choose default export: CSV, XLSX, or PDF.",                 icon: <Package    className="h-4 w-4 text-emerald-500" /> },
                    { label: "Scheduled Delivery",       desc: "Email report digests on a defined cron schedule.",          icon: <Zap        className="h-4 w-4 text-violet-500" /> },
                  ].map((item) => (
                    <div
                      key={item.label}
                      className="flex items-start gap-3 rounded-xl border border-dashed border-border bg-muted/20 p-4 opacity-60"
                    >
                      {item.icon}
                      <div>
                        <p className="text-sm font-semibold">{item.label}</p>
                        <p className="mt-0.5 text-xs text-muted-foreground">{item.desc}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* ══════════════ TEAM & ROLES ══════════════ */}
          {activeTab === "team" && (
            <div className="space-y-6">
              <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-5">
                <div>
                  <h2 className="text-base font-bold">Team &amp; Roles</h2>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    Control which role has access to admin and warehouse features.
                    Role is stored locally in this browser session.
                  </p>
                </div>

                <div className="rounded-xl border border-border bg-muted/30 px-4 py-3 flex items-center gap-3">
                  <UserCog className="h-5 w-5 text-violet-600 dark:text-violet-400 shrink-0" />
                  <div>
                    <p className="text-sm font-semibold">Current Session Role</p>
                    <p className="text-xs text-muted-foreground capitalize">{role}</p>
                  </div>
                </div>

                <div className="space-y-3">
                  {(
                    [
                      { r: "admin",     label: "Admin",    desc: "Full access — settings, all actions, user management." },
                      { r: "warehouse", label: "Warehouse", desc: "Scan, pack, and process returns. No settings access." },
                      { r: "viewer",    label: "Viewer",    desc: "Read-only dashboard access. No mutations allowed." },
                    ] as { r: string; label: string; desc: string }[]
                  ).map(({ r, label, desc }) => (
                    <div
                      key={r}
                      className={[
                        "flex items-start gap-3 rounded-xl border p-4 transition",
                        role === r
                          ? "border-violet-300 bg-violet-50 dark:border-violet-600/50 dark:bg-violet-950/30"
                          : "border-border bg-background",
                      ].join(" ")}
                    >
                      <div
                        className={`mt-0.5 h-2.5 w-2.5 shrink-0 rounded-full ${
                          role === r ? "bg-violet-500" : "bg-slate-300 dark:bg-slate-600"
                        }`}
                      />
                      <div>
                        <p className="text-sm font-semibold">{label}</p>
                        <p className="text-xs text-muted-foreground">{desc}</p>
                      </div>
                      {role === r && (
                        <span className="ml-auto inline-flex items-center gap-1 rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-bold text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
                          Active
                        </span>
                      )}
                    </div>
                  ))}
                </div>

                <div className="rounded-xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-700/50 dark:bg-sky-950/40">
                  <p className="text-xs font-semibold text-sky-700 dark:text-sky-300">Role Switching (Dev / Demo)</p>
                  <p className="mt-1 text-xs text-sky-600 dark:text-sky-400">
                    In production, roles are enforced server-side via Supabase RLS policies.
                    Use the role switcher in the top header to change roles for testing.
                  </p>
                </div>
              </div>
            </div>
          )}

        </div>
        {/* end right content */}
      </div>
      {/* end layout */}

      {/* ── Add / Edit Store Modal ───────────────────────────────────────────── */}
      {showAddStoreModal && (
        <div
          className="fixed inset-0 z-[400] flex items-end justify-center sm:items-center bg-black/60 backdrop-blur-sm p-0 sm:p-4"
          onClick={(e) => {
            if (e.target === e.currentTarget) closeAddStoreModal();
          }}
        >
          <div className="w-full sm:w-[95vw] sm:max-w-xl overflow-y-auto max-h-[92dvh] sm:max-h-[88vh] rounded-t-3xl sm:rounded-2xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950 animate-in slide-in-from-bottom-4 duration-200">

            {/* Modal header */}
            <div className="sticky top-0 z-10 flex items-center justify-between border-b border-slate-200 bg-white px-5 py-4 dark:border-slate-700 dark:bg-slate-950">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Marketplaces &amp; Stores</p>
                <h2 className="mt-0.5 text-lg font-bold text-foreground">
                  {editingStoreId ? "Edit Store" : "Add New Store"}
                </h2>
              </div>
              <button
                type="button"
                onClick={closeAddStoreModal}
                className="rounded-full p-2 text-slate-400 hover:bg-accent hover:text-accent-foreground transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Modal form */}
            <form onSubmit={handleAddStore} className="relative space-y-5 p-5">
              {storeModalCredLoading && (
                <div
                  className="absolute inset-0 z-20 flex items-center justify-center rounded-b-2xl bg-white/75 backdrop-blur-[1px] dark:bg-slate-950/75"
                  aria-busy
                >
                  <Loader2 className="h-8 w-8 animate-spin text-sky-600 dark:text-sky-400" />
                </div>
              )}

              {/* ── Basic Info ───────────────────────────────────────────── */}
              <div className="space-y-4">
                <div className="min-h-[6.5rem] space-y-1.5">
                  <label className={LABEL_CLS}>
                    Store Name <span className="text-rose-500">*</span>
                  </label>
                  <p className={`${HINT_CLS} min-h-[2.5rem]`}>A friendly label (e.g. &quot;My US Amazon Store&quot;).</p>
                  <input
                    type="text"
                    autoFocus
                    required
                    value={newStoreName}
                    onChange={(e) => setNewStoreName(e.target.value)}
                    placeholder="My Amazon Store…"
                    className={`${INPUT_CLS} h-10`}
                  />
                </div>

                <div className="min-h-[4.5rem] space-y-1.5">
                  <label className={LABEL_CLS}>
                    Platform <span className="text-rose-500">*</span>
                  </label>
                  <select
                    required
                    value={newStorePlatform}
                    disabled={!!editingStoreId}
                    onChange={(e) => {
                      const p = e.target.value;
                      setNewStorePlatform(p);
                      setNewStoreRegion("US");
                      if (p === "amazon") {
                        setNewStoreCredentials({
                          region: AMAZON_SP_API_DEFAULTS.region,
                          endpoint: AMAZON_SP_API_DEFAULTS.endpoint,
                        });
                      } else {
                        setNewStoreCredentials({});
                      }
                    }}
                    className={`${SELECT_CLS} h-10 ${editingStoreId ? "opacity-60 cursor-not-allowed" : ""}`}
                  >
                    <option value="amazon">Amazon</option>
                    <option value="walmart">Walmart</option>
                    <option value="ebay">eBay</option>
                    <option value="target">Target</option>
                    <option value="shopify">Shopify</option>
                    <option value="custom">Custom / Other</option>
                  </select>
                  <div className="min-h-[1.25rem]">
                    {editingStoreId && (
                      <p className="text-[11px] text-muted-foreground">Platform cannot be changed after creation.</p>
                    )}
                  </div>
                </div>

                {!editingStoreId && (
                  <div className="min-h-[6.5rem] space-y-1.5">
                    <label className={LABEL_CLS}>Marketplace Region</label>
                    <p className={`${HINT_CLS} min-h-[2.5rem]`}>The geographic marketplace where this store operates.</p>
                    <select
                      value={newStoreRegion}
                      onChange={(e) => setNewStoreRegion(e.target.value)}
                      className={`${SELECT_CLS} h-10`}
                    >
                      {newStorePlatform === "amazon" ? (
                        <>
                          <option value="US">United States (US)</option>
                          <option value="CA">Canada (CA)</option>
                          <option value="MX">Mexico (MX)</option>
                          <option value="UK">United Kingdom (UK)</option>
                          <option value="DE">Germany (DE)</option>
                          <option value="FR">France (FR)</option>
                          <option value="IT">Italy (IT)</option>
                          <option value="ES">Spain (ES)</option>
                          <option value="NL">Netherlands (NL)</option>
                          <option value="SE">Sweden (SE)</option>
                          <option value="PL">Poland (PL)</option>
                          <option value="TR">Turkey (TR)</option>
                          <option value="JP">Japan (JP)</option>
                          <option value="AU">Australia (AU)</option>
                          <option value="IN">India (IN)</option>
                          <option value="AE">UAE (AE)</option>
                          <option value="BR">Brazil (BR)</option>
                          <option value="SG">Singapore (SG)</option>
                        </>
                      ) : newStorePlatform === "walmart" ? (
                        <>
                          <option value="US">United States (US)</option>
                          <option value="CA">Canada (CA)</option>
                        </>
                      ) : (
                        <>
                          <option value="US">United States</option>
                          <option value="CA">Canada</option>
                          <option value="UK">United Kingdom</option>
                          <option value="AU">Australia</option>
                          <option value="EU">Europe</option>
                          <option value="GLOBAL">Global</option>
                        </>
                      )}
                    </select>
                  </div>
                )}
              </div>

              {/* ── Amazon SP-API (credentials JSONB) ───────────────────── */}
              {newStorePlatform === "amazon" && (
                <div className="rounded-xl border border-sky-200 bg-sky-50/50 p-4 dark:border-sky-700/50 dark:bg-sky-950/20">
                  <div className="mb-4 flex items-start gap-2">
                    <KeyRound className="mt-0.5 h-4 w-4 shrink-0 text-sky-600 dark:text-sky-400" />
                    <div>
                      <p className="text-sm font-bold text-sky-800 dark:text-sky-200">Amazon SP-API</p>
                      <p className="text-[11px] text-sky-600 dark:text-sky-400">
                        All fields are stored in <code className="rounded bg-sky-100 px-1 font-mono text-[10px] dark:bg-sky-900/50">marketplaces.credentials</code>.
                        {editingStoreId ? " Leave a secret blank to keep the saved value." : ""}
                      </p>
                    </div>
                  </div>
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    {AMAZON_CREDENTIAL_ROWS.map((row) => (
                      <div key={row.key} className="flex min-h-[5.25rem] flex-col gap-1.5">
                        <label className="text-sm font-medium leading-none">{row.label}</label>
                        <input
                          type={row.type}
                          autoComplete="off"
                          value={newStoreCredentials[row.key] ?? ""}
                          onChange={(e) =>
                            setNewStoreCredentials((prev) => ({ ...prev, [row.key]: e.target.value }))
                          }
                          placeholder={row.placeholder}
                          className={`${INPUT_CLS} h-10 min-h-[2.5rem] font-mono text-sm`}
                        />
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* ── Other platforms: API Credentials ─────────────────────── */}
              {newStorePlatform !== "amazon" && (PLATFORM_CRED_FIELDS[newStorePlatform] ?? []).length > 0 && (
                <div className="rounded-xl border border-sky-200 bg-sky-50/50 p-4 space-y-4 dark:border-sky-700/50 dark:bg-sky-950/20">
                  <div className="flex items-center gap-2">
                    <KeyRound className="h-4 w-4 text-sky-600 dark:text-sky-400 shrink-0" />
                    <div>
                      <p className="text-sm font-bold text-sky-800 dark:text-sky-200">API Credentials</p>
                      <p className="text-[11px] text-sky-600 dark:text-sky-400">
                        {editingStoreId
                          ? "Leave fields blank to keep existing credentials."
                          : "Optional — add now or edit later. Stored server-side via Supabase."}
                      </p>
                    </div>
                  </div>

                  {(PLATFORM_CRED_FIELDS[newStorePlatform] ?? []).map((f) => (
                    <div key={f.key} className="min-h-[5.25rem] space-y-1.5">
                      <label className={LABEL_CLS}>{f.label}</label>
                      <input
                        type={f.type}
                        autoComplete="off"
                        value={newStoreCredentials[f.key] ?? ""}
                        onChange={(e) =>
                          setNewStoreCredentials((prev) => ({ ...prev, [f.key]: e.target.value }))
                        }
                        placeholder={f.placeholder}
                        className={`${INPUT_CLS} h-10 font-mono`}
                      />
                    </div>
                  ))}
                </div>
              )}

              {/* Test connection */}
              {PLATFORM_TO_PROVIDER[newStorePlatform] && (
                <div className="flex flex-wrap items-center gap-2 border-t border-border pt-4">
                  <button
                    type="button"
                    onClick={() => void handleTestModalConnection()}
                    disabled={storeModalTestLoading || storeModalCredLoading}
                    className="inline-flex h-10 min-w-[160px] items-center justify-center gap-2 rounded-lg border border-sky-200 bg-sky-50 px-4 text-sm font-semibold text-sky-800 transition hover:bg-sky-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-200 dark:hover:bg-sky-950/60"
                  >
                    {storeModalTestLoading ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Wifi className="h-4 w-4" />
                    )}
                    {storeModalTestLoading ? "Testing…" : "Test Connection"}
                  </button>
                  <p className="text-[11px] text-muted-foreground">
                    Uses the credentials above without saving.
                  </p>
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-3 pt-1">
                <button
                  type="button"
                  onClick={closeAddStoreModal}
                  className="flex h-11 flex-1 items-center justify-center rounded-xl border border-border bg-muted/50 text-sm font-semibold text-muted-foreground transition hover:bg-accent"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={addStoreSaving || !newStoreName.trim() || storeModalCredLoading}
                  className="flex h-11 flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 text-sm font-semibold text-white shadow-sm transition hover:bg-sky-700 disabled:opacity-50"
                >
                  {addStoreSaving ? (
                    <><Loader2 className="h-4 w-4 animate-spin" />Saving…</>
                  ) : editingStoreId ? (
                    <><Save className="h-4 w-4" />Save Changes</>
                  ) : (
                    <><Plus className="h-4 w-4" />Create Store</>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

    </div>
  );
}
