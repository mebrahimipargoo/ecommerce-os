/** Organizations shown in admin organization pickers (`listCompaniesForImports`). */
export type CompanyOption = {
  id: string;
  display_name: string;
  /** From `organizations.type` when the query embeds it (e.g. platform user directory). */
  organization_type?: "tenant" | "internal";
};

/** Stores for Imports / Amazon Ledger target (`listStoresForImports`). */
export type StoreImportOption = {
  id: string;
  organization_id: string;
  /** Human-readable label for the dropdown. */
  display_name: string;
};
