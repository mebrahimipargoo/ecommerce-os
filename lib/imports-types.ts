/** Organizations shown in admin organization pickers (`listCompaniesForImports`). */
export type CompanyOption = { id: string; display_name: string };

/** Stores for Imports / Amazon Ledger target (`listStoresForImports`). */
export type StoreImportOption = {
  id: string;
  organization_id: string;
  /** Human-readable label for the dropdown. */
  display_name: string;
};
