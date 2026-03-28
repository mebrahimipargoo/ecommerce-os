import React from "react";
import {
  Document,
  Page,
  Text,
  View,
  Image,
  StyleSheet,
  Link,
} from "@react-pdf/renderer";
import { marketplaceSearchUrl } from "../../lib/marketplace-search-url";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimDetailPayload } from "./claim-actions";

/** Bulk / batch PDF: strict 3×3 grid per item (one detail page per item). */
export const BULK_ITEM_GRID_MAX = 9;
/** Single-claim PDF: same 3×3 cap per evidence page. */
export const EVIDENCE_IMAGES_PER_PAGE = BULK_ITEM_GRID_MAX;
export const BULK_GRID_SIZE = 3;

/** A4 points (72 dpi) — layout math for item-detail pages (must not overflow). */
const A4_PT = { w: 595, h: 842 };
const BULK_ITEM_PAD = 14;
const BULK_PAGE_FOOTER_PT = 18;

/**
 * Dynamic vertical budget for the 3×3 evidence grid (item detail + single-claim evidence p.1).
 *
 * Available_Height = PageInnerHeight − FooterBand − Fuzz − (Header + ItemLine + Metadata + Notes + PhotoTitle + GridTopMargin)
 * Max_Row_Height = (Available_Height − 2×rowGap) / 3
 *
 * Image containers MUST use at most (Max_Row_Height − label − cell chrome); never force a minimum image height
 * (that was breaking "one item per page" by pushing row 3 off the page).
 */
const PAGE_INNER_H = A4_PT.h - 2 * BULK_ITEM_PAD;
const FOOTER_CONTENT_CLEARANCE_PT = 20;
const LAYOUT_FUZZ_PT = 6;

const RES_GLOBAL_HEADER_PT = 52;
const RES_ITEM_CONTEXT_LINE_PT = 18;
/** Conservative ceiling for CompactMetadataGrid (3 columns; links / long lines). */
const RES_METADATA_MAX_PT = 140;
const RES_META_MARGIN_BOTTOM_PT = 8;
const RES_NOTES_BLOCK_PT = 38;
const RES_PHOTO_SECTION_TITLE_PT = 18;
const RES_GRID_WRAPPER_TOP_PT = 2;

const RESERVED_ABOVE_EVIDENCE_GRID_PT =
  RES_GLOBAL_HEADER_PT +
  RES_ITEM_CONTEXT_LINE_PT +
  RES_METADATA_MAX_PT +
  RES_META_MARGIN_BOTTOM_PT +
  RES_NOTES_BLOCK_PT +
  RES_PHOTO_SECTION_TITLE_PT +
  RES_GRID_WRAPPER_TOP_PT;

/** Total vertical space allocated to the 3×3 block (three rows + two inter-row gaps). */
const AVAILABLE_EVIDENCE_GRID_PT = Math.max(
  120,
  PAGE_INNER_H -
    FOOTER_CONTENT_CLEARANCE_PT -
    LAYOUT_FUZZ_PT -
    RESERVED_ABOVE_EVIDENCE_GRID_PT,
);

const GRID_INTER_ROW_GAP_PT = 2;
const BULK_GRID_BLOCK_H =
  AVAILABLE_EVIDENCE_GRID_PT - 2 * GRID_INTER_ROW_GAP_PT;
const BULK_GRID_ROW_H = BULK_GRID_BLOCK_H / BULK_GRID_SIZE;

const BULK_CELL_LABEL_H = 10;
/** Border (2) + padding (4) + label (10) + label margin (1) — must fit inside BULK_GRID_ROW_H. */
const GRID_CELL_CHROME_PT = 17;
const BULK_CELL_IMG_H = Math.max(
  1,
  Math.floor(BULK_GRID_ROW_H - BULK_CELL_LABEL_H - GRID_CELL_CHROME_PT),
);
const BULK_GRID_GAP = GRID_INTER_ROW_GAP_PT;
const BULK_CELL_W =
  (A4_PT.w - 2 * BULK_ITEM_PAD - 2 * GRID_INTER_ROW_GAP_PT) / BULK_GRID_SIZE;
/** Inner image width inside bordered cell (horizontal padding + border). */
const BULK_GRID_IMG_INNER_W = Math.max(1, BULK_CELL_W - 8);

const styles = StyleSheet.create({
  page: { padding: 32, fontSize: 9, fontFamily: "Helvetica", color: "#0f172a" },
  coverPage: { padding: 48, fontSize: 11, fontFamily: "Helvetica" },
  coverTitle: { fontSize: 11, fontWeight: "bold", marginBottom: 20, color: "#0f172a" },
  coverBody: { fontSize: 10, lineHeight: 1.55, color: "#1e293b", marginBottom: 10 },
  coverSignoff: { marginTop: 28, fontSize: 10, color: "#0f172a" },
  /** Global header — tenant left, marketplace + package title right (every page). */
  globalHeaderRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    borderBottomWidth: 1,
    borderBottomColor: "#e2e8f0",
    paddingBottom: 6,
    marginBottom: 8,
  },
  globalHeaderLeft: { flexDirection: "row", alignItems: "center", maxWidth: "52%" },
  globalHeaderRight: { alignItems: "flex-end", maxWidth: "46%" },
  globalHeaderCompany: { fontSize: 11, fontWeight: "bold", color: "#0f172a" },
  globalHeaderDetail: { fontSize: 7, color: "#64748b", marginTop: 2 },
  globalHeaderPackageTitle: {
    fontSize: 9,
    fontWeight: "bold",
    color: "#0f172a",
    marginTop: 4,
    textAlign: "right" as const,
  },
  globalLogo: { width: 56, height: 30, objectFit: "contain", marginRight: 8 },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    borderBottomWidth: 1,
    borderBottomColor: "#e2e8f0",
    paddingBottom: 8,
    marginBottom: 10,
  },
  headerLeft: { flexDirection: "row", alignItems: "center", maxWidth: "58%" },
  headerRight: { alignItems: "flex-end", maxWidth: "40%" },
  logo: { width: 56, height: 28, objectFit: "contain", marginRight: 8 },
  title: { fontSize: 12, fontWeight: "bold" },
  subtitle: { fontSize: 8, color: "#64748b", marginTop: 1 },
  mpTitle: { fontSize: 7, color: "#64748b", textTransform: "uppercase" },
  /** 3-column compact metadata */
  metaWrap: {
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 4,
    marginBottom: 8,
    overflow: "hidden",
  },
  metaRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "#e2e8f0",
  },
  metaRowLast: { flexDirection: "row" },
  metaCol: {
    flex: 1,
    paddingVertical: 5,
    paddingHorizontal: 8,
    borderRightWidth: 1,
    borderRightColor: "#e2e8f0",
    minWidth: 0,
  },
  metaColLast: {
    flex: 1,
    paddingVertical: 5,
    paddingHorizontal: 8,
    minWidth: 0,
  },
  metaColTitle: {
    fontSize: 7,
    fontWeight: "bold",
    color: "#475569",
    textTransform: "uppercase",
    letterSpacing: 0.3,
    marginBottom: 4,
  },
  metaLine: { fontSize: 8, lineHeight: 1.35, marginBottom: 2 },
  metaMono: { fontFamily: "Courier", fontSize: 7 },
  metaItemTitle: { fontSize: 9, fontWeight: "bold", marginBottom: 4, color: "#0f172a" },
  sectionTitle: {
    fontSize: 9,
    fontWeight: "bold",
    color: "#0f172a",
    marginBottom: 6,
    marginTop: 4,
  },
  continuedBadge: {
    fontSize: 8,
    color: "#64748b",
    marginBottom: 8,
    fontStyle: "italic",
  },
  reportBanner: {
    backgroundColor: "#f1f5f9",
    borderWidth: 1,
    borderColor: "#e2e8f0",
    padding: 6,
    marginBottom: 10,
    borderRadius: 4,
  },
  reportBannerText: { fontSize: 9, fontWeight: "bold", color: "#0f172a" },
  linkLine: { fontSize: 6, color: "#2563eb", marginTop: 1 },
  footer: {
    position: "absolute",
    bottom: 20,
    left: 32,
    right: 32,
    fontSize: 7,
    color: "#94a3b8",
    textAlign: "center",
  },
  emptyNote: { fontSize: 8, color: "#64748b", fontStyle: "italic" },
  /** Bulk master summary (page 1) */
  masterPage: { padding: 40, fontSize: 10, fontFamily: "Helvetica", color: "#0f172a" },
  masterTitle: { fontSize: 13, fontWeight: "bold", marginBottom: 8, color: "#0f172a" },
  masterIntro: {
    fontSize: 10,
    lineHeight: 1.5,
    color: "#334155",
    marginBottom: 14,
    textAlign: "justify",
  },
  masterTableWrap: {
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 4,
    marginTop: 4,
  },
  masterTableHeader: {
    flexDirection: "row",
    backgroundColor: "#f1f5f9",
    borderBottomWidth: 1,
    borderBottomColor: "#94a3b8",
    paddingVertical: 5,
    paddingHorizontal: 4,
  },
  masterTableRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "#e2e8f0",
    paddingVertical: 5,
    paddingHorizontal: 4,
    minHeight: 22,
  },
  masterTableRowLast: {
    flexDirection: "row",
    paddingVertical: 5,
    paddingHorizontal: 4,
    minHeight: 22,
  },
  masterTh: { fontSize: 7, fontWeight: "bold", color: "#475569", textTransform: "uppercase" },
  masterTd: { fontSize: 7, color: "#0f172a" },
  masterTdMono: { fontSize: 6, fontFamily: "Courier", color: "#0f172a" },
  masterColItem: { width: "26%", paddingRight: 4 },
  masterColOrder: { width: "16%", paddingRight: 4 },
  masterColDefect: { width: "20%", paddingRight: 4 },
  masterColStatus: { width: "14%", paddingRight: 4 },
  masterColPhoto: { width: "14%", paddingRight: 2, alignItems: "center" as const },
  masterColDetail: { width: "10%", textAlign: "right" as const },
  masterThumb: {
    width: 36,
    height: 36,
    objectFit: "cover" as const,
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 2,
    backgroundColor: "#f1f5f9",
  },
  masterThumbPlaceholder: {
    width: 36,
    height: 36,
    borderWidth: 1,
    borderColor: "#cbd5e1",
    backgroundColor: "#f8fafc",
  },
  masterLink: { fontSize: 7, color: "#2563eb", textDecoration: "underline" },
  notesSection: {
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 4,
    paddingVertical: 5,
    paddingHorizontal: 6,
    marginBottom: 5,
    minHeight: 28,
    backgroundColor: "#fafafa",
  },
  notesLabel: {
    fontSize: 7,
    fontWeight: "bold",
    color: "#475569",
    textTransform: "uppercase" as const,
    marginBottom: 4,
  },
  notesPlaceholder: { fontSize: 7, color: "#94a3b8", fontStyle: "italic" },
  /** Bulk item detail page (compact) */
  itemCompactPage: {
    padding: BULK_ITEM_PAD,
    fontSize: 6.5,
    fontFamily: "Helvetica",
    color: "#0f172a",
  },
  grid3Row: {
    flexDirection: "row",
    justifyContent: "space-between",
    height: BULK_GRID_ROW_H,
  },
  grid3Cell: {
    width: BULK_CELL_W,
    alignItems: "center" as const,
  },
  grid3Label: {
    fontSize: 6,
    fontWeight: "bold",
    color: "#475569",
    textAlign: "center" as const,
    marginBottom: 1,
    height: BULK_CELL_LABEL_H,
  },
  grid3Img: {
    width: BULK_GRID_IMG_INNER_W,
    height: BULK_CELL_IMG_H,
    objectFit: "contain" as const,
    borderWidth: 1,
    borderColor: "#94a3b8",
    backgroundColor: "#ffffff",
  },
  grid3CellBox: {
    width: BULK_CELL_W,
    height: BULK_GRID_ROW_H,
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 2,
    padding: 2,
    backgroundColor: "#fafafa",
  },
  bulkOverflowNote: { fontSize: 6, color: "#b45309", marginTop: 4 },
  itemPageFooter: {
    position: "absolute",
    bottom: BULK_PAGE_FOOTER_PT / 2,
    left: BULK_ITEM_PAD,
    right: BULK_ITEM_PAD,
    fontSize: 6,
    color: "#94a3b8",
    textAlign: "center" as const,
  },
});

function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function MarketplaceBrand({ storeName, storePlatform }: { storeName: string; storePlatform: string }) {
  const p = storePlatform.toLowerCase();
  if (p.includes("walmart")) {
    return (
      <View style={{ alignItems: "flex-end" }}>
        <Text style={styles.mpTitle}>Marketplace</Text>
        <Text style={{ fontSize: 16, fontWeight: "bold", color: "#0071CE", marginTop: 1 }}>Walmart</Text>
        <Text style={styles.subtitle}>{storeName}</Text>
      </View>
    );
  }
  if (p.includes("ebay")) {
    return (
      <View style={{ alignItems: "flex-end" }}>
        <Text style={styles.mpTitle}>Marketplace</Text>
        <Text style={{ fontSize: 15, fontWeight: "bold", color: "#0064D2", marginTop: 1 }}>eBay</Text>
        <Text style={styles.subtitle}>{storeName}</Text>
      </View>
    );
  }
  return (
    <View style={{ alignItems: "flex-end" }}>
      <Text style={styles.mpTitle}>Marketplace</Text>
      <Text style={{ fontSize: 16, fontWeight: "bold", color: "#FF9900", marginTop: 1 }}>amazon</Text>
      <Text style={styles.subtitle}>{storeName}</Text>
    </View>
  );
}

/**
 * Global header on every evidence page: tenant logo + company (left), marketplace + "Claim evidence package" (right).
 */
function GlobalPdfHeader({
  tenant,
  storeName,
  storePlatform,
}: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
}) {
  const company =
    tenant.company_name?.trim() ||
    (typeof tenant.workspace_name === "string" ? tenant.workspace_name.trim() : "") ||
    "Workspace";
  return (
    <View style={styles.globalHeaderRow} wrap={false}>
      <View style={styles.globalHeaderLeft}>
        {(tenant.company_logo_url || tenant.logo_url) ? (
          <Image
            src={String(tenant.company_logo_url || tenant.logo_url || "")}
            style={styles.globalLogo}
          />
        ) : null}
        <View>
          <Text style={styles.globalHeaderCompany}>{company}</Text>
          <Text style={styles.globalHeaderDetail}>FBA claim submission · Seller reimbursement evidence</Text>
        </View>
      </View>
      <View style={styles.globalHeaderRight}>
        <MarketplaceBrand storeName={storeName} storePlatform={storePlatform} />
        <Text style={styles.globalHeaderPackageTitle}>Claim evidence package</Text>
      </View>
    </View>
  );
}

function CoverLetterPage({
  tenant,
  storeName,
  storePlatform,
  orderId,
  defectSummary,
}: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  orderId: string;
  defectSummary: string;
}) {
  const company =
    tenant.company_name?.trim() ||
    (typeof tenant.workspace_name === "string" ? tenant.workspace_name.trim() : "") ||
    "Our company";
  const p = storePlatform.toLowerCase();
  const greeting =
    p.includes("walmart")
      ? "To Walmart Seller Support"
      : p.includes("ebay")
        ? "To eBay Seller Support"
        : "To Amazon Seller Support";

  return (
    <Page size="A4" style={styles.coverPage}>
      <GlobalPdfHeader tenant={tenant} storeName={storeName} storePlatform={storePlatform} />
      <Text style={[styles.coverTitle, { marginTop: 12 }]}>{greeting},</Text>
      <Text style={styles.coverBody}>
        Please find attached evidence for Order ID{" "}
        <Text style={{ fontFamily: "Courier", fontWeight: "bold" }}>{orderId || "—"}</Text>
        {" "}regarding the following defects: {defectSummary || "as described in the attached documentation"}.
      </Text>
      <Text style={styles.coverBody}>
        The following pages include labeled photographic documentation embedded in this PDF (packaging, labels, manifests, and product condition).
      </Text>
      <Text style={styles.coverSignoff}>
        Respectfully,{"\n"}
        {company}
      </Text>
      <Text style={styles.footer} fixed>
        Generated by E-commerce OS · Confidential claim evidence
      </Text>
    </Page>
  );
}

function introRecipientLabel(storePlatform: string): string {
  const p = storePlatform.toLowerCase();
  if (p.includes("walmart")) return "Walmart Seller Support";
  if (p.includes("ebay")) return "eBay Seller Support";
  return "Amazon Seller Support";
}

function CompactMetadataGrid({
  detail,
  claimAmountNote,
  marketplaceClaimIdNote,
  storePlatform,
}: {
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  storePlatform: string;
}) {
  const { claim, returnRow, pallet, packageRow: pkg } = detail;
  const itemName = (returnRow?.item_name ?? claim.item_name ?? "—").trim() || "—";
  const asin = (returnRow?.asin ?? claim.asin ?? "—").trim() || "—";
  const fnsku = (returnRow?.fnsku ?? claim.fnsku ?? "—").trim() || "—";
  const sku = (returnRow?.sku ?? claim.sku ?? "—").trim() || "—";

  const asinUrl = asin && asin !== "—" ? marketplaceSearchUrl(storePlatform, asin) : null;
  const fnskuUrl = fnsku && fnsku !== "—" ? marketplaceSearchUrl(storePlatform, fnsku) : null;
  const skuUrl = sku && sku !== "—" ? marketplaceSearchUrl(storePlatform, sku) : null;

  return (
    <View style={styles.metaWrap} wrap={false}>
      <View style={[styles.metaRow, { backgroundColor: "#f8fafc" }]}>
        <View style={styles.metaCol}>
          <Text style={styles.metaColTitle}>Identifiers</Text>
          <Text style={styles.metaItemTitle} wrap>
            {itemName}
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>ASIN </Text>
            <Text style={styles.metaMono}>{asin}</Text>
            {asinUrl ? (
              <Link src={asinUrl} style={styles.linkLine}>
                <Text> · search</Text>
              </Link>
            ) : null}
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>FNSKU </Text>
            <Text style={styles.metaMono}>{fnsku}</Text>
            {fnskuUrl ? (
              <Link src={fnskuUrl} style={styles.linkLine}>
                <Text> · search</Text>
              </Link>
            ) : null}
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>SKU </Text>
            <Text style={styles.metaMono}>{sku}</Text>
            {skuUrl ? (
              <Link src={skuUrl} style={styles.linkLine}>
                <Text> · search</Text>
              </Link>
            ) : null}
          </Text>
        </View>
        <View style={styles.metaCol}>
          <Text style={styles.metaColTitle}>Logistics</Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Pallet # </Text>
            <Text style={styles.metaMono}>{pallet?.pallet_number ?? "—"}</Text>
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Package # </Text>
            <Text style={styles.metaMono}>{pkg?.package_number ?? "—"}</Text>
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Tracking </Text>
            <Text style={styles.metaMono}>{pkg?.tracking_number ?? "—"}</Text>
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Carrier </Text>
            <Text>{pkg?.carrier_name ?? "—"}</Text>
          </Text>
        </View>
        <View style={styles.metaColLast}>
          <Text style={styles.metaColTitle}>Claim</Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Type </Text>
            <Text>{claim.claim_type ?? "—"}</Text>
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Order ID </Text>
            <Text style={styles.metaMono}>{claim.amazon_order_id ?? "—"}</Text>
          </Text>
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Amount </Text>
            <Text>{String(claim.amount ?? "—")}</Text>
            {claimAmountNote ? (
              <Text>
                {" "}
                (filed: {claimAmountNote})
              </Text>
            ) : null}
          </Text>
          {marketplaceClaimIdNote ? (
            <Text style={styles.metaLine}>
              <Text style={{ color: "#64748b" }}>Case ID </Text>
              <Text style={styles.metaMono}>{marketplaceClaimIdNote}</Text>
            </Text>
          ) : null}
          <Text style={styles.metaLine}>
            <Text style={{ color: "#64748b" }}>Link status </Text>
            <Text>{claim.marketplace_link_status ?? "—"}</Text>
          </Text>
        </View>
      </View>
    </View>
  );
}

function defectSummaryFromDetail(detail: ClaimDetailPayload): string {
  const ct = detail.claim.claim_type?.trim();
  const cond = detail.returnRow?.conditions?.filter(Boolean) ?? [];
  const parts = [ct, ...cond.map((c) => String(c))].filter(Boolean);
  if (parts.length === 0) return "inventory and fulfillment discrepancies";
  return parts.join(", ");
}

/** Single-line issue summary for compact bulk item headers. */
function oneLineIssueSummary(detail: ClaimDetailPayload): string {
  return defectSummaryFromDetail(detail);
}

function defectTypeForTable(detail: ClaimDetailPayload): string {
  const ct = detail.claim.claim_type?.trim();
  if (ct) return ct.length > 36 ? `${ct.slice(0, 34)}…` : ct;
  const cond = detail.returnRow?.conditions?.filter(Boolean) ?? [];
  if (cond.length > 0) {
    const s = cond.map((c) => String(c)).join(", ");
    return s.length > 36 ? `${s.slice(0, 34)}…` : s;
  }
  return "—";
}

function claimStatusLabel(detail: ClaimDetailPayload): string {
  const s = (detail.claim.status ?? "").trim() || "—";
  if (s === "—") return s;
  return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, " ");
}

function masterIntroOrderPhrase(pages: BulkClaimPdfPageInput[]): { line: string; multi: boolean } {
  const ids = [...new Set(pages.map((p) => (p.detail.claim.amazon_order_id ?? "").trim()).filter(Boolean))];
  if (ids.length === 0) return { line: "—", multi: false };
  if (ids.length === 1) return { line: ids[0]!, multi: false };
  return { line: ids.join(", "), multi: true };
}

function NotesCommentsBlock() {
  return (
    <View style={styles.notesSection} wrap={false}>
      <Text style={styles.notesLabel}>Notes / comments</Text>
      <Text style={styles.notesPlaceholder}>
        Add operator notes, carrier context, or Seller Central case references here (optional).
      </Text>
    </View>
  );
}

/**
 * 3×3 evidence grid — bounding boxes sized for A4; images use objectFit contain (sharp when zoomed).
 * Pass at most BULK_ITEM_GRID_MAX items; extras should be truncated upstream with a note.
 */
function EvidenceGrid3x3({ items }: { items: { label: string; dataUri: string }[] }) {
  const slots = items.slice(0, BULK_ITEM_GRID_MAX);
  while (slots.length < BULK_ITEM_GRID_MAX) {
    slots.push({ label: "", dataUri: "" });
  }
  const rows: { label: string; dataUri: string }[][] = [];
  for (let r = 0; r < BULK_GRID_SIZE; r++) {
    rows.push(slots.slice(r * BULK_GRID_SIZE, r * BULK_GRID_SIZE + BULK_GRID_SIZE));
  }
  return (
    <View style={{ marginTop: RES_GRID_WRAPPER_TOP_PT }}>
      {rows.map((row, ri) => (
        <View
          key={ri}
          style={[
            styles.grid3Row,
            ri < BULK_GRID_SIZE - 1 ? { marginBottom: GRID_INTER_ROW_GAP_PT } : {},
          ]}
          wrap={false}
        >
          {row.map((cell, ci) => (
            <View key={ci} style={styles.grid3Cell} wrap={false}>
              {cell.dataUri ? (
                <View style={styles.grid3CellBox}>
                  <Text style={styles.grid3Label}>{cell.label}</Text>
                  <Image src={cell.dataUri} style={styles.grid3Img} />
                </View>
              ) : (
                <View style={{ width: BULK_CELL_W, height: BULK_GRID_ROW_H }} />
              )}
            </View>
          ))}
        </View>
      ))}
    </View>
  );
}

function MasterSummaryPage({
  tenant,
  pages,
  reportKind,
  primaryStorePlatform,
  primaryStoreName,
}: {
  tenant: CoreSettings;
  pages: BulkClaimPdfPageInput[];
  reportKind?: "master" | "batch";
  primaryStorePlatform: string;
  primaryStoreName: string;
}) {
  const n = pages.length;
  const company =
    tenant.company_name?.trim() ||
    (typeof tenant.workspace_name === "string" ? tenant.workspace_name.trim() : "") ||
    "Our company";
  const who = introRecipientLabel(primaryStorePlatform);
  const { line: orderPhrase, multi } = masterIntroOrderPhrase(pages);

  return (
    <Page size="A4" style={styles.masterPage}>
      <GlobalPdfHeader tenant={tenant} storeName={primaryStoreName} storePlatform={primaryStorePlatform} />

      {reportKind ? (
        <View style={{ marginBottom: 8, alignSelf: "flex-start", backgroundColor: "#eef2ff", paddingHorizontal: 8, paddingVertical: 4, borderRadius: 4 }}>
          <Text style={{ fontSize: 8, fontWeight: "bold", color: "#3730a3" }}>
            {reportKind === "master" ? "MASTER QUEUE" : "BATCH SELECTION"} · {n} line item{n === 1 ? "" : "s"}
          </Text>
        </View>
      ) : null}

      <Text style={[styles.masterIntro, { marginBottom: 10 }]}>
        To {who}, please review the evidence regarding {multi ? "orders " : "order "}
        <Text style={{ fontFamily: "Courier", fontWeight: "bold" }}>{orderPhrase}</Text>.
      </Text>

      <Text style={styles.masterTitle}>Master summary — line items</Text>

      <View style={styles.masterTableWrap}>
        <View style={styles.masterTableHeader}>
          <Text style={[styles.masterTh, styles.masterColItem]}>Item name</Text>
          <Text style={[styles.masterTh, styles.masterColOrder]}>Order ID</Text>
          <Text style={[styles.masterTh, styles.masterColDefect]}>Defect type</Text>
          <Text style={[styles.masterTh, styles.masterColStatus]}>Status</Text>
          <Text style={[styles.masterTh, styles.masterColPhoto]}>Photo</Text>
          <Text style={[styles.masterTh, styles.masterColDetail]}>Detail</Text>
        </View>
        {pages.map((p, idx) => {
          const d = p.detail;
          const itemName = (d.returnRow?.item_name ?? d.claim.item_name ?? "—").trim() || "—";
          const order = (d.claim.amazon_order_id ?? "—").trim() || "—";
          const defectType = defectTypeForTable(d);
          const status = claimStatusLabel(d);
          const detailPage1Based = idx + 2;
          const rowStyle = idx === pages.length - 1 ? styles.masterTableRowLast : styles.masterTableRow;
          const thumb = p.evidenceImages?.[0]?.dataUri ?? null;
          return (
            <View key={p.detail.claim.id} style={rowStyle}>
              <Text style={[styles.masterTd, styles.masterColItem]} wrap>
                {itemName.length > 48 ? `${itemName.slice(0, 46)}…` : itemName}
              </Text>
              <Text style={[styles.masterTdMono, styles.masterColOrder]} wrap>
                {order}
              </Text>
              <Text style={[styles.masterTd, styles.masterColDefect]} wrap>
                {defectType}
              </Text>
              <Text style={[styles.masterTd, styles.masterColStatus]} wrap>
                {status}
              </Text>
              <View style={[styles.masterColPhoto, { justifyContent: "center", alignItems: "center" }]}>
                {thumb ? (
                  <Image src={thumb} style={styles.masterThumb} />
                ) : (
                  <View style={styles.masterThumbPlaceholder} />
                )}
              </View>
              <Text style={[styles.masterTd, styles.masterColDetail]}>
                <Link src={`#claim-item-${idx + 1}`}>
                  <Text style={styles.masterLink}>p. {detailPage1Based}</Text>
                </Link>
              </Text>
            </View>
          );
        })}
      </View>

      <Text style={{ marginTop: 14, fontSize: 9, color: "#0f172a" }}>
        Respectfully,{"\n"}
        {company}
      </Text>
      <Text style={{ ...styles.footer, position: "absolute", bottom: 24, left: 40, right: 40 }}>Page 1 · Master summary · E-commerce OS</Text>
    </Page>
  );
}

function ItemBulkDetailPage({
  tenant,
  pageInput,
  itemIndex,
  totalItems,
  detailPageNumber1Based,
}: {
  tenant: CoreSettings;
  pageInput: BulkClaimPdfPageInput;
  itemIndex: number;
  totalItems: number;
  detailPageNumber1Based: number;
}) {
  const d = pageInput.detail;
  const claim = d.claim;
  const raw = pageInput.evidenceImages ?? [];
  const shown = raw.slice(0, BULK_ITEM_GRID_MAX);
  const truncated = raw.length > BULK_ITEM_GRID_MAX;
  const issue = oneLineIssueSummary(d);

  return (
    <Page id={`claim-item-${itemIndex + 1}`} size="A4" style={styles.itemCompactPage}>
      <GlobalPdfHeader
        tenant={tenant}
        storeName={pageInput.storeName}
        storePlatform={pageInput.storePlatform}
      />

      <Text style={{ fontSize: 6.5, color: "#64748b", marginBottom: 4 }} wrap>
        Item {itemIndex + 1} of {totalItems}
        {" · "}
        <Text style={{ fontFamily: "Courier", color: "#0f172a" }}>{claim.id}</Text>
        {" · Issue: "}
        <Text style={{ fontWeight: "bold", color: "#0f172a" }}>{issue}</Text>
      </Text>

      <CompactMetadataGrid
        detail={d}
        claimAmountNote={pageInput.claimAmountNote}
        marketplaceClaimIdNote={pageInput.marketplaceClaimIdNote}
        storePlatform={pageInput.storePlatform}
      />

      <NotesCommentsBlock />

      <Text style={[styles.sectionTitle, { marginTop: 2, marginBottom: 4 }]}>Photographic evidence</Text>

      {shown.length > 0 ? (
        <EvidenceGrid3x3 items={shown} />
      ) : (
        <Text style={styles.emptyNote}>No images embedded for this item.</Text>
      )}
      {truncated ? (
        <Text style={styles.bulkOverflowNote}>
          Showing first {BULK_ITEM_GRID_MAX} of {raw.length} photos — export this claim alone for the full set.
        </Text>
      ) : null}

      <Text style={styles.itemPageFooter} fixed>
        Item {itemIndex + 1} of {totalItems} · {claim.id} · Page {detailPageNumber1Based} · E-commerce OS
      </Text>
    </Page>
  );
}

export function SingleClaimPdfDocument({
  tenant,
  storeName,
  storePlatform,
  detail,
  claimAmountNote,
  marketplaceClaimIdNote,
  evidenceImages,
}: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  /** Embedded images (data URIs). Empty array = metadata only. */
  evidenceImages?: { label: string; dataUri: string }[] | null;
}) {
  const orderId = (detail.claim.amazon_order_id ?? "—").trim() || "—";
  const summary = defectSummaryFromDetail(detail);
  const items = evidenceImages ?? [];
  const chunks = chunkArray(items, EVIDENCE_IMAGES_PER_PAGE);
  const pagesNeeded = Math.max(1, chunks.length);

  return (
    <Document>
      <CoverLetterPage
        tenant={tenant}
        storeName={storeName}
        storePlatform={storePlatform}
        orderId={orderId}
        defectSummary={summary}
      />
      {Array.from({ length: pagesNeeded }).map((_, pageIdx) => {
        const chunk = chunks[pageIdx] ?? [];
        const isFirst = pageIdx === 0;
        const isEmptyEvidence = items.length === 0;

        return (
          <Page key={pageIdx} size="A4" style={styles.itemCompactPage}>
            <GlobalPdfHeader tenant={tenant} storeName={storeName} storePlatform={storePlatform} />
            {isFirst ? (
              <>
                <CompactMetadataGrid
                  detail={detail}
                  claimAmountNote={claimAmountNote}
                  marketplaceClaimIdNote={marketplaceClaimIdNote}
                  storePlatform={storePlatform}
                />
                <NotesCommentsBlock />
                <Text style={[styles.sectionTitle, { marginTop: 2, marginBottom: 4 }]}>Photographic evidence</Text>
              </>
            ) : (
              <>
                <Text style={styles.continuedBadge}>
                  Photographic evidence (continued) — page {pageIdx + 1} of {pagesNeeded}
                </Text>
                <Text style={[styles.sectionTitle, { marginTop: 2, marginBottom: 4 }]}>Photographic evidence</Text>
              </>
            )}

            {chunk.length > 0 ? (
              <EvidenceGrid3x3 items={chunk} />
            ) : isFirst && isEmptyEvidence ? (
              <Text style={styles.emptyNote}>
                No photographs were embedded for this claim. Ensure return/package/item photos exist in storage and try
                exporting again from the Claim Engine (images are fetched client-side to avoid blank placeholders).
              </Text>
            ) : null}

            <Text style={styles.footer} fixed>
              Claim ID {detail.claim.id}
              {pagesNeeded > 1 ? ` · Evidence page ${pageIdx + 1}/${pagesNeeded}` : ""}
              {" · "}Generated by E-commerce OS
            </Text>
          </Page>
        );
      })}
    </Document>
  );
}

export type BulkClaimPdfPageInput = {
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  evidenceImages?: { label: string; dataUri: string }[];
};

export function BulkClaimsPdfDocument({
  tenant,
  pages,
  reportKind,
}: {
  tenant: CoreSettings;
  pages: BulkClaimPdfPageInput[];
  reportKind?: "master" | "batch";
}) {
  if (pages.length === 0) {
    return (
      <Document>
        <Page size="A4" style={styles.masterPage}>
          <GlobalPdfHeader tenant={tenant} storeName="—" storePlatform="amazon" />
          <Text style={{ marginTop: 12 }}>No claims selected.</Text>
        </Page>
      </Document>
    );
  }

  const primaryStorePlatform = pages[0]?.storePlatform ?? "amazon";
  const primaryStoreName = pages[0]?.storeName ?? "Store";

  return (
    <Document>
      <MasterSummaryPage
        tenant={tenant}
        pages={pages}
        reportKind={reportKind}
        primaryStorePlatform={primaryStorePlatform}
        primaryStoreName={primaryStoreName}
      />
      {pages.map((p, idx) => (
        <ItemBulkDetailPage
          key={p.detail.claim.id}
          tenant={tenant}
          pageInput={p}
          itemIndex={idx}
          totalItems={pages.length}
          detailPageNumber1Based={idx + 2}
        />
      ))}
    </Document>
  );
}
