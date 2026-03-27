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

const styles = StyleSheet.create({
  page: { padding: 36, fontSize: 10, fontFamily: "Helvetica" },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    borderBottomWidth: 1,
    borderBottomColor: "#e2e8f0",
    paddingBottom: 12,
    marginBottom: 16,
  },
  headerLeft: { flexDirection: "row", alignItems: "center", maxWidth: "55%" },
  headerRight: { alignItems: "flex-end", maxWidth: "42%" },
  logo: { width: 72, height: 36, objectFit: "contain", marginRight: 8 },
  title: { fontSize: 14, fontWeight: "bold" },
  subtitle: { fontSize: 9, color: "#64748b", marginTop: 2 },
  mpTitle: { fontSize: 8, color: "#64748b", textTransform: "uppercase" },
  section: { marginBottom: 12 },
  sectionTitle: {
    fontSize: 11,
    fontWeight: "bold",
    marginBottom: 6,
    color: "#0f172a",
  },
  row: { flexDirection: "row", marginBottom: 4 },
  label: { width: 110, color: "#64748b" },
  value: { flex: 1, color: "#0f172a" },
  mono: { fontFamily: "Courier" },
  photoNote: { fontSize: 8, color: "#64748b", marginTop: 4 },
  idTable: {
    borderWidth: 1,
    borderColor: "#e2e8f0",
    borderRadius: 4,
    marginTop: 6,
  },
  idRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "#f1f5f9",
    paddingVertical: 6,
    paddingHorizontal: 8,
  },
  idRowLast: {
    flexDirection: "row",
    paddingVertical: 6,
    paddingHorizontal: 8,
  },
  idColLabel: { width: 72, paddingTop: 2 },
  idColValue: { flex: 1 },
  idLabelBold: { fontSize: 9, fontWeight: "bold", color: "#475569" },
  linkLine: { fontSize: 7, color: "#2563eb", marginTop: 2 },
  reportBanner: {
    backgroundColor: "#f8fafc",
    borderWidth: 1,
    borderColor: "#e2e8f0",
    padding: 8,
    marginBottom: 12,
    borderRadius: 4,
  },
  reportBannerText: { fontSize: 10, fontWeight: "bold", color: "#0f172a" },
  footer: {
    position: "absolute",
    bottom: 24,
    left: 36,
    right: 36,
    fontSize: 8,
    color: "#94a3b8",
    textAlign: "center",
  },
});

function MarketplaceBrand({ storeName, storePlatform }: { storeName: string; storePlatform: string }) {
  const p = storePlatform.toLowerCase();
  if (p.includes("walmart")) {
    return (
      <View style={{ alignItems: "flex-end" }}>
        <Text style={styles.mpTitle}>Marketplace</Text>
        <Text style={{ fontSize: 20, fontWeight: "bold", color: "#0071CE", marginTop: 2 }}>Walmart</Text>
        <Text style={styles.subtitle}>{storeName}</Text>
      </View>
    );
  }
  if (p.includes("ebay")) {
    return (
      <View style={{ alignItems: "flex-end" }}>
        <Text style={styles.mpTitle}>Marketplace</Text>
        <Text style={{ fontSize: 18, fontWeight: "bold", color: "#0064D2", marginTop: 2 }}>eBay</Text>
        <Text style={styles.subtitle}>{storeName}</Text>
      </View>
    );
  }
  return (
    <View style={{ alignItems: "flex-end" }}>
      <Text style={styles.mpTitle}>Marketplace</Text>
      <Text style={{ fontSize: 20, fontWeight: "bold", color: "#FF9900", marginTop: 2 }}>amazon</Text>
      <Text style={styles.subtitle}>{storeName}</Text>
      <Text style={[styles.subtitle, { fontSize: 8 }]}>{storePlatform}</Text>
    </View>
  );
}

function Header({
  tenant,
  storeName,
  storePlatform,
}: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
}) {
  return (
    <View style={styles.headerRow} fixed>
      <View style={styles.headerLeft}>
        {(tenant.company_logo_url || tenant.logo_url) ? (
          <Image
            src={String(tenant.company_logo_url || tenant.logo_url || "")}
            style={styles.logo}
          />
        ) : null}
        <View>
          <Text style={styles.title}>
            {tenant.company_name?.trim() || (typeof tenant.workspace_name === "string" ? tenant.workspace_name : "") || "Workspace"}
          </Text>
          <Text style={styles.subtitle}>Claim evidence report</Text>
        </View>
      </View>
      <View style={styles.headerRight}>
        <MarketplaceBrand storeName={storeName} storePlatform={storePlatform} />
      </View>
    </View>
  );
}

function IdentifierPdfRow({
  label,
  value,
  storePlatform,
  isLast,
}: {
  label: string;
  value: string;
  storePlatform: string;
  isLast?: boolean;
}) {
  const v = (value ?? "").trim();
  const searchUrl = v ? marketplaceSearchUrl(storePlatform, v) : null;
  const rowStyle = isLast ? styles.idRowLast : styles.idRow;
  return (
    <View style={rowStyle} wrap={false}>
      <View style={styles.idColLabel}>
        <Text style={styles.idLabelBold}>{label}</Text>
      </View>
      <View style={styles.idColValue}>
        <Text style={[styles.mono, { fontSize: 9, color: "#0f172a" }]}>{v || "—"}</Text>
        {searchUrl && v ? (
          <Link src={searchUrl} style={styles.linkLine}>
            <Text>Marketplace search →</Text>
          </Link>
        ) : null}
      </View>
    </View>
  );
}

function ClaimBody({
  detail,
  claimAmountNote,
  marketplaceClaimIdNote,
  storePlatform,
}: {
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  /** Used for printable marketplace search links (Golden Rule: paired with each identifier). */
  storePlatform: string;
}) {
  const { claim, returnRow, pallet, packageRow: pkg } = detail;
  const itemName =
    returnRow?.item_name ?? claim.item_name ?? "—";
  const asin = returnRow?.asin ?? claim.asin ?? "—";
  const fnsku = returnRow?.fnsku ?? claim.fnsku ?? "—";
  const sku = returnRow?.sku ?? claim.sku ?? "—";

  const photos: string[] = [];
  if (returnRow?.photo_evidence) {
    Object.entries(returnRow.photo_evidence).forEach(([k, n]) => {
      if (n && n > 0) photos.push(`Evidence bucket "${k}": ${n} photo(s)`);
    });
  }

  const itemPhoto = returnRow?.photo_item_url;
  const expiryPhoto = returnRow?.photo_expiry_url;
  const canShowImg = (u: string | null | undefined) =>
    typeof u === "string" && (u.startsWith("http://") || u.startsWith("https://"));

  return (
    <>
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Identifiers</Text>
        <Text style={{ marginBottom: 6, fontWeight: "bold", fontSize: 11 }}>{itemName}</Text>
        <View style={styles.idTable}>
          <IdentifierPdfRow label="ASIN" value={asin} storePlatform={storePlatform} />
          <IdentifierPdfRow label="FNSKU" value={fnsku} storePlatform={storePlatform} />
          <IdentifierPdfRow label="SKU" value={sku} storePlatform={storePlatform} isLast />
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Logistics</Text>
        <View style={styles.row}>
          <Text style={styles.label}>Pallet #</Text>
          <Text style={styles.value}>{pallet?.pallet_number ?? "—"}</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Package #</Text>
          <Text style={styles.value}>{pkg?.package_number ?? "—"}</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Tracking</Text>
          <Text style={[styles.value, styles.mono]}>
            {pkg?.tracking_number ?? "—"}
          </Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Carrier</Text>
          <Text style={styles.value}>{pkg?.carrier_name ?? "—"}</Text>
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Synced claim</Text>
        <View style={styles.row}>
          <Text style={styles.label}>Claim type</Text>
          <Text style={styles.value}>{claim.claim_type ?? "—"}</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Order / Ref</Text>
          <Text style={[styles.value, styles.mono]}>
            {claim.amazon_order_id ?? "—"}
          </Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Amount (synced)</Text>
          <Text style={styles.value}>{String(claim.amount ?? "")}</Text>
        </View>
        {claimAmountNote ? (
          <View style={styles.row}>
            <Text style={styles.label}>Claim amount (filed)</Text>
            <Text style={styles.value}>{claimAmountNote}</Text>
          </View>
        ) : null}
        {marketplaceClaimIdNote ? (
          <View style={styles.row}>
            <Text style={styles.label}>Marketplace claim ID</Text>
            <Text style={[styles.value, styles.mono]}>{marketplaceClaimIdNote}</Text>
          </View>
        ) : null}
        <View style={styles.row}>
          <Text style={styles.label}>Marketplace link</Text>
          <Text style={styles.value}>
            {claim.marketplace_link_status ?? "—"}
          </Text>
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Photo evidence</Text>
        <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
          {canShowImg(itemPhoto) ? (
            <View>
              <Text style={{ fontSize: 8, color: "#64748b", marginBottom: 4 }}>Item</Text>
              <Image src={itemPhoto!} style={{ width: 140, height: 140, objectFit: "cover", borderRadius: 4 }} />
            </View>
          ) : null}
          {canShowImg(expiryPhoto) ? (
            <View>
              <Text style={{ fontSize: 8, color: "#64748b", marginBottom: 4 }}>Expiry</Text>
              <Image src={expiryPhoto!} style={{ width: 140, height: 140, objectFit: "cover", borderRadius: 4 }} />
            </View>
          ) : null}
        </View>
        {!canShowImg(itemPhoto) && !canShowImg(expiryPhoto) && photos.length === 0 ? (
          <Text style={styles.value}>No photo URLs on file for this return.</Text>
        ) : (
          photos.map((p, i) => (
            <Text key={i} style={styles.photoNote}>
              {p}
            </Text>
          ))
        )}
      </View>
    </>
  );
}

export function SingleClaimPdfDocument({
  tenant,
  storeName,
  storePlatform,
  detail,
  claimAmountNote,
  marketplaceClaimIdNote,
}: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
}) {
  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <Header tenant={tenant} storeName={storeName} storePlatform={storePlatform} />
        <ClaimBody
          detail={detail}
          claimAmountNote={claimAmountNote}
          marketplaceClaimIdNote={marketplaceClaimIdNote}
          storePlatform={storePlatform}
        />
        <Text style={styles.footer} fixed>
          Generated by E-commerce OS · Claim ID {detail.claim.id}
        </Text>
      </Page>
    </Document>
  );
}

export function BulkClaimsPdfDocument({
  tenant,
  pages,
  reportKind,
}: {
  tenant: CoreSettings;
  pages: {
    storeName: string;
    storePlatform: string;
    detail: ClaimDetailPayload;
    claimAmountNote?: string;
    marketplaceClaimIdNote?: string;
  }[];
  /** Shown on the first page — Master = all ready_to_send; Batch = selected rows. */
  reportKind?: "master" | "batch";
}) {
  return (
    <Document>
      {pages.map((p, idx) => (
        <Page key={idx} size="A4" style={styles.page}>
          {idx === 0 && reportKind ? (
            <View style={styles.reportBanner}>
              <Text style={styles.reportBannerText}>
                {reportKind === "master"
                  ? "MASTER REPORT — ALL READY-TO-SEND CLAIMS"
                  : "BATCH REPORT — SELECTED CLAIMS"}
              </Text>
            </View>
          ) : null}
          <Header
            tenant={tenant}
            storeName={p.storeName}
            storePlatform={p.storePlatform}
          />
          <ClaimBody
            detail={p.detail}
            claimAmountNote={p.claimAmountNote}
            marketplaceClaimIdNote={p.marketplaceClaimIdNote}
            storePlatform={p.storePlatform}
          />
          <Text style={styles.footer} fixed>
            Claim {idx + 1} of {pages.length} · {p.detail.claim.id}
          </Text>
        </Page>
      ))}
    </Document>
  );
}
