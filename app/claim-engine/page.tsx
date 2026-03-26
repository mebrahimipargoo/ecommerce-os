import { supabaseServer } from "../../lib/supabase-server";
import {
  listClaimPipelineReturns,
  listPallets,
  listPackages,
} from "../returns/actions";
import { ClaimEngineClient } from "./ClaimEngineClient";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

type ClaimRow = {
  id: string;
  amount: number;
  status: "pending" | "recovered" | "suspicious";
  claim_type: string | null;
  marketplace_provider: string | null;
  created_at: string;
  amazon_order_id: string | null;
};

export default async function ClaimEnginePage() {
  let claims: ClaimRow[] = [];
  let claimsError: string | null = null;
  try {
    const { data, error } = await supabaseServer
      .from("claims")
      .select(
        "id, amount, status, claim_type, marketplace_provider, created_at, amazon_order_id",
      )
      .eq("organization_id", DEFAULT_ORGANIZATION_ID)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) throw new Error(error.message);
    claims = (data ?? []) as ClaimRow[];
  } catch (err) {
    claimsError = err instanceof Error ? err.message : "Failed to load claims.";
  }

  const [pipeRes, palRes, pkgRes] = await Promise.all([
    listClaimPipelineReturns(DEFAULT_ORGANIZATION_ID),
    listPallets(DEFAULT_ORGANIZATION_ID),
    listPackages(DEFAULT_ORGANIZATION_ID),
  ]);

  return (
    <ClaimEngineClient
      claims={claims}
      claimPipelineItems={pipeRes.ok ? pipeRes.data ?? [] : []}
      pallets={palRes.ok ? palRes.data ?? [] : []}
      packages={pkgRes.ok ? pkgRes.data ?? [] : []}
      claimsError={claimsError}
      pipelineError={pipeRes.ok ? null : pipeRes.error ?? null}
    />
  );
}
